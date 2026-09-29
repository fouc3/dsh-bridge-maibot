"""DSH Harness bridge plugin for MaiBot.

Drives a local DeepSeek Harness through the host-side ``dsh-bridge`` relay, and
lets the bot dispatch work without waiting for it.

Asynchronous flow
-----------------
Blocking on a long agent run would stall the conversation, so dispatch is split
in three parts:

1. ``dsh_dispatch`` returns immediately with ``stop_after_execution``; the bot
   ends its turn and tells the user it is on it -- in its own words, because a
   Tool result is fed back to the model rather than sent to the user directly.
2. The bridge runs the job in the background and remembers which chat stream
   asked for it.
3. When the job finishes, the bridge calls this plugin's local listener; the
   plugin appends the result to the stream's context and wakes the Planner, so
   the bot reports back in its own voice.

Follow-up questions are answered from the stored result by default, which costs
nothing; only an explicit request sends the bot back to the Harness session.

Security posture
----------------
The Harness agent can run arbitrary commands on the host, so this plugin is
deliberately conservative:

* ``allowed_senders`` is empty by default, which means **nobody** may trigger
  it. An operator must opt specific senders in explicitly.
* Incoming chat text is treated as *prompt content only*. It is never parsed as
  instructions to this plugin or to the bridge.
* ``enable_write_ops`` gates whether prompts may modify the filesystem. When it
  is false, prompts carry a read-only directive.
* ``bridge_token`` is never logged or echoed back to the chat.
* The callback listener authenticates every request with the same token and
  rejects everything else before touching any state.
"""

import asyncio
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

from maibot_sdk import Command, MaiBotPlugin, PluginConfigBase, Field, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

__all__ = ["DshHarnessPlugin", "create_plugin"]


class PluginSectionConfig(PluginConfigBase):
    """基础配置节。

    The runner requires a ``[plugin]`` section carrying ``config_version``;
    without it the plugin is refused before it ever loads.
    """

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(
        default=True,
        description="是否启用插件。关闭后所有工具与命令直接拒绝。",
        json_schema_extra={"label": "启用插件", "order": 0},
    )
    config_version: str = Field(
        default="1.0.0",
        description="配置版本号，请勿手动修改。",
        json_schema_extra={"label": "配置版本", "order": 1},
    )


class DshHarnessConfig(PluginConfigBase):
    """Operator-facing settings; the bridge coordinates live here."""

    __ui_label__ = "DSH Harness 桥接"

    plugin: PluginSectionConfig = Field(
        default_factory=PluginSectionConfig,
        json_schema_extra={"label": "插件基础设置", "order": 0},
    )

    # --- 连接 ---
    bridge_host: str = Field(
        default="172.24.0.1",
        description="宿主机 dsh-bridge 的地址。容器内通常就是 docker 网桥网关。",
        json_schema_extra={"label": "宿主地址", "order": 10, "group": "连接设置"},
    )
    bridge_port: int = Field(
        default=13081,
        description="宿主机 dsh-bridge 的监听端口。",
        json_schema_extra={"label": "宿主端口", "order": 11, "group": "连接设置"},
    )
    bridge_token: str = Field(
        default="",
        description=(
            "与 dsh-bridge 约定的共享令牌，必须与宿主 ~/.config/dsh-bridge.env "
            "里的 DSH_BRIDGE_TOKEN 完全一致。留空时本插件拒绝一切请求。"
        ),
        json_schema_extra={"label": "共享令牌", "order": 12, "group": "连接设置"},
    )
    connect_timeout_s: int = Field(
        default=10,
        description="建立 TCP 连接的超时（秒）。",
        json_schema_extra={"label": "连接超时（秒）", "order": 13, "group": "连接设置"},
    )
    request_timeout_s: int = Field(
        default=900,
        description="单个请求的总超时（秒），需大于 agent 自身的耗时。",
        json_schema_extra={"label": "请求超时（秒）", "order": 14, "group": "连接设置"},
    )

    # --- 行为 ---
    default_cwd: str = Field(
        default="/tmp",
        description="新建会话与默认查找会话时使用的工作目录。",
        json_schema_extra={"label": "默认工作目录", "order": 20, "group": "任务行为"},
    )
    allowed_senders: list[str] = Field(
        default_factory=list,
        description=(
            "允许触发本插件的发送者 ID 白名单。"
            "默认为空 = 任何人都不能触发，必须显式填写才生效。"
        ),
        json_schema_extra={
            "label": "允许触发的发送者",
            "order": 21,
            "group": "任务行为",
            "hint": "留空时本插件不会响应任何人。填入 QQ 号，例如 [\"2152595244\"]。",
        },
    )
    enable_write_ops: bool = Field(
        default=False,
        description="是否允许 agent 执行写操作。为 false 时，提示词会被加上只读约束前缀。",
        json_schema_extra={
            "label": "允许写操作",
            "order": 22,
            "group": "任务行为",
            "hint": "关闭时助手只做只读的调查与分析，不会改动文件。",
        },
    )
    max_reply_chars: int = Field(
        default=1500,
        description="回传到聊天流的最大字符数，超出则截断。",
        json_schema_extra={"label": "回复最大字数", "order": 23, "group": "任务行为"},
    )
    search_scan_limit: int = Field(
        default=20,
        description="检索时最多拉取多少个会话的历史来匹配。",
        json_schema_extra={"label": "检索扫描上限", "order": 24, "group": "任务行为"},
    )
    report_style: str = Field(
        default="brief",
        description="汇报风格：brief = 让麦麦简要总结；detail = 倾向展开细节。",
        json_schema_extra={"label": "汇报风格", "order": 25, "group": "任务行为"},
    )
    followup_enabled: bool = Field(
        default=True,
        description="是否允许麦麦在已有结果答不上时，回去追问 DSH 原会话（会重新消耗算力）。",
        json_schema_extra={"label": "允许回问助手", "order": 26, "group": "任务行为"},
    )

    # --- 回调 ---
    callback_listen_host: str = Field(
        default="0.0.0.0",
        description=(
            "接收宿主回调的监听地址。默认 0.0.0.0 是为了让容器外的宿主能访问；"
            "每个请求都会校验令牌，令牌不对一律 401。"
        ),
        json_schema_extra={"label": "回调监听地址", "order": 30, "group": "回调与汇报"},
    )
    callback_listen_port: int = Field(
        default=13082,
        description="接收回调的监听端口，需与宿主 DSH_BRIDGE_CALLBACK_URL 一致。",
        json_schema_extra={"label": "回调监听端口", "order": 31, "group": "回调与汇报"},
    )


class CallbackListener:
    """A tiny authenticated HTTP endpoint the bridge reports outcomes to.

    Runs on its own thread so it never blocks the bot's event loop. Every
    request is rejected unless it carries the shared token; the handler only
    parses JSON after that check passes.

    Only one report per ``taskId`` is acted on, so a bridge retry cannot make
    the bot announce the same job twice.
    """

    MAX_BODY_BYTES: ClassVar[int] = 256 * 1024

    def __init__(self, *, host: str, port: int, token_provider, on_event, logger) -> None:
        self._host = host
        self._port = port
        # A callable rather than a value: the token may be filled in through
        # the settings page after the listener is already running, and
        # restarting the bot to pick it up would be unreasonable.
        self._token_provider = token_provider
        self._on_event = on_event
        self._logger = logger
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._seen: set[str] = set()
        self._seen_lock = threading.Lock()

    @property
    def bound_address(self) -> str:
        if self._server is None:
            return f"{self._host}:{self._port} (not started)"
        host, port = self._server.server_address[:2]
        return f"{host}:{port}"

    def start(self) -> None:
        listener = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:  # noqa: D102 - silence default stderr logging
                return

            def _respond(self, code: int, payload: dict) -> None:
                body = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:  # noqa: N802 - name required by the base class
                if not listener._authorised(self.headers.get("authorization", "")):
                    # Unauthorised requests never reach parsing or state.
                    self._respond(401, {"ok": False, "error": "unauthorized"})
                    return

                try:
                    length = int(self.headers.get("content-length", "0"))
                except ValueError:
                    self._respond(400, {"ok": False, "error": "bad content-length"})
                    return
                if length <= 0 or length > CallbackListener.MAX_BODY_BYTES:
                    self._respond(400, {"ok": False, "error": "bad body size"})
                    return

                try:
                    event = json.loads(self.rfile.read(length).decode())
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self._respond(400, {"ok": False, "error": "invalid json"})
                    return
                if not isinstance(event, dict):
                    self._respond(400, {"ok": False, "error": "event must be an object"})
                    return

                task_id = str(event.get("taskId") or "")
                if task_id == "":
                    self._respond(400, {"ok": False, "error": "missing taskId"})
                    return

                # Replay-safe: a duplicate report is acknowledged but ignored.
                with listener._seen_lock:
                    if task_id in listener._seen:
                        self._respond(200, {"ok": True, "duplicate": True})
                        return
                    listener._seen.add(task_id)

                try:
                    listener._on_event(event)
                except Exception as exc:  # noqa: BLE001 - never fail the HTTP call on bot errors
                    listener._logger.error("dsh-harness 处理回调失败: %s", exc)
                    with listener._seen_lock:
                        listener._seen.discard(task_id)
                    self._respond(500, {"ok": False, "error": "dispatch failed"})
                    return

                self._respond(200, {"ok": True})

        self._server = ThreadingHTTPServer((self._host, self._port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def _authorised(self, header: str) -> bool:
        """Constant-time comparison of the bearer credential."""
        import hmac

        # Read the current token on every request so a settings change takes
        # effect without a restart.
        try:
            token = self._token_provider() or ""
        except Exception:  # noqa: BLE001 - a broken config must not authenticate
            return False
        if not token:
            return False
        prefix = "Bearer "
        if not header.startswith(prefix):
            return False
        return hmac.compare_digest(header[len(prefix) :], token)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


class DshHarnessPlugin(MaiBotPlugin):
    """MaiBot plugin that relays work to DeepSeek Harness."""

    REQUEST_TIMEOUT_GRACE: ClassVar[int] = 30

    # Declaring the model makes the runner generate defaults, backfill new
    # fields on upgrade, and render the settings page; the instance then
    # arrives as `self.config`.
    config_model: ClassVar[type[PluginConfigBase]] = DshHarnessConfig

    def __init__(self) -> None:
        # The SDK base initializes state its own machinery consults right after
        # load (for example the dynamic-API component registry). Skipping it
        # breaks plugin registration entirely, not just this class.
        super().__init__()
        self._listener: CallbackListener | None = None
        # Captured at load time so the listener thread can hand work back to
        # the loop the bot actually runs on.
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def on_load(self) -> None:
        """Start the callback listener as soon as the plugin is loaded."""
        self.ctx.logger.info("dsh-harness 插件已加载")
        self._loop = asyncio.get_running_loop()
        self._start_listener()

    def _start_listener(self) -> None:
        """Bind the callback listener using the current configuration."""
        cfg = self._config()
        try:
            self._listener = CallbackListener(
                host=cfg.callback_listen_host,
                port=cfg.callback_listen_port,
                token_provider=lambda: self._config().bridge_token,
                on_event=self._on_bridge_event,
                logger=self.ctx.logger,
            )
            self._listener.start()
            self.ctx.logger.info("dsh-harness 回调监听已启动: %s", self._listener.bound_address)
        except OSError as exc:
            # A busy port must not take the whole plugin down; dispatch still
            # works, only the proactive report is unavailable.
            self.ctx.logger.error("dsh-harness 回调监听启动失败: %s", exc)
            self._listener = None

    async def on_unload(self) -> None:
        if self._listener is not None:
            self._listener.stop()
            self._listener = None
        self.ctx.logger.info("dsh-harness 插件已卸载")

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        if scope != "self":
            return
        # The token is read per request, so only a changed bind address needs
        # the listener restarted.
        cfg = self._config()
        bound = self._listener is not None
        wants = (cfg.callback_listen_host, cfg.callback_listen_port)
        current = (getattr(self._listener, "_host", None), getattr(self._listener, "_port", None))
        if bound and wants != current:
            self.ctx.logger.info("dsh-harness 回调监听地址变更，正在重启监听器")
            self._listener.stop()
            self._listener = None
            self._start_listener()
        elif not bound:
            self._start_listener()
        self.ctx.logger.info("dsh-harness 配置已更新: version=%s", version)

    # ------------------------------------------------------------------
    # Callback handling
    # ------------------------------------------------------------------

    def _on_bridge_event(self, event: dict[str, Any]) -> None:
        """Handle one completed task reported by the bridge.

        Runs on the listener thread, so it only schedules the async work; the
        bot itself is driven from the event loop.
        """
        stream_id = event.get("streamId")
        if not stream_id:
            self.ctx.logger.warning("dsh-harness 回调缺少 streamId，已忽略: %s", event.get("taskId"))
            return

        loop = self._loop
        if loop is None or loop.is_closed():
            self.ctx.logger.warning("dsh-harness 事件循环不可用，回调已丢弃: %s", event.get("taskId"))
            return

        asyncio.run_coroutine_threadsafe(self._report_back(event), loop)

    async def _report_back(self, event: dict[str, Any]) -> None:
        """Inject the result as context, then wake the bot to report it.

        The bot is woken rather than sent a fixed string, so the report is
        phrased by the model in its own voice.
        """
        stream_id = event["streamId"]
        status = event.get("status")
        reply = (event.get("reply") or "").strip()

        summary = self._describe_for_bot(status, reply)
        try:
            await self.ctx.maisaka.context.append(
                stream_id=stream_id,
                segments=[{"type": "text", "content": summary["context"]}],
                visible_text=summary["context"],
                source_kind="plugin:dsh-harness",
            )
            await self.ctx.maisaka.proactive.trigger(
                stream_id=stream_id,
                intent=summary["intent"],
                reason="dsh_task_finished",
                metadata={"taskId": event.get("taskId"), "status": status},
            )
        except Exception as exc:  # noqa: BLE001 - a failed report must not crash the plugin
            self.ctx.logger.error("dsh-harness 主动汇报失败: %s", exc)

    def _describe_for_bot(self, status: str | None, reply: str) -> dict[str, str]:
        """Build the instruction the bot uses to phrase its own report.

        Only one distinction is drawn: whether an actual answer exists. When the
        run failed there is nothing for the bot to read, so it must be told this
        is a fault rather than a result -- otherwise it would report an error
        string as if it were the outcome.

        A run that stopped to ask something is *not* special-cased. The bot sees
        the agent's own words, and it is the one holding the conversation, so it
        is better placed than this layer to decide whether to report progress or
        relay a question back to the user.
        """
        style = self._config().report_style
        length_hint = "一两句话即可，不要罗列细节" if style == "brief" else "可以适当展开关键结论"

        if status == "error":
            return {
                "context": (
                    "本地助手执行失败——注意，这是故障信息，不是助手给你的结果。"
                    f"失败原因：\n{reply}"
                ),
                "intent": (
                    f"请用你的口吻告诉用户任务没做成，简要说明原因，{length_hint}。"
                    "不要把它当成任务结果来汇报。"
                ),
            }

        return {
            "context": f"你派给本地助手的任务已经停下来并给出了以下内容：\n{reply}",
            "intent": (
                f"请根据上面的内容用自己的话回应用户，{length_hint}。"
                "如果助手是在向你提问或需要用户拍板，就把问题转达给用户并说明需要什么；"
                "如果是结果，就简要汇报完成情况。完整内容已存入上下文，用户追问时可以据此回答。"
            ),
        }

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def _config(self) -> DshHarnessConfig:
        """Return the typed config the SDK injects.

        Declaring ``config_model`` makes the runner generate the defaults, fill
        in missing fields, and expose the schema to the WebUI; the instance is
        then reachable as ``self.config``.
        """
        return self.config

    def _guard(self, sender_id: str | None) -> str | None:
        """Return a refusal message, or None when the call may proceed."""
        cfg = self._config()
        if not cfg.plugin.enabled:
            return "插件当前已禁用。"
        if not cfg.bridge_token:
            return "插件未配置 bridge_token，请先在麦麦插件配置中填写。"
        allowed = [str(item) for item in (cfg.allowed_senders or [])]
        if not allowed:
            return "未配置 allowed_senders，出于安全考虑本插件不会响应任何请求。"
        if sender_id is None or str(sender_id) not in allowed:
            return "你没有使用该功能的权限。"
        return None

    # ------------------------------------------------------------------
    # Bridge transport
    # ------------------------------------------------------------------

    async def _call(self, op: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        """Send one request to the bridge and return its ``value``.

        Raises ``RuntimeError`` with a readable message on any failure so both
        the tools and the command surface can report the same way.
        """
        return await self._call_inner(op, args)

    async def _call_inner(
        self,
        op: str,
        args: dict[str, Any] | None = None,
        *,
        allow_session_repair: bool = True,
    ) -> dict[str, Any]:
        cfg = self._config()
        payload = {"id": uuid.uuid4().hex, "op": op, "args": args or {}}
        timeout = cfg.connect_timeout_s + cfg.request_timeout_s + self.REQUEST_TIMEOUT_GRACE

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.bridge_host, cfg.bridge_port),
                timeout=cfg.connect_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"连接 dsh-bridge 超时（{cfg.bridge_host}:{cfg.bridge_port}），请确认宿主中转进程已启动。"
            ) from exc
        except OSError as exc:
            raise RuntimeError(
                f"无法连接 dsh-bridge（{cfg.bridge_host}:{cfg.bridge_port}）：{exc}"
            ) from exc

        try:
            writer.write((json.dumps({"token": cfg.bridge_token}) + "\n").encode())
            await writer.drain()
            hello = await self._read_line(reader, cfg.connect_timeout_s)
            if not hello.get("ok"):
                raise RuntimeError("dsh-bridge 拒绝了本次连接：令牌不匹配。")

            writer.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
            await writer.drain()
            reply = await self._read_line(reader, timeout)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001 - closing must never mask the real error
                pass

        if reply.get("ok"):
            return reply.get("value") or {}

        error = reply.get("error") or {}
        code = error.get("code", "unknown")

        # A directory with no session yet is an ordinary first run, not a
        # failure: create the session and retry once so the user never sees it.
        if code == "no-session" and allow_session_repair and op == "prompt":
            cwd = (args or {}).get("cwd") or cfg.default_cwd
            await self._call_inner("sessions_new", {"cwd": cwd}, allow_session_repair=False)
            return await self._call_inner(op, args, allow_session_repair=False)

        raise RuntimeError(f"dsh-bridge 返回错误 [{code}]：{error.get('message', '')}")

    @staticmethod
    async def _read_line(reader: asyncio.StreamReader, timeout: float) -> dict[str, Any]:
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("等待 dsh-bridge 响应超时。") from exc
        if not raw:
            raise RuntimeError("dsh-bridge 在返回结果前关闭了连接。")
        try:
            return json.loads(raw.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("dsh-bridge 返回了无法解析的内容。") from exc

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _prepare_prompt(self, text: str) -> str:
        """Apply the read-only directive when write operations are disabled."""
        if self._config().enable_write_ops:
            return text
        return (
            "以下是一个请求。请只做只读的调查与分析，不要修改任何文件、"
            "不要执行有副作用的命令：\n\n" + text
        )

    def _format_reply(self, value: dict[str, Any]) -> str:
        cfg = self._config()
        reply = (value.get("reply") or "").strip()
        if not reply:
            reason = value.get("stopReason") or "未知"
            return f"（agent 没有返回文本内容，stopReason={reason}）"
        if len(reply) > cfg.max_reply_chars:
            return reply[: cfg.max_reply_chars] + "\n…（内容过长已截断）"
        return reply

    # ------------------------------------------------------------------
    # Tools
    # ------------------------------------------------------------------

    @Tool(
        "dsh_list_sessions",
        brief_description="列出本地助手有哪些工作区，以及每个工作区里的会话",
        detailed_description=(
            "查看本地助手保存的会话，**按工作目录分组**返回。可用于：\n"
            "- 回答「都有哪些工作区」——直接看返回的 cwd 列表；\n"
            "- 回答「某个目录里有哪些会话」——传 cwd；\n"
            "- 找到可以接着聊的会话名——传 named_only=true，只看有名字的。\n"
            "\n"
            "只有带 name 的会话才能被明确继续；要继续某个会话，"
            "把它的 name 填进 dsh_dispatch 的 session_name。\n"
            "\n"
            "注意：绝大多数历史会话没有名字也没有时间（助手侧不记录），"
            "这是正常的——它们只能用其所在目录的默认身份继续。\n"
            "\n"
            "- cwd：string，可选。只看这个目录。\n"
            "- named_only：boolean，可选。只列出有名字的会话，默认 false。\n"
            "- limit：integer，可选。最多返回多少个工作区，默认 10。"
        ),
        parameters=[
            ToolParameterInfo(
                name="cwd",
                param_type=ToolParamType.STRING,
                description="只看这个目录下的会话；留空则列出所有工作区",
                required=False,
            ),
            ToolParameterInfo(
                name="named_only",
                param_type=ToolParamType.STRING,
                description="只列出有名字的会话（true/false）",
                required=False,
            ),
            ToolParameterInfo(
                name="limit",
                param_type=ToolParamType.INTEGER,
                description="最多返回多少个工作区",
                required=False,
                default=10,
            ),
        ],
    )
    async def handle_list_sessions(
        self,
        cwd: str | None = None,
        named_only: str | bool | None = None,
        limit: int = 10,
        **kwargs,
    ):
        named = str(named_only).strip().lower() in ("true", "1", "yes") if named_only else False
        value = await self._call(
            "workspaces",
            {
                "cwd": self._config().default_cwd,
                "namedOnly": named,
                "maxPerWorkspace": 10,
            },
        )
        workspaces = value.get("workspaces", [])
        if cwd:
            workspaces = [w for w in workspaces if w.get("cwd") == cwd]

        trimmed = []
        for workspace in workspaces[: max(1, int(limit))]:
            trimmed.append(
                {
                    "cwd": workspace.get("cwd"),
                    "sessions": workspace.get("sessionCount"),
                    "named": workspace.get("namedCount"),
                    "last_used": workspace.get("lastUsedAt"),
                    "names": [s.get("name") for s in workspace.get("sessions", []) if s.get("name")],
                }
            )

        return {
            "workspaces": trimmed,
            "workspace_count": len(trimmed),
            "total_sessions": value.get("totalSessions"),
            "named_sessions": value.get("namedSessions"),
            "message": (
                "要接着某个会话聊，把它的 name 传给 dsh_dispatch 的 session_name；"
                "没名字的会话就用它所在目录的默认身份继续。"
            ),
        }

    @Tool(
        "dsh_new_session",
        brief_description="在本地 DeepSeek Harness 新建一个会话",
        detailed_description=(
            "创建一个新的 Harness 会话并返回 session_id，后续可用它继续对话。\n"
            "- cwd：string，可选。会话的工作目录。\n"
            "- name：string，可选。会话名，便于复用。"
        ),
        parameters=[
            ToolParameterInfo(
                name="cwd",
                param_type=ToolParamType.STRING,
                description="会话工作目录",
                required=False,
            ),
            ToolParameterInfo(
                name="name",
                param_type=ToolParamType.STRING,
                description="会话名",
                required=False,
            ),
        ],
    )
    async def handle_new_session(self, cwd: str | None = None, name: str | None = None, **kwargs):
        cfg = self._config()
        value = await self._call(
            "sessions_new",
            {"cwd": cwd or cfg.default_cwd, "name": name or None},
        )
        return value

    @Tool(
        "dsh_dispatch",
        brief_description="把一项耗时的任务派给本地助手后台执行，立即返回不等待",
        detailed_description=(
            "把任务交给本地 DeepSeek Harness 在后台执行。本工具**立刻返回**，"
            "不需要等待任务完成；任务干完后会自动回到这个聊天流汇报。\n"
            "适用场景：需要跑代码、查文件、执行命令、分析大内容等耗时工作。\n"
            "调用后请用你自己的话简短告诉用户「已经安排上了」，不要假装结果已经出来。\n"
            "\n"
            "关于会话：\n"
            "- 同一个 session_name 会**接续同一个会话**，助手能记得之前聊过的内容。\n"
            "- 想接着某个旧会话干活时，用 dsh_list_sessions 查到它的 name 再填进来。\n"
            "- 留空则使用该目录下的默认会话。\n"
            "- 建议按用途起稳定的名字，例如「文件整理」「每周报告」，这样同一个群的任务能连贯进行。\n"
            "\n"
            "- task：string，必填。要交给本地助手做的事情，写清楚目标。\n"
            "- stream_id：string，必填。当前聊天流 ID，用于把结果汇报回这里。\n"
            "- session_name：string，可选。会话名，用于接续之前的会话。\n"
            "- cwd：string，可选。任务的工作目录。"
        ),
        parameters=[
            ToolParameterInfo(
                name="task",
                param_type=ToolParamType.STRING,
                description="要交给本地助手做的事情",
                required=True,
            ),
            ToolParameterInfo(
                name="stream_id",
                param_type=ToolParamType.STRING,
                description="当前聊天流 ID",
                required=True,
            ),
            ToolParameterInfo(
                name="session_name",
                param_type=ToolParamType.STRING,
                description="会话名；同名会话会被接续，留空用默认会话",
                required=False,
            ),
            ToolParameterInfo(
                name="cwd",
                param_type=ToolParamType.STRING,
                description="任务工作目录",
                required=False,
            ),
        ],
    )
    async def handle_dispatch(
        self,
        task: str,
        stream_id: str,
        session_name: str | None = None,
        cwd: str | None = None,
        **kwargs,
    ):
        cfg = self._config()
        value = await self._call(
            "dispatch",
            {
                "cwd": cwd or cfg.default_cwd,
                "text": self._prepare_prompt(task),
                "streamId": stream_id,
                "name": (session_name or "").strip() or None,
            },
        )
        # `stop_after_execution` ends this turn once the batch completes: the
        # job is running elsewhere, so waiting here would only stall the chat.
        return {
            "success": True,
            "status": "dispatched",
            "task_id": value.get("taskId"),
            "session_name": value.get("sessionName"),
            "message": (
                "任务已交给本地助手在后台执行，完成后会自动汇报。"
                "请用你自己的话简短告知用户已经安排上了，不要编造结果。"
            ),
            "stop_after_execution": True,
        }

    @Tool(
        "dsh_task_status",
        brief_description="查询之前派给本地助手的任务状态与完整结果",
        detailed_description=(
            "查一个已派任务的当前状态和结果。**优先用它回答用户的追问**——"
            "完整结果已经存在这里，直接读即可，不需要重新执行任务。\n"
            "只有当用户明确要求「再去看看 / 重新确认 / 让助手再查一下」，"
            "或者已有结果确实回答不了时，才考虑用 dsh_followup 回去追问。\n"
            "- task_id：string，必填。派活时返回的任务 ID。"
        ),
        parameters=[
            ToolParameterInfo(
                name="task_id",
                param_type=ToolParamType.STRING,
                description="任务 ID",
                required=True,
            ),
        ],
    )
    async def handle_task_status(self, task_id: str, **kwargs):
        value = await self._call("task_status", {"taskId": task_id})
        status = value.get("status")
        reply = (value.get("reply") or "").strip()
        if status == "running":
            return {"status": "running", "message": "任务还在执行中，还没有结果。"}
        return {
            "status": status,
            "reply": reply,
            "message": (
                "这是该任务的完整结果，请据此回答用户的问题；"
                "如果用户问的细节这里没有，再考虑回去追问。"
            ),
        }

    @Tool(
        "dsh_followup",
        brief_description="回到本地助手的原会话继续追问（会重新消耗算力）",
        detailed_description=(
            "在之前那个本地助手会话里继续追问。这**会真的再执行一次**，"
            "有耗时和成本，只有在已有结果答不上、或用户明确要求重新确认时才用。\n"
            "这个是当场等结果的，追问一般很快，不需要再走汇报流程。\n"
            "- task_id：string，必填。原任务 ID。\n"
            "- question：string，必填。要追问的内容。"
        ),
        parameters=[
            ToolParameterInfo(
                name="task_id",
                param_type=ToolParamType.STRING,
                description="原任务 ID",
                required=True,
            ),
            ToolParameterInfo(
                name="question",
                param_type=ToolParamType.STRING,
                description="要追问的内容",
                required=True,
            ),
        ],
    )
    async def handle_followup(self, task_id: str, question: str, **kwargs):
        cfg = self._config()
        if not cfg.followup_enabled:
            return {"success": False, "message": "追问功能已被管理员关闭，请根据已有结果回答。"}

        original = await self._call("task_status", {"taskId": task_id})
        cwd = original.get("cwd") or cfg.default_cwd
        # Reuse the exact conversation the original task ran in. Without the
        # session name the question would land in whatever session that
        # directory defaults to, which may be an unrelated conversation.
        session_name = original.get("sessionName")
        value = await self._call(
            "prompt",
            {
                "cwd": cwd,
                "name": session_name or None,
                "text": self._prepare_prompt(question),
            },
        )
        return {
            "success": True,
            "reply": self._format_reply(value),
            "session_name": session_name,
            "message": "这是本地助手对追问的回答，请用你的话转达给用户。",
        }

    @Tool(
        "dsh_ask",
        brief_description="向本地助手提问并**当场等待**结果（会阻塞，适合很快的任务）",
        detailed_description=(
            "同步提问并等待结果返回。因为它会阻塞本轮直到任务做完，"
            "**只适合预期很快的任务**；耗时任务请改用 dsh_dispatch。\n"
            "- text：string，必填。要问的内容。\n"
            "- cwd：string，可选。工作目录。"
        ),
        parameters=[
            ToolParameterInfo(
                name="text",
                param_type=ToolParamType.STRING,
                description="要问的内容",
                required=True,
            ),
            ToolParameterInfo(
                name="cwd",
                param_type=ToolParamType.STRING,
                description="工作目录",
                required=False,
            ),
        ],
    )
    async def handle_ask(self, text: str, cwd: str | None = None, name: str | None = None, **kwargs):
        cfg = self._config()
        value = await self._call(
            "prompt",
            {
                "cwd": cwd or cfg.default_cwd,
                "name": name or None,
                "text": self._prepare_prompt(text),
            },
        )
        return {
            "reply": self._format_reply(value),
            "session_id": value.get("sessionId"),
            "stop_reason": value.get("stopReason"),
            "tool_calls": value.get("toolCalls"),
        }

    @Tool(
        "dsh_search",
        brief_description="在本地 Harness 的历史对话中检索关键字",
        detailed_description=(
            "在 Harness 保存的会话历史里查找包含关键字的轮次。\n"
            "注意：ACP 不提供全文检索，这里是「分页读历史 + 字面量过滤」，"
            "因此需要配合 cwd 或较小的条数上限使用。\n"
            "- query：string，必填。关键字。\n"
            "- cwd：string，可选。限定工作目录。"
        ),
        parameters=[
            ToolParameterInfo(
                name="query",
                param_type=ToolParamType.STRING,
                description="检索关键字",
                required=True,
            ),
            ToolParameterInfo(
                name="cwd",
                param_type=ToolParamType.STRING,
                description="限定工作目录",
                required=False,
            ),
        ],
    )
    async def handle_search(self, query: str, cwd: str | None = None, **kwargs):
        cfg = self._config()
        needle = (query or "").strip().lower()
        if not needle:
            return {"error": "query 不能为空"}

        listing = await self._call(
            "sessions_list",
            {"cwd": cwd or cfg.default_cwd, "filterCwd": cwd or None, "source": "agent"},
        )
        sessions = listing.get("sessions", [])[: cfg.search_scan_limit]

        hits: list[dict[str, Any]] = []
        for session in sessions:
            try:
                history = await self._call(
                    "sessions_history",
                    {"cwd": session.get("cwd") or cwd or cfg.default_cwd, "limit": 20},
                )
            except RuntimeError:
                continue
            for line in history.get("history", []):
                if needle in line.lower():
                    hits.append({"sessionId": session.get("sessionId"), "line": line[:300]})
                    break

        return {"query": query, "scanned": len(sessions), "hits": hits}

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    @Command("dsh", pattern=r"^/dsh(\s+.*)?$")
    async def handle_command(self, **kwargs):
        """Human-facing entry point: ``/dsh list|new|ask <text>|search <kw>``."""
        stream_id = kwargs.get("stream_id")
        sender_id = kwargs.get("sender_id") or kwargs.get("user_id")
        raw = (kwargs.get("matched_text") or kwargs.get("raw_message") or "").strip()

        refusal = self._guard(sender_id)
        if refusal:
            await self.ctx.send.text(refusal, stream_id)
            return True, refusal, 2

        parts = raw.split(maxsplit=2)[1:]
        action = parts[0].lower() if parts else "help"

        try:
            if action == "list":
                value = await self._call(
                    "sessions_list",
                    {"cwd": self._config().default_cwd, "source": "agent"},
                )
                sessions = value.get("sessions", [])[:10]
                if not sessions:
                    text = "没有找到任何会话。"
                else:
                    lines = [f"- {s.get('sessionId')}  ({s.get('cwd')})" for s in sessions]
                    text = "最近的会话：\n" + "\n".join(lines)

            elif action == "new":
                value = await self._call("sessions_new", {"cwd": self._config().default_cwd})
                text = f"已新建会话：{value.get('sessionId')}"

            elif action == "ask" and len(parts) >= 2:
                value = await self._call(
                    "prompt",
                    {
                        "cwd": self._config().default_cwd,
                        "text": self._prepare_prompt(" ".join(parts[1:])),
                    },
                )
                text = self._format_reply(value)

            elif action == "search" and len(parts) >= 2:
                result = await self.handle_search(" ".join(parts[1:]))
                hits = result.get("hits", [])
                text = (
                    "没有匹配的历史。"
                    if not hits
                    else "匹配到：\n" + "\n".join(f"- {h['sessionId']}: {h['line']}" for h in hits[:5])
                )

            else:
                text = "用法：/dsh list | /dsh new | /dsh ask <内容> | /dsh search <关键字>"

        except RuntimeError as exc:
            text = f"执行失败：{exc}"

        await self.ctx.send.text(text, stream_id)
        return True, text, 2


def create_plugin() -> DshHarnessPlugin:
    """SDK entry point."""
    return DshHarnessPlugin()
