"""Tests for the plugin's callback listener.

The listener is the one new component that the bridge talks to directly, so it
gets tested on its own terms: authentication, framing, replay safety, and the
handoff to the bot.

This imports ``plugin`` with the MaiBot SDK stubbed out, because the SDK only
exists inside the MaiBot runtime. Only the listener and the report-shaping
helper are exercised; nothing here touches a live bot.

Usage:  python3 test/plugin_callback_test.py
"""

from __future__ import annotations

import json
import sys
import threading
import types
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = ROOT / "plugin"


def install_sdk_stub() -> None:
    """Provide a minimal but faithful stand-in for maibot_sdk.

    The stub mirrors the two contracts a plugin can easily violate and which
    the real runtime reacts to at load time:

    * ``MaiBotPlugin.__init__`` seeds SDK-owned state, so a subclass that
      forgets ``super().__init__()`` fails the same way it does in production;
    * ``config_model`` is honoured, with the instance exposed as ``self.config``.
    """
    sdk = types.ModuleType("maibot_sdk")

    class _Decorator:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __call__(self, fn):
            return fn

    class _FieldSpec:
        """What Field() returns: a declared default plus its metadata."""

        def __init__(self, *, default=None, factory=None, extra=None) -> None:
            self.default = default
            self.factory = factory
            self.extra = extra or {}

    class PluginConfigBase:
        """Minimal stand-in that honours the SDK's Field surface.

        Only the shape a plugin actually depends on is reproduced: declared
        defaults (including ``default_factory``) and nested config classes
        materialized as instances.
        """

        def __init__(self, **values) -> None:
            for name, spec in self._field_specs().items():
                if name in values:
                    setattr(self, name, values[name])
                elif spec.factory is not None:
                    setattr(self, name, spec.factory())
                elif isinstance(spec.default, type) and issubclass(spec.default, PluginConfigBase):
                    # A nested config class materializes as an instance, which
                    # is how the real SDK exposes sub-sections.
                    setattr(self, name, spec.default())
                else:
                    setattr(self, name, spec.default)

        @classmethod
        def _field_specs(cls):
            """Collect declared fields, subclasses overriding superclasses."""
            specs = {}
            for klass in reversed(cls.__mro__):
                for name, value in vars(klass).items():
                    if name.startswith("_") or not isinstance(value, _FieldSpec):
                        continue
                    specs[name] = value
            return specs

    def Field(default=None, **kwargs):  # noqa: N802 - mirrors the SDK name
        return _FieldSpec(
            default=default,
            factory=kwargs.get("default_factory"),
            extra=kwargs.get("json_schema_extra"),
        )

    class MaiBotPlugin:  # noqa: D101 - mirrors the SDK base
        config_model = None

        def __init__(self) -> None:
            # SDK-owned state; the dynamic-API registry is what the runtime
            # touches right after load.
            self._dynamic_api_components: dict = {}
            self._dynamic_api_handlers: dict = {}
            self._plugin_config_data: dict = {}
            self._plugin_config_instance = None

        @property
        def config(self):  # noqa: D102 - mirrors the SDK property
            model = type(self).config_model
            if model is None:
                raise RuntimeError("当前插件未声明 config_model，无法通过 config 属性访问强类型配置")
            if self._plugin_config_instance is None:
                self._plugin_config_instance = model()
            return self._plugin_config_instance

    sdk.MaiBotPlugin = MaiBotPlugin
    sdk.PluginConfigBase = PluginConfigBase
    sdk.Field = Field
    sdk.Tool = _Decorator
    sdk.Command = _Decorator

    types_mod = types.ModuleType("maibot_sdk.types")

    class ToolParameterInfo:  # noqa: D101 - stub
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class ToolParamType:  # noqa: D101 - stub
        STRING = "string"
        INTEGER = "integer"

    types_mod.ToolParameterInfo = ToolParameterInfo
    types_mod.ToolParamType = ToolParamType
    sdk.types = types_mod

    sys.modules["maibot_sdk"] = sdk
    sys.modules["maibot_sdk.types"] = types_mod


install_sdk_stub()
sys.path.insert(0, str(PLUGIN_DIR))

from plugin import CallbackListener  # noqa: E402

TOKEN = "callback-test-token-0123456789"
failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" — {detail}" if detail and not condition else ""))
    if not condition:
        failures.append(name)


class FakeLogger:
    def info(self, *a, **k) -> None: ...
    def warning(self, *a, **k) -> None: ...
    def error(self, *a, **k) -> None: ...


def post(url: str, payload, token: str | None = TOKEN, raw: bytes | None = None):
    """POST JSON (or raw bytes) and return (status, parsed-body-or-text)."""
    headers = {"content-type": "application/json"}
    if token is not None:
        headers["authorization"] = f"Bearer {token}"
    body = raw if raw is not None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            text = resp.read().decode()
            try:
                return resp.status, json.loads(text)
            except json.JSONDecodeError:
                return resp.status, text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode()
        try:
            return exc.code, json.loads(text)
        except json.JSONDecodeError:
            return exc.code, text


def main() -> int:
    received: list[dict] = []
    lock = threading.Lock()

    def on_event(event: dict) -> None:
        with lock:
            received.append(event)

    listener = CallbackListener(
        host="127.0.0.1",
        port=0,
        token_provider=lambda: TOKEN,
        on_event=on_event,
        logger=FakeLogger(),
    )
    listener.start()
    host, port = listener._server.server_address[:2]
    url = f"http://{host}:{port}/event"

    try:
        # 1. A wrong token is rejected before anything else happens.
        status, body = post(url, {"taskId": "t1", "streamId": "s1"}, token="wrong")
        check("wrong token rejected with 401", status == 401 and body.get("error") == "unauthorized")
        check("unauthorized request did not reach the handler", len(received) == 0)

        # 2. A missing Authorization header is rejected too.
        status, _ = post(url, {"taskId": "t1", "streamId": "s1"}, token=None)
        check("missing token rejected", status == 401)

        # 3. A good token succeeds and reaches the handler exactly once.
        status, body = post(url, {"taskId": "t1", "streamId": "s1", "status": "done", "reply": "ok"})
        check("valid callback accepted", status == 200 and body.get("ok") is True, str(body))
        check("handler received the event", len(received) == 1, f"got {len(received)}")
        check("event fields preserved", received[0].get("streamId") == "s1")

        # 4. A duplicate report is acknowledged but not re-delivered.
        status, body = post(url, {"taskId": "t1", "streamId": "s1", "status": "done", "reply": "ok"})
        check("duplicate acknowledged", status == 200 and body.get("duplicate") is True)
        check("duplicate not re-delivered", len(received) == 1, f"got {len(received)}")

        # 5. A different task id is delivered normally.
        status, _ = post(url, {"taskId": "t2", "streamId": "s1", "status": "done", "reply": "ok"})
        check("distinct task delivered", status == 200 and len(received) == 2)

        # 6. Malformed input is refused without reaching the handler.
        status, _ = post(url, None, raw=b"{not json")
        check("invalid json rejected", status == 400 and len(received) == 2)
        status, _ = post(url, {"streamId": "s1"})
        check("missing taskId rejected", status == 400 and len(received) == 2)

        # 7. A handler failure is reported as 500 and does NOT consume the id,
        #    so the bridge's retry can still succeed.
        boom_calls: list[dict] = []

        def failing(event: dict) -> None:
            boom_calls.append(event)
            raise RuntimeError("bot unavailable")

        listener2 = CallbackListener(
            host="127.0.0.1", port=0, token_provider=lambda: TOKEN, on_event=failing, logger=FakeLogger()
        )
        listener2.start()
        h2, p2 = listener2._server.server_address[:2]
        url2 = f"http://{h2}:{p2}/event"
        status, _ = post(url2, {"taskId": "t3", "streamId": "s1"})
        check("handler failure surfaces as 500", status == 500)
        status, _ = post(url2, {"taskId": "t3", "streamId": "s1"})
        check("failed task id is retried, not deduped", len(boom_calls) == 2, f"got {len(boom_calls)}")
        listener2.stop()

        # 8. An oversized body is refused.
        big = json.dumps({"taskId": "big", "streamId": "s", "reply": "x" * (300 * 1024)}).encode()
        status, _ = post(url, None, raw=big)
        check("oversized body rejected", status == 400, str(status))

        # 9. The listener can be stopped cleanly.
        listener.stop()
        listener.start()  # same object can restart, proving stop() releases the port
        listener.stop()
        check("listener stops and restarts cleanly", True)

    finally:
        try:
            listener.stop()
        except Exception:
            pass

    # 9b. The token is read per request, so filling it in through the settings
    #     page takes effect without restarting the bot. Hard-coding it at
    #     startup would silently reject every callback until a reload.
    rotating = {"token": ""}
    received_after = []
    rotation_listener = CallbackListener(
        host="127.0.0.1",
        port=0,
        token_provider=lambda: rotating["token"],
        on_event=lambda ev: received_after.append(ev),
        logger=FakeLogger(),
    )
    rotation_listener.start()
    rh, rp = rotation_listener._server.server_address[:2]
    rurl = f"http://{rh}:{rp}/event"

    # Nothing is accepted while the token is unset.
    status, _ = post(rurl, {"taskId": "rot-0", "streamId": "s"}, token=TOKEN)
    check("an unset token accepts nothing", status == 401)

    rotating["token"] = TOKEN
    status, body = post(rurl, {"taskId": "rot-1", "streamId": "s"})
    check("a token set later is honoured without a restart", status == 200 and body.get("ok") is True)
    check("the late-set token delivered the event", len(received_after) == 1)

    # Rotating again must invalidate the old value.
    rotating["token"] = "a-completely-different-token"
    status, _ = post(rurl, {"taskId": "rot-2", "streamId": "s"}, token=TOKEN)
    check("a rotated token invalidates the old one", status == 401)
    rotation_listener.stop()

    # 10. The plugin must be constructible and expose its typed config. Both
    #     of these failed against the real runtime and are cheap to lock down:
    #     a subclass that skips super().__init__() has no SDK state, and one
    #     that declares no config_model has no typed config.
    from plugin import DshHarnessPlugin, DshHarnessConfig

    try:
        instance = DshHarnessPlugin()
        check("plugin constructs", True)
    except Exception as exc:  # noqa: BLE001 - report, do not crash the suite
        instance = None
        check("plugin constructs", False, repr(exc))

    if instance is not None:
        check(
            "SDK-owned state is initialized",
            hasattr(instance, "_dynamic_api_components"),
            "a subclass must call super().__init__(); the runtime reads this right after load",
        )
        check(
            "config_model is declared",
            getattr(type(instance), "config_model", None) is DshHarnessConfig,
            "the runner needs it to generate defaults and the settings schema",
        )
        try:
            cfg = instance.config
            check("typed config is reachable", isinstance(cfg, DshHarnessConfig))
            # A security-relevant default: nothing may trigger the plugin until
            # an operator opts senders in.
            check("allowed_senders defaults to empty", list(cfg.allowed_senders) == [])
            check("write ops default to off", cfg.enable_write_ops is False)

            # Every field must carry a Chinese label. The settings page falls
            # back to the raw field name (e.g. "bridge_host") when none is
            # given, which reads as an unfinished plugin.
            specs = DshHarnessConfig._field_specs() if hasattr(DshHarnessConfig, "_field_specs") else {}
            unlabelled = []
            for name, spec in specs.items():
                label = (spec.extra or {}).get("label")
                if not label:
                    unlabelled.append(name)
            # The stub only carries json_schema_extra; the real SDK is checked
            # separately, so treat an empty field set as "not applicable".
            if specs:
                check("every field carries a display label", not unlabelled, f"missing: {unlabelled}")
        except Exception as exc:  # noqa: BLE001
            check("typed config is reachable", False, repr(exc))

    print()
    if failures:
        print(f"{len(failures)} 项失败: {', '.join(failures)}")
        return 1
    print("全部通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
