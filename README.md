# dsh-bridge-maibot

让 **麦麦（MaiBot）** 指挥本地 **DeepSeek Harness** 干活的插件。

这是 [dsh-bridge-host](../dsh-bridge-host) 的**麦麦侧**一半：一个麦麦插件，
通过宿主上的中转进程派活、续聊、查会话，并让麦麦**用自己的话**汇报结果。

---

## 效果

麦麦不会卡在那里等 agent。整个循环是：

```
用户 → 麦麦 ──dsh_dispatch──▶ 宿主中转 ──▶ DeepSeek Harness
              │ 立即返回 + stop_after_execution
              ▼                        │ 后台干活
      麦麦自己说「我去看看」             │
              └◀── 干完回调 ── 插件注入上下文 + 唤醒麦麦
                                       │
                            麦麦自己组织语言汇报
```

三个关键设计：

1. **工具返回值是给模型看的，不是代发消息。** `dsh_dispatch` 返回一句
   「已派活、别编造结果」，麦麦据此**用自己的话**告诉用户。
2. **派活后立刻结束本轮。** 返回 `stop_after_execution: true`，不干等。
3. **干完由宿主回调、插件唤醒麦麦。** 麦麦拿到已注入的结果，自己组织措辞。

**追问不重跑。** 完整结果存在宿主账本里，`dsh_task_status` 直接读；
只有明确要求"再去看看"时才用 `dsh_followup` 回去问 —— 而且是回到**同一个会话**。

---

## 前置条件

**必须先部署 [dsh-bridge-host](../dsh-bridge-host)** —— 本插件只是它的调用方。

你还需要：

* 麦麦（MaiBot）已运行，且能访问宿主网桥地址（通常 `172.24.0.1:13081`）；
* 一个与宿主 `DSH_BRIDGE_TOKEN` **一致**的共享令牌。

---

## 安装

麦麦插件的属主通常是容器内的 `root`：

```bash
sudo cp -r plugin /path/to/MaiMBot/plugins/deepseek-v4-pro_dsh-harness
```

然后在麦麦 WebUI → 插件管理中配置（见下），**并务必填写 `allowed_senders`**。

---

## 配置

| 配置项 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 关闭后所有工具直接拒绝 |
| `bridge_host` | `172.24.0.1` | 宿主中转进程地址 |
| `bridge_port` | `13081` | 宿主中转进程端口 |
| `bridge_token` | 空 | **必须**与宿主 `DSH_BRIDGE_TOKEN` 一致 |
| `default_cwd` | `/tmp` | 默认工作目录 |
| **`allowed_senders`** | **空** | **白名单；空 = 谁都不能触发** |
| `enable_write_ops` | `false` | 为 false 时提示词带只读约束前缀 |
| `callback_listen_host` | `0.0.0.0` | 回调监听地址（需让宿主可达） |
| `callback_listen_port` | `13082` | 回调监听端口 |
| `report_style` | `brief` | `brief` 简要 / `detail` 展开 |
| `followup_enabled` | `true` | 是否允许回问 DSH |
| `max_reply_chars` | `1500` | 单条回复截断长度 |

> 宿主要能回调到插件，需设置 `DSH_BRIDGE_CALLBACK_URL=http://<容器IP>:13082/event`。

---

## 工具

| 工具 | 作用 |
|---|---|
| `dsh_dispatch` | **派活**，立即返回；`session_name` 可接续指定会话 |
| `dsh_list_sessions` | 列出工作区与会话；`cwd` 筛选目录，`named_only` 只看能续的 |
| `dsh_task_status` | 查任务完整结果（**回答追问优先用这个**） |
| `dsh_followup` | 回到原会话追问（会真的重跑，有成本） |
| `dsh_ask` | 同步提问（阻塞，仅适合很快的任务） |
| `/dsh list\|new\|ask\|search` | 人工调试入口 |

### 会话：接续之前聊过的内容

会话身份是 `(agent, cwd, name)` 三元组：

* **同名 `session_name` → 接续同一段对话**（助手记得之前聊的）；
* **换名字或换目录 → 全新对话**（互不干扰）；
* 首次使用会自动创建，不需要先建。

想接着某个旧会话干活时，先用 `dsh_list_sessions` 查到它的 `name`。

---

## 安全

宿主上的 agent **能执行任意命令**，所以本插件刻意保守：

* `allowed_senders` **默认为空 = 谁都不能触发**，必须显式配置；
* 聊天内容只当作 **prompt 内容**，绝不解析为对插件或中转进程的指令；
* `enable_write_ops` 默认 `false`；
* `bridge_token` 永不记录到日志；
* 回调监听对每个请求校验令牌，不匹配直接 401。

> **强烈建议**：能触发 Harness 的人必须在白名单里。

---

## 测试

```bash
python3 test/plugin_callback_test.py
```

覆盖回调端的 401 门禁、幂等去重、失败不吞 taskId（保证重试有效）、超长请求拒绝。

> 测试会用桩替掉 `maibot_sdk`，因此**不需要麦麦运行时**即可运行。
> 它只测回调接收端，不触碰真实 bot。

---

## 相关仓库

* **[dsh-bridge-host](../dsh-bridge-host)** —— 宿主侧中转进程，本插件依赖它。

## 许可

MIT
