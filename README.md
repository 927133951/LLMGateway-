# LLM Gateway — 本地统一网关

把任意多个 OpenAI 兼容的 API（中转站、官方接口、本地模型服务……）合并成一个**本地统一入口**：
一个地址、一个 Key，访问所有上游的所有模型。指定 `auto` 模型时自动在全部模型之间**故障切换**，
一个超时就换下一个，直到请求成功为止。

同时支持 **OpenAI 协议** 和 **Claude（Anthropic）协议** 双端点。

---

## 目录

- [特性](#特性)
- [快速开始](#快速开始)
- [文件夹结构](#文件夹结构)
- [控制台使用](#控制台使用)
- [接入你的客户端](#接入你的客户端)
- [API 文档](#api-文档)
- [故障切换机制](#故障切换机制)
- [配置项](#配置项)
- [数据与迁移](#数据与迁移)
- [常见问题](#常见问题)

---

## 特性

| 特性 | 说明 |
|------|------|
| 多上游合并 | 添加任意数量的上游 API，模型列表自动拉取并合并 |
| 统一入口 | 一个本地地址 + 一个网关 Key，替代散落各处的上游 Key |
| auto 模式 | 模型名填 `auto`，按顺序遍历所有上游的所有模型，失败/超时自动切换 |
| 双协议 | OpenAI `/v1/chat/completions` + Claude `/v1/messages`，含流式 SSE |
| 流式转换 | Claude 协议客户端请求时，网关实时把 OpenAI 格式的流翻译成 Anthropic 事件流 |
| 思维链映射 | DeepSeek 风格的 `reasoning_content` 自动映射为 Anthropic 的 `thinking` 块 |
| 工具调用 | `tools` / `tool_calls` ↔ `tool_use` 双向转换，可接 Claude Code 等智能体客户端 |
| 负载轮转 | auto 模式每次请求从不同起点开始遍历，负载自然分散 |
| 零依赖 | 纯 Node.js 原生实现，无需 `npm install` |
| 免安装运行 | 没装 Node？首次启动自动下载便携版到文件夹内，不动系统、不要管理员权限 |
| 可视化控制台 | 浏览器管理上游、查看模型、一键测试 |

---

## 快速开始

### 1. 启动

**Windows**：双击 `start.bat`

**Mac / Linux**：

```bash
cd autokey
node server.js        # 需要 Node.js >= 18
```

启动脚本会自动检测环境：

```
系统已有 Node ≥ 18      → 直接使用
文件夹内有便携版 Node   → 使用 .runtime\node\（不污染系统）
都没有                  → 自动下载官方 LTS 便携版（约 30MB，仅首次）
                        → 下载失败才会提示手动安装
```

### 2. 打开控制台

服务启动后约 2 秒浏览器会自动打开，没弹的话手动访问：

```
http://localhost:4567
```

### 3. 添加上游

在「添加上游 API」区域填入：

| 字段 | 说明 |
|------|------|
| 名称 | 可选，默认取域名 |
| 请求地址 | 如 `https://ai.121628.xyz/v1`（带不带 `/v1` 都行，自动处理） |
| API 密钥 | 该上游的 key，如 `sk-xxxx` |

点击「添加并拉取模型」，成功后立即显示该上游的全部模型和健康状态。

完成。现在任何客户端都可以指向这个网关了。

---

## 文件夹结构

```
autokey/
├── start.bat          # Windows 一键启动（自动检测/配置环境）
├── server.js          # 网关服务端（Node 原生，零依赖）
├── index.html         # 控制台前端页面
├── setup-node.ps1     # 便携版 Node 自动下载脚本（start.bat 调用）
├── providers.json     # 配置与数据（自动生成）★ 备份这个文件 = 备份一切
└── .runtime/          # 便携版 Node（自动生成，可删除，删后会自动重新下载）
```

---

## 控制台使用

浏览器打开 `http://localhost:4567`：

- **网关信息卡片**：显示网关地址和网关 Key，带复制按钮。客户端接入就填这两个。
- **全局系统提示词**：自动注入所有经过网关的请求（双协议都生效），可用于强制中文回复等。
  客户端自带 system 时会合并（网关提示词在前）。留空则完全不注入。保存后立即生效，无需重启。
- **添加上游**：如上所述。地址不规范没关系，末尾的 `/` 和 `/v1` 会自动归一化。
- **已添加的上游**：每个上游一张卡片，显示模型数量或失败原因（红色标记 = 当前连不上）。
  - 「刷新模型」：重新拉取该上游的模型列表
  - 「删除」：移除该上游
- **合并后的模型列表**：所有上游模型的汇总表，「来源」列标明每个模型来自哪个上游。
- **快速测试**：选一个模型（或 auto）、选协议、输入内容直接发送，验证整条链路。

---

## 接入你的客户端

所有客户端只需要三个信息：

```
API 地址:  http://localhost:4567/v1
API Key:   sk-gw-xxxxxxxxxxxxxxxx   （控制台里查看/复制）
模型:      auto                     （或模型列表中的任意具体名称）
```

### OpenAI 协议客户端（绝大多数工具/软件）

- 地址：`http://localhost:4567/v1`
- 认证头：`Authorization: Bearer <网关Key>`
- 端点：`POST /v1/chat/completions`

Python 示例：

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:4567/v1",
    api_key="sk-gw-xxxxxxxxxxxxxxxx",
)

resp = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "你好"}],
    stream=True,
)
for chunk in resp:
    print(chunk.choices[0].delta.content or "", end="")
```

Node.js 示例：

```js
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:4567/v1",
  apiKey: "sk-gw-xxxxxxxxxxxxxxxx",
});

const resp = await client.chat.completions.create({
  model: "auto",
  messages: [{ role: "user", content: "你好" }],
});
console.log(resp.choices[0].message.content);
```

### Claude 协议客户端（Anthropic SDK / Claude Code 等）

- 地址：`http://localhost:4567`（SDK 自己会拼 `/v1/messages`）
- 认证头：`x-api-key: <网关Key>` 或 `Authorization: Bearer <网关Key>` 均可

Python 示例：

```python
from anthropic import Anthropic

client = Anthropic(
    base_url="http://localhost:4567",
    api_key="sk-gw-xxxxxxxxxxxxxxxx",
)

msg = client.messages.create(
    model="auto",
    max_tokens=1024,
    messages=[{"role": "user", "content": "你好"}],
)
print(msg.content)
```

Claude Code 示例（环境变量方式接入）：

```bash
set ANTHROPIC_BASE_URL=http://localhost:4567
set ANTHROPIC_API_KEY=sk-gw-xxxxxxxxxxxxxxxx
claude
```

> 提示：Claude Code 里模型名可以填 `auto`，网关会自己找可用模型。

---

## API 文档

### GET /v1/models — 合并模型列表

返回同时兼容 OpenAI 与 Anthropic 两种格式的超集：

```bash
curl http://localhost:4567/v1/models \
  -H "Authorization: Bearer sk-gw-xxxx"
```

```json
{
  "object": "list",
  "data": [
    {
      "id": "deepseek-v4-flash-free",
      "object": "model",
      "owned_by": "ai.121628",
      "type": "model",
      "display_name": "deepseek-v4-flash-free"
    }
  ],
  "has_more": false
}
```

### POST /v1/chat/completions — OpenAI 协议

请求与响应格式与 OpenAI 完全一致，支持 `stream: true`（SSE 原样透传）。

```bash
curl http://localhost:4567/v1/chat/completions \
  -H "Authorization: Bearer sk-gw-xxxx" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "auto",
    "messages": [{"role": "user", "content": "你好"}]
  }'
```

响应中的 `model` 字段是实际应答的上游模型名。

### POST /v1/messages — Claude 协议

请求为 Anthropic Messages 格式，网关内部转换为 OpenAI 格式发给上游，
再把结果（包括流式事件、thinking 块、tool_use）翻译回 Anthropic 格式。

```bash
curl http://localhost:4567/v1/messages \
  -H "x-api-key: sk-gw-xxxx" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "auto",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "你好"}]
  }'
```

支持的字段映射：

| Claude 侧 | 转换后（OpenAI 侧） |
|-----------|---------------------|
| `system`（字符串或块数组） | system 角色消息 |
| `messages[].content` 文本块 | 文本内容 |
| 图片块（base64 `source`） | `image_url` data URI |
| `max_tokens` / `temperature` / `top_p` | 同名参数 |
| `stop_sequences` | `stop` |
| `tools` + `input_schema` | `tools` + JSON Schema |
| `tool_choice`（auto/any/tool） | `auto` / `required` / 指定函数 |
| `tool_result` 块 | `role:"tool"` 消息 |

流式事件完整输出：`message_start` → `content_block_start` → `content_block_delta`
（`text_delta` / `thinking_delta` / `input_json_delta`）→ `content_block_stop` → `message_delta` → `message_stop`。

### 管理接口（本机控制台用，无需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/models` | 合并模型列表 |
| GET | `/v1/models/:id` | 单个模型详情 |
| POST | `/v1/messages/count_tokens` | Anthropic 令牌计数（估算值，供 SDK 客户端调用） |
| POST | `/v1/chat/completions` | OpenAI 协议对话 |
| POST | `/v1/messages` | Claude 协议对话 |
| GET | `/api/config` | 网关地址、Key、超时参数（仅本机管理用） |
| GET | `/api/providers/list` | 全部上游及模型缓存 |
| POST | `/api/providers` | 添加上游 `{name?, baseUrl, apiKey}`，立即拉取模型 |
| DELETE | `/api/providers/:id` | 删除上游 |
| POST | `/api/providers/:id/refresh` | 强制刷新该上游模型列表 |

### 排查日志

所有 `/v1/*` 请求与每次模型切换的成败都会写入项目目录下的 **`gateway.log`**，
客户端报错时先看这个文件里对应时间点的记录。

---

## 故障切换机制

### 候选顺序怎么来

```
model = "auto"  →  所有上游 × 各自全部模型，平铺成一条候选链
model = 具体名  →  所有提供该模型的上游（同名模型多个来源时会依次尝试）
                   如果没有任何上游声明有这个名字 → 仍然拿原名去每个上游试一遍
```

### 模型健康记忆（大池子不迷路）

几百个模型的池子里，靠"每次从头试"是走不到头的。网关会记住每个模型的表现：

- **失败进冷却池**：失败的模型被雪藏 5 分钟（限流类错误 10 分钟），期间排到队尾不再优先尝试
- **成功者优先**：最近成功过的模型自动排到最前面，后续请求直击可用模型
- **断点续试**：一整轮全失败后，游标停在走过的位置，下次请求从那里继续往深走，
  而不是回到起点重复打转
- 冷却只是降级不是拉黑，时间到了自动回归候选队列

### 单次请求的生命周期

1. **起点轮转**：每次请求候选链的起始位置向前滚动一格，避免永远打头几个模型
2. **逐个尝试**：发出请求，等待响应头，限时 `CONNECT_TIMEOUT_MS`（默认 60s）
3. **失败即切**：连接失败、HTTP 错误、超时 → 记录原因，立刻换下一个
4. **坏响应也算失败**：上游返回 200 但内容是空的、结构不对（如数字型 id）、
   或 JSON 里内嵌 error → 一律判定失败并切换，绝不把垃圾透传给客户端；
   流式请求会先探测首个有效内容块再对客户端提交，之前发现异常照样切换
5. **流式保活**：一旦开始出字，改看"静默时间"，超过 `IDLE_TIMEOUT_MS`（默认 240s）
   无新数据才判定卡死，正常长回答不会被掐断
6. **全部失败**：返回 502，附上最后若干条的失败明细，方便排查

服务端日志会实时打印每次尝试的结果：

```
[fail] upstreamA/gpt-x: timeout after 60000ms
[ok]   ai.121628/deepseek-v4-flash-free (1832ms)
```

---

## 配置项

通过环境变量覆盖默认值（可选）：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PORT` | `4567` | 服务端口 |
| `HOST` | `0.0.0.0` | 监听地址；只允许本机访问改成 `127.0.0.1` |
| `CONNECT_TIMEOUT_MS` | `60000` | 单次尝试的最长等待（到收到响应头为止） |
| `IDLE_TIMEOUT_MS` | `240000` | 流式传输中允许的最大静默时间 |
| `FAIL_COOLDOWN_MS` | `300000` | 模型失败后的雪藏时长（冷却池） |
| `RATE_COOLDOWN_MS` | `600000` | 限流（429）失败的雪藏时长 |

示例：

```bash
set PORT=8080
node server.js
```

---

## 数据与迁移

- 所有上游、Key、模型缓存、网关 Key 都存在 **`providers.json`**
- 服务重启不丢配置；**整个文件夹拷贝到别的设备即可原样运行**
- 网关 Key 跟着配置走，换设备后客户端不用改任何东西
- 想重置网关 Key：编辑 `providers.json`，删除 `"gatewayKey"` 字段，重启后自动生成新的
- 换了网络环境后建议在控制台点一遍各上游的「刷新模型」

---

## 常见问题

**Q：双击 start.bat 黑窗口一闪就没 / 报端口占用**

已经有另一个网关实例在跑了。找到旧的黑色窗口关掉，或者换个端口
（`set PORT=4568` 后再运行）。确认占用情况：任务管理器搜 `node.exe`。

**Q：浏览器没自动弹出控制台**

手动打开 `http://localhost:4567`。

**Q：局域网其他设备想访问这台机器的网关**

- 保持默认监听 `0.0.0.0`
- Windows 防火墙首次弹窗时点「允许」（错过了就在防火墙设置里放行 node.exe 或 4567 端口）
- 其他设备把地址里的 `localhost` 换成本机的局域网 IP（`ipconfig` 查看），如 `http://192.168.x.x:4567/v1`

**Q：某个模型一直失败怎么办**

auto 模式下无所谓——它会被自动跳过。想清理就在控制台删除对应上游，
或让上游方处理。

**Q：为什么响应里的 model 不是我填的名字**

填了 `auto` 时，`model` 字段显示的是最终成功应答的那个真实模型，这是预期行为。

**Q：安全提醒**

网关 Key 等同于你所有上游资产的钥匙，不要把 `providers.json` 或网关 Key
发给别人；不需要局域网访问时可在环境变量里设 `HOST=127.0.0.1` 收紧。

**Q：如何彻底重来**

停止服务 → 删除 `providers.json`（以及可选的 `.runtime`）→ 重新双击 `start.bat`。
