# inference-output-stream-backpressure-validator

验证流式推理输出、客户端背压和资源释放。

> **网络写入说明**：本仓库中所有“网络写入”均为**本地模拟**。
> `ClientConsumer.write()` 只是把 token 推入内存数组（`writeBuffer` / `sent`），
> 不创建任何 socket、不监听端口、不发起任何真实网络 IO。
> 时间也是模拟的：`session.tick()` 推进逻辑时钟，不使用真实定时器。

## 运行

```bash
node --test        # 运行全部测试（Node.js 24.16.0 验证）
node src/index.js  # 运行本地模拟演示
```

## 模型

针对“客户端读取慢、服务端持续生成、结果队列增长、KV 缓存被占用”的场景，
`InferenceSession` 每 tick 依据以下输入做决策：

- **输入请求**：`totalTokens`（计划生成长度）、`deadline`（tick 截止时间）
- **客户端窗口**：`ClientConsumer.window`，写信用耗尽即背压
- **写缓冲**：`writeBuffer` / `bufferMax`，模拟客户端侧缓冲堆积
- **生成速度**：`generationRate`（每 tick 生成的 token 数）
- **token 队列**：共享输出缓存 `tokens`，深度为 `queueDepth`
- **取消**：`cancel(reason)` 请求终止
- **缓存引用**：`kvCacheRefs`，生成期间持有 KV 缓存，终止时释放

### 每 tick 的决策（优先级从高到低）

| 条件 | 动作 |
| --- | --- |
| 已取消 | `terminate`（原因如 `client-aborted`） |
| 到达 deadline | `terminate('deadline')` |
| `queueDepth >= maxQueueSize` | `terminate('queue-overflow')` |
| 生成完毕且队列与消费者缓冲均排空 | `terminate('completed')` |
| `queueDepth >= highWatermark` | `pause-generation`（记录暂停点） |
| 暂停中且 `queueDepth <= lowWatermark` | `resume-generation` |
| 暂停中 | `paused` |
| 任一在线消费者窗口耗尽或缓冲满 | `continue-buffering` |
| 其他 | `stream` |

### token 状态机

- `queued`：在输出缓存中，可投递
- `released`：所有在线消费者都已越过，缓存已释放
- `dropped`：终止时未发送，附带 `dropReason` / `droppedAt`，状态明确

已发送 token（消费者 `sent` 中的 seq）**不能重发**：重连时从断线游标
`resumeFrom` 续传；若该位置已被缓存释放，记录 `consumer-gap` 证据并从
`baseSeq` 继续，绝不补发已释放/已发送的 token。

### 多消费者共享结果流

多个消费者共享同一输出缓存，各自维护游标。缓存释放条件：seq 小于所有
在线消费者游标的最小值。为防止一个慢客户端无限阻塞其他消费者，落后超过
`maxConsumerLag` 的消费者会被驱逐：其未送达 token 记入 `dropped`，其余
消费者继续推进，缓存随之前移释放。

### 输出缓存增长与不一致证据

- `peakQueueDepth`：队列深度峰值；`pausePoints`：每次暂停的 tick 与深度
- `cacheReleasedTotal`：累计释放的缓存条目；终止时 `kvCacheRefs` 归零
- `evidence` 记录不一致证据：
  - `consumer-gap`：消费者续传位置落后于缓存起点，存在无法补发的空洞
  - `consumer-evicted`：消费者落后过多被驱逐
  - `resend-attempt`：检测到对已发送 token 的重发尝试（防御性检查）

## 测试覆盖（`test/backpressure.test.js`）

- **慢消费者**：窗口耗尽产生背压，队列到高水位暂停生成，恢复读取后继续
- **断线重连**：已发送 token 不重发、未发送 token 续传；缓存已释放位置
  产生 `consumer-gap` 证据
- **部分发送**：取消时队列与写缓冲中未送达 token 状态为 `dropped`，
  KV 缓存引用释放
- **队列上限**：无消费者持续生成，达到 `maxQueueSize` 终止请求
- **多消费者公平性**：落后消费者被驱逐，快消费者收齐全部 token
- **deadline**：超时终止，未发送 token 状态明确

## 文件

- `src/consumer.js`：客户端消费者模拟（窗口、写缓冲、读取）
- `src/session.js`：推理会话（生成、队列、决策、缓存释放、证据）
- `src/index.js`：导出入口与本地演示
