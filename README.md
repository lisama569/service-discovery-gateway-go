# 服务发现与流量调度网关（Go，本地可跑）

一个零外部依赖的本地演示系统：用**进程内注册中心**模拟服务发现，用**回环地址（127.0.0.1）上的假 HTTP 后端**模拟真实实例，
完整演示实例注册/注销/心跳、心跳过期剔除、健康与负载调度、会话亲和、按调用方限流、配置热更新与优雅下线。

## 目录结构

| 包 | 职责 |
|---|---|
| `registry` | 进程内注册中心：注册（幂等）、注销、心跳、TTL 过期扫描、优雅下线状态机 |
| `gateway` | 路由核心：限流判定 → 会话亲和 → 最少连接选实例 → 转发 → 超时/受控重试；HTTP 网关入口 |
| `ratelimit` | 按调用方的令牌桶，配额热更新 |
| `config` | 网关配置的原子热更新（非法配置拒绝，旧配置继续生效） |
| `backend` | 回环假后端：可配延迟、可挂起（模拟无响应）、统计在途/完成数、可优雅关停 |
| `cmd/gateway` | 可直接运行的端到端演示 |

## 一、注册与心跳协议

实例信息：`{ID, Service, Addr, Weight, Status, LastBeat, UpdatedAt}`，`Status` 三态：

- `up`：正常，可接新流量；
- `draining`：优雅下线中，**不接新流量**，但已绑定它的旧会话仍可路由过去，直到在途请求收尾；
- `down`：已死亡（心跳超时被扫描器标记）或从未复活，不可路由。

协议规则：

1. **注册 `Register`**：以 `ID` 为幂等键。重复注册同一 ID 不报错，而是刷新地址/权重并把心跳时间更新为当前时刻；
   无论实例之前是 `up` 还是已被标记 `down`，重新注册即恢复为 `up`（实例重启复用 ID 的场景）。
   `ID` 或 `Service` 为空返回 `ErrInvalidInstance`。
2. **心跳 `Heartbeat`**：刷新 `LastBeat`。`down` 或不存在的 ID 返回 `ErrInstanceNotFound`；
   `draining` 中的实例仍允许心跳（下线期间保活，让旧会话能正常收尾）。
3. **过期判定**：`now - LastBeat > TTL` 即视为不健康：`List` 不再返回它，后台扫描协程（间隔 `sweep`）把它标记为 `down`。
4. **主动注销 `Deregister`**：从注册中心彻底删除；对不存在的 ID 返回 `ErrInstanceNotFound`（不静默成功）。
5. **优雅下线 `BeginDrain`**：置为 `draining`。推荐顺序：`BeginDrain(id)` → 等在途收尾 → `Deregister(id)`。

### 失败原因区分（不糊成一种错误）

| 错误 | 触发条件 |
|---|---|
| `registry.ErrInvalidInstance` | 注册参数非法 |
| `registry.ErrInstanceNotFound` | 注销/心跳/下线一个不存在或已 down 的实例 |
| `registry.ErrServiceNotFound` | 查询一个从未注册过的服务 |
| `gateway.ErrNoHealthyInstance` | 服务存在但当前没有健康实例（含服务不存在，底层错误被包装） |
| `ratelimit.ErrRateLimited` | 调用方配额不足 |
| `gateway.ErrBackendTimeout` | 后端在 `BackendTimeout` 内无响应 |
| `gateway.ErrBackendUnreachable` | 连接拒绝/中断/5xx 等传输类失败 |
| `gateway.ErrRetryExhausted` | 允许的尝试次数用尽（与最后一次具体原因 `errors.Join`，可同时 `errors.Is`） |
| `gateway.ErrGatewayShutting` | 网关优雅下线期间到达的新请求 |

网关 HTTP 状态码映射：限流 → `429`，无健康实例/正在关停 → `503`，重试耗尽 → `502`。

## 二、调度规则

请求 `Request{Service, Caller, SessionKey}`，处理流水线：

1. 网关处于关停中 → 立即返回 `ErrGatewayShutting`，不排队；
2. 进入即在途计数 `+1`（defer `-1`，优雅下线靠它等待收尾）；
3. 取一次配置快照——**热更新不影响已经开始的请求**；
4. 限流判定（见下）；
5. 选实例：
   - **会话亲和**：带 `SessionKey` 时，若上次绑定的实例仍 `up`，继续用；
     若该实例正在 `draining` 但心跳未过期，旧会话仍路由到它收尾；
     若实例已消失/过期，改选新实例并重建亲和关系。
   - **最少连接（按权重）**：在健康实例中选 `在途请求数 / Weight` 最小者，扩缩容后负载自动重新均衡。
6. 转发：每次尝试都包一个 `BackendTimeout` 的 context，**超时快速失败**；
7. 重试：只有 `ErrBackendTimeout`/`ErrBackendUnreachable` 这类传输失败才重试；
   最多尝试 `1 + MaxRetries` 次，重试绝不打同一实例（`tried` 集合排除），
   因此**重试不会对故障实例放大压力**；耗尽返回 `ErrRetryExhausted` + 最后一次原因。

> 说明：调度依据（输入参数、选中实例、在途数、是否 draining、尝试序列、失败原因）全部通过 `Router.SetLogger` 输出，
> 演示程序和单测都会打印，路由结果可复现、可解释。

## 三、限流规则

- 每个 `Caller` 一个独立令牌桶：容量 `Burst`，按 `Rate` 个/秒补充；
- 判定在互斥锁内一次完成“补充 + 扣减”，并发下每个请求恰好扣一次令牌，**不丢失、不重复**；
- `Rate=0, Burst=0` 表示完全封禁；
- 未配置的调用方用 `DefaultQuota`；
- **热更新**：某调用方配额变化后其旧桶按新参数重建，余量 `min(旧余量, 新Burst)`——既不偷放也不多扣；
  新请求一律按新规则判定。配置非法（超时为 0、负数等）时 `Update` 返回错误并拒绝替换，线上继续沿用旧配置；
- 配置 map 在存储时深拷贝，调用方事后修改原 map 不影响网关。

## 四、优雅下线语义

- **实例下线**：`BeginDrain` 后健康列表立即排除它（新会话/无会话请求不会选中），旧会话仍可打到它；`Deregister` 后旧会话漂移到健康实例并重新固定。
- **网关下线**：`BeginShutdown` 后新请求立刻收到 `ErrGatewayShutting`/503；`WaitInflight` 等在途请求收尾（带超时），
  之后才关闭 HTTP listener。配置热更新与下线期间，已经开始处理的请求持旧配置快照继续跑完。

## 五、本地验证

需要 Go 1.25+，无需任何外部服务或端口配置（均使用 `127.0.0.1:0` 随机端口）。

```bash
# 端到端演示：亲和、限流热更新、实例优雅下线、网关关停
go run ./cmd/gateway

# 全部单测（含并发注册注销、心跳过期、热更新、优雅下线等场景）
go test ./...

# 带竞态检测反复跑（推荐）
go test -race -count=3 ./...
```

### 测试覆盖一览

| 测试 | 验证点 |
|---|---|
| `registry.TestRegisterIdempotent` | 重复注册幂等并刷新元数据 |
| `registry.TestHeartbeatExpiry` / `TestSweepLoopMarksExpired` | 停心跳超时后不可路由、被标记 down、错误分类、扫描协程生效 |
| `registry.TestConcurrentRegisterDeregister` | 16 goroutine 并发注册/注销无脏数据 |
| `registry.TestDrainNotInHealthyList` | draining 不接新流量但可续心跳 |
| `ratelimit.TestConcurrentCountNoLossNoDouble` | 2000 次并发判定总数精确、放行数恰为 burst |
| `ratelimit.TestHotReloadAppliesToNewRequests` | 配额热更新后立即按新桶判定 |
| `gateway.TestStickySessionPinned` / `TestDrainingStickyStillServed` | 会话固定与下线期旧会话收尾、摘除后漂移重固定 |
| `gateway.TestLeastLoadedSpreads` | 9 个并发请求严格 3/3/3 摊开 |
| `gateway.TestTimeoutFailFastAndRetry` / `TestRetryExhaustedNoAmplification` | 80ms 超时快速失败、总尝试数封顶不放大 |
| `gateway.TestGracefulShutdownInflightFinishes` | 关停期新请求被拒、在途请求正常收尾 |
| `gateway.TestHotReloadInFlightUsesOldNewRequestsUseNew` | 在途请求按旧规则收尾，新请求按新规则 |
| `gateway.TestLoopbackIntegration` | 真实回环 HTTP：200/429/503/502 状态码与重试端到端行为 |

每个测试都用 `t.Logf` 打印输入与判定依据，用 `-v` 可查看完整决策日志，例如：

```
[route] input service="svc" caller="alice" session="s1" timeout=100ms maxRetries=1
[route] decision=forward attempt=1/2 instance="inst-3" inflight=1 draining=true
[route] decision=success instance="inst-3" attempts=1
```
