# service-discovery-gateway-go

进程内注册中心 + 回环假后端 + 流量调度网关的本地可跑实现，用于复现和解释：
实例上下线、心跳过期、健康变化、配置热更新、优雅下线与并发路由。

## 结构

```
internal/registry   注册中心：注册(幂等)/注销/心跳/过期剔除/优雅下线标记
internal/limiter      限流器：按调用方固定窗口配额，支持配置热更新
internal/gateway    网关：限流 → 服务发现 → 选实例(会话保持+最少在途) → 调用后端
internal/backend    回环地址(127.0.0.1)上的假后端 HTTP 服务
cmd/gateway         端到端演示程序
```

## 注册与心跳协议

实例以 `(Service, ID)` 为唯一键，通过 `Registry` 交互：

| 操作 | 语义 | 失败原因（`errors.Is` 可区分） |
|---|---|---|
| `Register(inst)` | 注册；同键重复注册**幂等**：刷新地址/健康/元数据与心跳，不重复计数。DRAINING 中重注册不会把状态拉回 UP | `ErrInvalidInstance`（service/id/addr 为空） |
| `Heartbeat(svc, id)` | 刷新心跳时间戳 | `ErrServiceNotFound` / `ErrInstanceNotFound` / `ErrHeartbeatExpired`（已超 TTL） |
| `Deregister(svc, id)` | 主动注销，立即摘除，不再参与调度 | `ErrServiceNotFound` / `ErrInstanceNotFound` |
| `SetHealthy(svc, id, ok)` | 上报健康状态，不健康实例不接新流量 | 同上 |
| `BeginDrain(svc, id)` | 进入 DRAINING：不接**新会话**，存量粘性会话继续直到摘除 | `ErrInstanceDraining`（重复 drain） |
| `Sweep()` | 剔除心跳超过 TTL 的实例（后台协程按 TTL/2 周期自动执行） | — |

实例状态机：`UP → DRAINING → (Deregister/Sweep) 摘除`；心跳超时由后台清理直接摘除。

## 调度与限流规则

**限流（每次请求先过限流，整个请求只扣一次配额，重试不重复扣）：**

- 每调用方固定窗口：`Interval` 内最多 `Permits` 次，超限返回 `ErrRateLimited`。
- 未知调用方：`AllowUnknown=false` 返回 `ErrUnknownCaller`；为 true 按 `DefaultRule` 限流。
- 热更新 `Update(cfg)`：**规则未变**的调用方保留当前窗口计数（防止热更新被用来刷配额）；规则变化/新增/删除的调用方重置窗口，新请求立即按新规则判定。
- 计数与配置读写同锁，并发下不丢不重（`TestConcurrentCountingExact` 验证 32 goroutine × 50 次精确等于配额）。

**调度：**

- 候选实例：`Status==UP && Healthy`；DRAINING 实例仅供存量粘性会话收尾。
- 负载感知：按网关在途请求数最少优先（least-inflight），并列随机。
- 会话保持：`sessionKey` 非空时绑定实例；实例 DRAINING 时存量会话**不迁移**；实例摘除/不健康时才改绑。
- 快速失败：后端超过 `BackendTimeout` 无响应 → `ErrBackendTimeout`，**不重试**（避免放大对故障后端的压力）。
- 有限重试：仅连接类错误（实例刚下线、连接拒绝）换实例重试，最多 `MaxRetries` 次。
- 无可用实例 → `ErrNoHealthyInstance`；服务未注册 → `ErrServiceUnknown`。

## 本地验证

```bash
# 全部单测（含 -race），日志打印输入与判定依据
go test ./... -v -race

# 端到端演示：注册→心跳过期剔除→优雅下线→限流热更新
go run ./cmd/gateway
```

测试覆盖：

- `internal/registry`：幂等重注册、并发注册/注销/心跳（16×200 轮）、心跳过期剔除、
  持续心跳保活、错误原因区分（`ErrInvalidInstance`/`ErrServiceNotFound`/
  `ErrInstanceNotFound`/`ErrHeartbeatExpired`/`ErrInstanceDraining`）、DRAINING 生命周期。
- `internal/limiter`：配额内放行/超限拒绝、未知调用方两种策略、并发计数精确性、
  热更新保留/重置窗口、收紧配额立即生效、窗口到期重置。
- `internal/gateway`：健康路由、无健康实例、限流拒绝、会话保持、
  DRAINING 存量会话收尾 + 新会话改道 + 摘除后改绑、后端 hang 快速失败（0 重试）、
  死实例有限重试且配额不被重复扣、并发路由（32×50 全部成功）。
