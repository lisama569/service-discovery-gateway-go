// Package gateway 实现服务发现之上的流量调度网关。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/config"
	"github.com/lisama569/service-discovery-gateway-go/ratelimit"
	"github.com/lisama569/service-discovery-gateway-go/registry"
)

// 路由失败原因分类，调用方可用 errors.Is 精确区分。
var (
	// ErrGatewayShutting 网关正在优雅下线，拒绝新请求。
	ErrGatewayShutting = errors.New("gateway: shutting down, rejecting new requests")
	// ErrNoHealthyInstance 没有可接流量的健康实例（含服务不存在）。
	ErrNoHealthyInstance = errors.New("gateway: no healthy instance available")
	// ErrBackendTimeout 后端在配置超时内没有响应，快速失败。
	ErrBackendTimeout = errors.New("gateway: backend timeout")
	// ErrBackendUnreachable 后端不可达（连接被拒/连接中断等传输错误）。
	ErrBackendUnreachable = errors.New("gateway: backend unreachable")
	// ErrRetryExhausted 允许的重试次数用完仍失败。
	ErrRetryExhausted = errors.New("gateway: retries exhausted")
)

// Logger 用于打印输入与判定依据（测试可接 t.Logf）。
type Logger func(format string, args ...any)

// forwarder 抽象一次后端转发，测试可用假实现替代真实 HTTP。
type forwarder func(ctx context.Context, inst registry.Instance) error

// Router 负责选实例、保持会话亲和、转发请求与失败处理。
type Router struct {
	reg     *registry.Registry
	cfg     *config.Holder
	limiter *ratelimit.Limiter
	forward forwarder
	log     Logger

	inflight atomic.Int64 // 网关在途请求计数
	shutting atomic.Bool  // 优雅下线标志

	mu     sync.Mutex
	sticky map[string]string        // sessionKey -> instanceID
	load   map[string]*atomic.Int64 // instanceID -> 在途请求数
}

// httpClient 复用连接；超时由每次请求的 ctx 控制。
var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 100,
	},
}

// defaultForward 通过回环 HTTP 转发到假后端。
func defaultForward(ctx context.Context, inst registry.Instance) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+inst.Addr+"/", nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrBackendTimeout
		}
		return fmt.Errorf("%w: %v", ErrBackendUnreachable, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: status %d", ErrBackendUnreachable, resp.StatusCode)
	}
	return nil
}

// New 创建路由器。
func New(reg *registry.Registry, cfg *config.Holder, limiter *ratelimit.Limiter) *Router {
	return &Router{
		reg:     reg,
		cfg:     cfg,
		limiter: limiter,
		forward: defaultForward,
		log:     func(string, ...any) {},
		sticky:  make(map[string]string),
		load:    make(map[string]*atomic.Int64),
	}
}

// SetLogger 注入日志函数。
func (g *Router) SetLogger(l Logger) {
	if l != nil {
		g.log = l
	}
}

// setForward 注入转发实现（测试用）。
func (g *Router) setForward(f forwarder) { g.forward = f }

// counter 取（懒创建）实例级在途计数器。
func (g *Router) counter(id string) *atomic.Int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.load[id]
	if !ok {
		c = new(atomic.Int64)
		g.load[id] = c
	}
	return c
}

// pickLeastLoaded 按 在途数/权重 最小挑选健康实例。
func pickLeastLoaded(list []registry.Instance, load map[string]*atomic.Int64) registry.Instance {
	best := list[0]
	bestRatio := loadOf(load, best.ID) / float64(best.Weight)
	for _, inst := range list[1:] {
		ratio := loadOf(load, inst.ID) / float64(inst.Weight)
		if ratio < bestRatio {
			best, bestRatio = inst, ratio
		}
	}
	return best
}

func loadOf(load map[string]*atomic.Int64, id string) float64 {
	if c, ok := load[id]; ok {
		return float64(c.Load())
	}
	return 0
}

// choose 选出本次请求要尝试的实例，并返回排除集合（避免重试打到同一实例）。
// 有会话亲和时：上次实例仍健康则继续用；正在 draining 但存活也继续用（旧会话收尾）；
// 已消失/过期则改选新实例并刷新亲和关系。
func (g *Router) choose(service, sessionKey string) (registry.Instance, []registry.Instance, error) {
	if sessionKey != "" {
		g.mu.Lock()
		sid := g.sticky[sessionKey]
		g.mu.Unlock()
		if sid != "" {
			healthy, drainingSticky := g.reg.ListForSticky(service, sid)
			if drainingSticky != nil {
				return *drainingSticky, healthy, nil
			}
			for _, inst := range healthy {
				if inst.ID == sid {
					// 其余健康实例作为重试候选
					rest := make([]registry.Instance, 0, len(healthy)-1)
					for _, other := range healthy {
						if other.ID != sid {
							rest = append(rest, other)
						}
					}
					return inst, rest, nil
				}
			}
			// 亲和实例没了：落入正常选择，稍后刷新亲和表
		}
	}
	list, err := g.reg.List(service)
	if err != nil {
		return registry.Instance{}, nil, fmt.Errorf("%w: %v", ErrNoHealthyInstance, err)
	}
	if len(list) == 0 {
		return registry.Instance{}, nil, ErrNoHealthyInstance
	}
	inst := pickLeastLoaded(list, g.snapshotLoad())
	rest := make([]registry.Instance, 0, len(list)-1)
	for _, other := range list {
		if other.ID != inst.ID {
			rest = append(rest, other)
		}
	}
	return inst, rest, nil
}

func (g *Router) snapshotLoad() map[string]*atomic.Int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	cp := make(map[string]*atomic.Int64, len(g.load))
	for k, v := range g.load {
		cp[k] = v
	}
	return cp
}

// rememberSticky 记录/刷新会话 -> 实例 亲和关系。
func (g *Router) rememberSticky(sessionKey, instanceID string) {
	if sessionKey == "" {
		return
	}
	g.mu.Lock()
	g.sticky[sessionKey] = instanceID
	g.mu.Unlock()
}

func (g *Router) forgetSticky(sessionKey string) {
	if sessionKey == "" {
		return
	}
	g.mu.Lock()
	delete(g.sticky, sessionKey)
	g.mu.Unlock()
}

// Request 描述一次待调度请求。
type Request struct {
	Service    string
	Caller     string
	SessionKey string // 非空时尽量保持同会话固定到同一实例
}

// Result 记录路由判定依据，便于测试与日志解释。
type Result struct {
	InstanceID string
	Retried    int
	Attempts   []string
}

// Do 处理一次请求：下线检查 -> 配置快照 -> 限流判定 -> 选实例 -> 转发，
// 后端超时快速失败，重试次数受控、且不重复打同一实例，避免放大压力。
func (g *Router) Do(ctx context.Context, req Request) (*Result, error) {
	if g.shutting.Load() {
		return nil, ErrGatewayShutting
	}
	// 进入即计入在途；优雅下线时存量请求靠这个计数等待收尾。
	g.inflight.Add(1)
	defer g.inflight.Add(-1)

	cfg := g.cfg.Get() // 请求开始时取快照，配置中途热更新不影响本请求
	g.log("input service=%q caller=%q session=%q timeout=%s maxRetries=%d",
		req.Service, req.Caller, req.SessionKey, cfg.BackendTimeout, cfg.MaxRetries)

	if err := g.limiter.Allow(req.Caller, cfg); err != nil {
		g.log("decision=reject reason=rate_limited caller=%q", req.Caller)
		return nil, err
	}

	inst, rest, err := g.choose(req.Service, req.SessionKey)
	if err != nil {
		g.log("decision=reject reason=no_instance service=%q", req.Service)
		return nil, err
	}

	tried := map[string]bool{inst.ID: true}
	attempts := []string{inst.ID}
	var lastErr error

	maxAttempts := 1 + cfg.MaxRetries
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// 重试：从剩余健康实例里按负载挑一个，绝不重复打同一实例
			next := pickRetry(rest, tried, g.snapshotLoad())
			if next == nil {
				break
			}
			inst = *next
			tried[inst.ID] = true
			attempts = append(attempts, inst.ID)
		}
		c := g.counter(inst.ID)
		c.Add(1)
		g.log("decision=forward attempt=%d/%d instance=%q inflight=%d draining=%v",
			attempt+1, maxAttempts, inst.ID, c.Load(), inst.Status == registry.StatusDraining)

		cctx, cancel := context.WithTimeout(ctx, cfg.BackendTimeout)
		err := g.forward(cctx, inst)
		cancel()
		c.Add(-1)

		if err == nil {
			g.rememberSticky(req.SessionKey, inst.ID)
			g.log("decision=success instance=%q attempts=%d", inst.ID, len(attempts))
			return &Result{InstanceID: inst.ID, Retried: attempt, Attempts: attempts}, nil
		}
		lastErr = err
		g.log("attempt=%d instance=%q failed: %v", attempt+1, inst.ID, err)
		if !isRetryable(err) {
			break
		}
		// 亲和实例失败：忘掉旧关系，重试选中的新实例会重建亲和
		if req.SessionKey != "" && attempt == 0 {
			g.forgetSticky(req.SessionKey)
		}
	}
	// 用 Join 同时保留“重试耗尽”和最后一次失败的具体原因，
	// 调用方可分别用 errors.Is 区分超时、不可达等。
	return nil, errors.Join(ErrRetryExhausted, lastErr)
}

// isRetryable 只有后端传输类失败才重试；限流、无实例等不在此处。
func isRetryable(err error) bool {
	return errors.Is(err, ErrBackendTimeout) || errors.Is(err, ErrBackendUnreachable)
}

// pickRetry 从未试过的候选中按最少连接选一个。
func pickRetry(rest []registry.Instance, tried map[string]bool, load map[string]*atomic.Int64) *registry.Instance {
	var best *registry.Instance
	bestRatio := 0.0
	for i := range rest {
		inst := &rest[i]
		if tried[inst.ID] {
			continue
		}
		ratio := loadOf(load, inst.ID) / float64(inst.Weight)
		if best == nil || ratio < bestRatio {
			cp := *inst
			best = &cp
			bestRatio = ratio
		}
	}
	return best
}

// BeginShutdown 网关优雅下线：在途请求收尾，拒绝新请求。
func (g *Router) BeginShutdown() {
	g.shutting.Store(true)
}

// Inflight 返回当前在途请求数。
func (g *Router) Inflight() int64 { return g.inflight.Load() }

// WaitInflight 等待在途请求全部结束，超时返回 false。
func (g *Router) WaitInflight(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if g.inflight.Load() == 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return g.inflight.Load() == 0
}
