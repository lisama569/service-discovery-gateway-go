// Package gateway 提供服务发现感知的路由与流量调度。
//
// 调度规则：
//   - 新会话只落在 UP 且 Healthy 的实例上，按在途请求数最少优先（least-inflight）。
//   - 会话保持：同一 sessionKey 复用已绑定实例；实例 DRAINING 时存量会话继续，
//     实例 DOWN/摘除/不健康时才改绑其他实例。
//   - 后端超时快速失败，不重试；仅连接类错误（实例刚下线）换实例有限重试，
//     且整个 Route 只扣一次配额，避免重试放大压力。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/internal/limiter"
	"github.com/lisama569/service-discovery-gateway-go/internal/registry"
)

var (
	// ErrNoHealthyInstance 表示当前没有可调度实例。
	ErrNoHealthyInstance = errors.New("gateway: no healthy instance")
	// ErrBackendTimeout 表示后端长时间无响应，快速失败。
	ErrBackendTimeout = errors.New("gateway: backend timeout")
	// ErrServiceUnknown 表示服务未注册。
	ErrServiceUnknown = errors.New("gateway: service unknown")
)

// Transport 抽象到后端的调用，便于测试替换。
type Transport interface {
	// Do 向 addr 上的实例发起一次请求，返回实例标识。
	Do(ctx context.Context, addr string) (string, error)
}

// HTTPTransport 通过 HTTP 调用回环假后端。
type HTTPTransport struct {
	Client *http.Client
}

// NewHTTPTransport 创建默认 HTTP 传输。
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{Client: &http.Client{}}
}

// Do 实现 Transport。
func (t *HTTPTransport) Do(ctx context.Context, addr string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/work", nil)
	if err != nil {
		return "", err
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("backend %s status %d", addr, resp.StatusCode)
	}
	return string(body), nil
}

// Options 是网关配置。
type Options struct {
	Registry  *registry.Registry
	Limiter   *limiter.Limiter
	Transport Transport
	// BackendTimeout 为后端响应超时，超时快速失败。
	BackendTimeout time.Duration
	// MaxRetries 为可重试错误（如连接拒绝）的最大重试次数，避免放大压力。
	MaxRetries int
}

// Gateway 是流量调度入口。
type Gateway struct {
	reg      *registry.Registry
	lim      *limiter.Limiter
	tr       Transport
	timeout  time.Duration
	maxRetry int

	mu       sync.Mutex
	sessions map[string]string        // service|sessionKey -> instanceID
	inflight map[string]*atomic.Int64 // instanceID -> 在途请求数

	statsRouted   atomic.Int64
	statsRejected atomic.Int64
	statsRetries  atomic.Int64
}

// New 创建网关。
func New(opts Options) *Gateway {
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	return &Gateway{
		reg:      opts.Registry,
		lim:      opts.Limiter,
		tr:       opts.Transport,
		timeout:  opts.BackendTimeout,
		maxRetry: opts.MaxRetries,
		sessions: make(map[string]string),
		inflight: make(map[string]*atomic.Int64),
	}
}

// Route 为 caller 对 service 的一次请求做限流、选实例并调用后端。
// sessionKey 非空时启用会话保持：同一会话尽量落在同一实例。
func (g *Gateway) Route(caller, service, sessionKey string) (instanceID string, err error) {
	// 1. 限流：整个请求只扣一次配额，重试不重复扣。
	if err := g.lim.Allow(caller); err != nil {
		g.statsRejected.Add(1)
		return "", err
	}

	snaps, err := g.reg.List(service)
	if err != nil {
		g.statsRejected.Add(1)
		if errors.Is(err, registry.ErrServiceNotFound) {
			return "", fmt.Errorf("%w: %q", ErrServiceUnknown, service)
		}
		return "", err
	}

	sessKey := service + "|" + sessionKey
	exclude := map[string]bool{}
	var lastErr error
	for attempt := 0; attempt <= g.maxRetry; attempt++ {
		inst, err := g.pick(snaps, sessKey, sessionKey != "", exclude)
		if err != nil {
			g.statsRejected.Add(1)
			return "", err
		}
		id, err := g.call(inst)
		if err == nil {
			g.statsRouted.Add(1)
			return id, nil
		}
		lastErr = err
		// 超时/无响应：快速失败，绝不重试，避免放大后端压力。
		if errors.Is(err, ErrBackendTimeout) {
			g.statsRejected.Add(1)
			return "", err
		}
		// 连接类错误（实例可能刚下线）：解除会话绑定，换实例有限重试。
		exclude[inst.ID] = true
		g.unbind(sessKey, inst.ID)
		g.statsRetries.Add(1)
	}
	g.statsRejected.Add(1)
	return "", fmt.Errorf("route %s/%s failed after retries: %w", service, sessionKey, lastErr)
}

// pick 选择实例：优先复用会话绑定，否则在健康实例中按在途数最少选取。
func (g *Gateway) pick(snaps []registry.Snapshot, sessKey string, sticky bool, exclude map[string]bool) (registry.Snapshot, error) {
	byID := make(map[string]registry.Snapshot, len(snaps))
	for _, s := range snaps {
		byID[s.ID] = s
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if sticky {
		if boundID, ok := g.sessions[sessKey]; ok && !exclude[boundID] {
			if s, ok := byID[boundID]; ok && s.Healthy && s.Status != registry.StatusDown {
				// 存量会话：UP 或 DRAINING 都继续复用（优雅下线收尾）。
				return s, nil
			}
			delete(g.sessions, sessKey)
		}
	}
	// 新会话候选：UP 且 Healthy，且未被本次重试排除。
	var best *registry.Snapshot
	var bestLoad int64
	for i := range snaps {
		s := snaps[i]
		if exclude[s.ID] || !s.Healthy || s.Status != registry.StatusUp {
			continue
		}
		load := g.loadOf(s.ID)
		if best == nil || load < bestLoad || (load == bestLoad && rand.Intn(2) == 0) {
			sc := s
			best = &sc
			bestLoad = load
		}
	}
	if best == nil {
		return registry.Snapshot{}, fmt.Errorf("%w (service candidates=%d)", ErrNoHealthyInstance, len(snaps))
	}
	if sticky {
		g.sessions[sessKey] = best.ID
	}
	return *best, nil
}

func (g *Gateway) call(inst registry.Snapshot) (string, error) {
	counter := g.counterOf(inst.ID)
	counter.Add(1)
	defer counter.Add(-1)
	ctx := context.Background()
	cancel := func() {}
	if g.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, g.timeout)
	}
	defer cancel()
	id, err := g.tr.Do(ctx, inst.Addr)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return "", fmt.Errorf("%w: instance %s exceeded %s", ErrBackendTimeout, inst.ID, g.timeout)
		}
		return "", fmt.Errorf("instance %s unreachable: %w", inst.ID, err)
	}
	return id, nil
}

// unbind 解除会话与故障实例的绑定（仅当仍绑定该实例）。
func (g *Gateway) unbind(sessKey, instID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sessions[sessKey] == instID {
		delete(g.sessions, sessKey)
	}
}

func (g *Gateway) counterOf(id string) *atomic.Int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.inflight[id]
	if !ok {
		c = &atomic.Int64{}
		g.inflight[id] = c
	}
	return c
}

func (g *Gateway) loadOf(id string) int64 {
	if c, ok := g.inflight[id]; ok {
		return c.Load()
	}
	return 0
}

// isTimeout 识别网络层超时（如 i/o timeout）。
func isTimeout(err error) bool {
	var nerr net.Error
	if errors.As(err, &nerr) {
		return nerr.Timeout()
	}
	return false
}

// Stats 返回观测数据。
func (g *Gateway) Stats() Stats {
	return Stats{
		Routed:   g.statsRouted.Load(),
		Rejected: g.statsRejected.Load(),
		Retries:  g.statsRetries.Load(),
	}
}

// Stats 是网关运行统计。
type Stats struct {
	Routed   int64
	Rejected int64
	Retries  int64
}
