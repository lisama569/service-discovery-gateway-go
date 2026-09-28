package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/backend"
	"github.com/lisama569/service-discovery-gateway-go/config"
	"github.com/lisama569/service-discovery-gateway-go/ratelimit"
	"github.com/lisama569/service-discovery-gateway-go/registry"
)

// TestLoopbackIntegration 在真实回环连接上验证：
// 健康后端 200、全部不可用 503、限流 429、挂起后端超时重试、优雅关停 503。
func TestLoopbackIntegration(t *testing.T) {
	// 1) 三个假后端：两个快，一个挂起（模拟长时间无响应）
	fast1 := backend.New("fast-1", "127.0.0.1:0", 5*time.Millisecond, false)
	fast2 := backend.New("fast-2", "127.0.0.1:0", 5*time.Millisecond, false)
	hang := backend.New("hang", "127.0.0.1:0", 0, true)
	for _, b := range []*backend.Server{fast1, fast2, hang} {
		if err := b.Start(); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _ = fast1.Shutdown(); _ = fast2.Shutdown(); _ = hang.Shutdown() }()

	reg := registry.NewWithClock(10*time.Second, 0, time.Now)
	defer reg.Close()
	regInst := func(s *backend.Server) {
		if err := reg.Register(registry.Instance{
			ID: s.Addr(), Service: "svc", Addr: s.Addr(), Weight: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	regInst(fast1)
	regInst(fast2)
	regInst(hang)

	cfg := &config.Config{
		BackendTimeout: 80 * time.Millisecond,
		MaxRetries:     1,
		Quotas: map[string]config.CallerQuota{
			"limited": {Rate: 0, Burst: 0},
		},
		DefaultQuota: config.CallerQuota{Rate: 100000, Burst: 100000},
	}
	r := New(reg, config.NewHolder(cfg), ratelimit.New())
	r.SetLogger(func(format string, args ...any) { t.Logf("[route] "+format, args...) })
	srv := NewServer("127.0.0.1:0", r)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown() }()

	get := func(caller string) (int, string) {
		req, _ := http.NewRequestWithContext(context.Background(),
			http.MethodGet, "http://"+srv.Addr()+"/svc/svc", nil)
		req.Header.Set("X-Caller", caller)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, resp.Header.Get("X-Instance-Id")
	}

	// 普通调用方：即便首次选中挂起实例，也会在 ~80ms 超时后重试到健康实例
	start := time.Now()
	code, inst := get("normal")
	elapsed := time.Since(start)
	if code != http.StatusOK {
		t.Fatalf("retry to healthy backend must return 200, got %d", code)
	}
	if inst != fast1.Addr() && inst != fast2.Addr() {
		t.Fatalf("must end on a fast backend, got %s", inst)
	}
	t.Logf("[input/decision] 正常请求（可能先打到挂起实例）80ms超时重试 -> 200, 后端=%s, 耗时=%s",
		inst, elapsed.Round(time.Millisecond))

	// 被封禁调用方：429，且完全不会触达后端
	before := fast1.Handled() + fast2.Handled()
	code, _ = get("limited")
	if code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited caller must get 429, got %d", code)
	}
	after := fast1.Handled() + fast2.Handled()
	if after != before {
		t.Fatalf("rejected requests must never reach backends: before=%d after=%d", before, after)
	}
	t.Log("[input/decision] Rate=0 调用方 -> 429，后端处理数不变")

	// 全部实例摘除：503 + ErrNoHealthyInstance
	_ = reg.Deregister(fast1.Addr())
	_ = reg.Deregister(fast2.Addr())
	_ = reg.Deregister(hang.Addr())
	code, _ = get("normal")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("no instances must return 503, got %d", code)
	}
	t.Log("[input/decision] 全部摘除后 -> 503")

	// 重新注册后恢复，再优雅关停网关：新请求 503
	regInst(fast1)
	if code, _ = get("normal"); code != http.StatusOK {
		t.Fatalf("service must recover after re-register, got %d", code)
	}
	r.BeginShutdown()
	if code, _ = get("normal"); code != http.StatusServiceUnavailable {
		t.Fatalf("after BeginShutdown new requests must get 503, got %d", code)
	}
	t.Log("[input/decision] BeginShutdown 后新请求 -> 503 ErrGatewayShutting")
}

// TestConcurrentRoutingStickySafe 在 -race 下对同一会话做并发路由，
// 验证亲和表、负载计数并发安全。
func TestConcurrentRoutingStickySafe(t *testing.T) {
	fb := newFakeBackend()
	cfg := baseCfg()
	cfg.BackendTimeout = 5 * time.Second
	r, reg := newTestRouter(t, cfg, fb)
	regN(t, reg, 2)

	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sess := fmt.Sprintf("s%d", i%5) // 5 个会话，每会话 10 个并发
			if _, err := r.Do(context.Background(), Request{
				Service: "svc", Caller: "a", SessionKey: sess,
			}); err == nil {
				ok.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 50 {
		t.Fatalf("all concurrent requests must succeed, got %d/50", ok.Load())
	}
	t.Logf("[input/decision] 50并发(5会话)全部成功，亲和表/负载计数无竞态")
}
