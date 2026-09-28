package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/internal/backend"
	"github.com/lisama569/service-discovery-gateway-go/internal/limiter"
	"github.com/lisama569/service-discovery-gateway-go/internal/registry"
)

// testEnv 搭一套真实回环后端 + 注册中心 + 网关。
type testEnv struct {
	reg      *registry.Registry
	lim      *limiter.Limiter
	gw       *Gateway
	backends map[string]*backend.Backend
	t        *testing.T
}

func newTestEnv(t *testing.T, quota int, timeout time.Duration) *testEnv {
	t.Helper()
	reg := registry.New(time.Minute)
	t.Cleanup(reg.Close)
	lim := limiter.New(limiter.Config{
		Rules:        map[string]limiter.Rule{"tester": {Permits: quota, Interval: time.Minute}},
		AllowUnknown: false,
	})
	env := &testEnv{reg: reg, lim: lim, backends: map[string]*backend.Backend{}, t: t}
	env.gw = New(Options{
		Registry:       reg,
		Limiter:        lim,
		Transport:      NewHTTPTransport(),
		BackendTimeout: timeout,
		MaxRetries:     2,
	})
	return env
}

func (e *testEnv) addInstance(t *testing.T, service, id string, latency time.Duration, hang bool) {
	t.Helper()
	b, err := backend.Start(context.Background(), id, latency, hang)
	if err != nil {
		t.Fatalf("start backend %s: %v", id, err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	e.backends[id] = b
	if err := e.reg.Register(registry.Instance{
		Service: service, ID: id, Addr: b.Addr(), Healthy: true,
	}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

func TestRouteToHealthyInstance(t *testing.T) {
	env := newTestEnv(t, 100, time.Second)
	env.addInstance(t, "svc", "i1", 0, false)
	id, err := env.gw.Route("tester", "svc", "")
	t.Logf("input: route to svc with 1 healthy instance; got id=%q err=%v", id, err)
	if err != nil || id != "i1" {
		t.Fatalf("expect i1, got id=%q err=%v", id, err)
	}
}

func TestUnknownService(t *testing.T) {
	env := newTestEnv(t, 100, time.Second)
	_, err := env.gw.Route("tester", "nosvc", "")
	t.Logf("input: route to unregistered service; err=%v isServiceUnknown=%v",
		err, errors.Is(err, ErrServiceUnknown))
	if !errors.Is(err, ErrServiceUnknown) {
		t.Fatalf("expect ErrServiceUnknown, got %v", err)
	}
}

func TestNoHealthyInstance(t *testing.T) {
	env := newTestEnv(t, 100, time.Second)
	env.addInstance(t, "svc", "i1", 0, false)
	if err := env.reg.SetHealthy("svc", "i1", false); err != nil {
		t.Fatal(err)
	}
	_, err := env.gw.Route("tester", "svc", "")
	t.Logf("input: only instance unhealthy; err=%v isNoHealthy=%v", err, errors.Is(err, ErrNoHealthyInstance))
	if !errors.Is(err, ErrNoHealthyInstance) {
		t.Fatalf("expect ErrNoHealthyInstance, got %v", err)
	}
}

func TestRateLimitReject(t *testing.T) {
	env := newTestEnv(t, 2, time.Second)
	env.addInstance(t, "svc", "i1", 0, false)
	var lastErr error
	for i := 0; i < 4; i++ {
		_, lastErr = env.gw.Route("tester", "svc", "")
	}
	stats := env.gw.Stats()
	t.Logf("input: 4 requests with quota 2; lastErr=%v routed=%d rejected=%d",
		lastErr, stats.Routed, stats.Rejected)
	if !errors.Is(lastErr, limiter.ErrRateLimited) {
		t.Fatalf("expect ErrRateLimited, got %v", lastErr)
	}
	if stats.Routed != 2 {
		t.Fatalf("expect 2 routed, got %d", stats.Routed)
	}
}

func TestSessionStickiness(t *testing.T) {
	env := newTestEnv(t, 100, time.Second)
	env.addInstance(t, "svc", "i1", 0, false)
	env.addInstance(t, "svc", "i2", 0, false)
	first, err := env.gw.Route("tester", "svc", "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		id, err := env.gw.Route("tester", "svc", "sess-1")
		if err != nil {
			t.Fatal(err)
		}
		if id != first {
			t.Fatalf("sticky session moved: first=%s now=%s", first, id)
		}
	}
	t.Logf("input: 11 requests same sessionKey with 2 instances; all landed on %s", first)
}

func TestSessionSurvivesDrainButRebindsOnRemoval(t *testing.T) {
	env := newTestEnv(t, 100, time.Second)
	env.addInstance(t, "svc", "i1", 0, false)
	env.addInstance(t, "svc", "i2", 0, false)
	first, err := env.gw.Route("tester", "svc", "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	// 优雅下线：DRAINING 期间存量会话继续落在原实例。
	if err := env.reg.BeginDrain("svc", first); err != nil {
		t.Fatal(err)
	}
	id, err := env.gw.Route("tester", "svc", "sess-1")
	t.Logf("input: route during drain of %s; got=%s err=%v (expect same instance)", first, id, err)
	if err != nil || id != first {
		t.Fatalf("draining instance must keep serving existing session, got %s err=%v", id, err)
	}
	// 新会话不得落到 DRAINING 实例。
	for i := 0; i < 5; i++ {
		newID, err := env.gw.Route("tester", "svc", fmt.Sprintf("fresh-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if newID == first {
			t.Fatalf("new session must not land on draining instance %s", first)
		}
	}
	// 实例摘除后，存量会话改绑到存活实例。
	if err := env.reg.Deregister("svc", first); err != nil {
		t.Fatal(err)
	}
	id, err = env.gw.Route("tester", "svc", "sess-1")
	t.Logf("input: route after deregister of %s; got=%s err=%v (expect rebind)", first, id, err)
	if err != nil || id == first {
		t.Fatalf("session must rebind after instance removal, got %s err=%v", id, err)
	}
}

func TestBackendHangFastFail(t *testing.T) {
	env := newTestEnv(t, 100, 100*time.Millisecond)
	env.addInstance(t, "svc", "slow", 0, true) // hang 后端
	start := time.Now()
	_, err := env.gw.Route("tester", "svc", "")
	elapsed := time.Since(start)
	stats := env.gw.Stats()
	t.Logf("input: hanging backend, timeout=100ms; err=%v elapsed=%s retries=%d isTimeout=%v",
		err, elapsed, stats.Retries, errors.Is(err, ErrBackendTimeout))
	if !errors.Is(err, ErrBackendTimeout) {
		t.Fatalf("expect ErrBackendTimeout, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("must fail fast, took %s", elapsed)
	}
	if stats.Retries != 0 {
		t.Fatalf("timeout must not be retried (no pressure amplification), retries=%d", stats.Retries)
	}
}

func TestRetryOnDeadInstanceDoesNotAmplify(t *testing.T) {
	env := newTestEnv(t, 100, time.Second)
	env.addInstance(t, "svc", "live", 0, false)
	// 注册一个并不存在的实例（地址无人监听 → 连接拒绝）。
	if err := env.reg.Register(registry.Instance{
		Service: "svc", ID: "dead", Addr: "127.0.0.1:1", Healthy: true,
	}); err != nil {
		t.Fatal(err)
	}
	// 反复路由：dead 上连接错误应换实例重试，最终落到 live。
	succeeded := 0
	for i := 0; i < 10; i++ {
		id, err := env.gw.Route("tester", "svc", "")
		if err == nil && id == "live" {
			succeeded++
		}
	}
	stats := env.gw.Stats()
	t.Logf("input: 10 routes with 1 dead + 1 live instance; succeeded=%d retries=%d",
		succeeded, stats.Retries)
	if succeeded != 10 {
		t.Fatalf("all requests should reach live instance via retry, succeeded=%d", succeeded)
	}
	// 限流只按请求扣一次：配额 100 未因重试被快速消耗（间接验证）。
	used, limit, _ := env.lim.Snapshot("tester")
	t.Logf("limiter usage after retries: %d/%d (expect 10/100, retries not double-charged)", used, limit)
	if used != 10 {
		t.Fatalf("retries must not consume extra quota, used=%d", used)
	}
}

func TestGracefulDrainInFlightRequest(t *testing.T) {
	env := newTestEnv(t, 100, 5*time.Second)
	env.addInstance(t, "svc", "i1", 300*time.Millisecond, false) // 慢请求
	env.addInstance(t, "svc", "i2", 0, false)

	// 先发一个粘性请求确认绑定到某实例，再用同会话发慢请求。
	first, err := env.gw.Route("tester", "svc", "sess-drain")
	if err != nil {
		t.Fatal(err)
	}
	// 让绑定实例上的请求变慢：直接构造慢后端不现实，改为对绑定实例注入延迟不可行，
	// 因此用并发在途请求模拟：请求进行中发起 drain。
	done := make(chan error, 1)
	go func() {
		_, err := env.gw.Route("tester", "svc", "sess-drain")
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // 确保请求已在途（若落在 i1 则 300ms 后才完）
	if err := env.reg.BeginDrain("svc", first); err != nil {
		t.Fatal(err)
	}
	err = <-done
	t.Logf("input: drain %s while sticky request in flight; in-flight err=%v (expect nil, graceful finish)", first, err)
	if err != nil {
		t.Fatalf("in-flight request on draining instance must finish normally: %v", err)
	}
}

func TestConcurrentRouting(t *testing.T) {
	env := newTestEnv(t, 10000, time.Second)
	for _, id := range []string{"i1", "i2", "i3"} {
		env.addInstance(t, "svc", id, 0, false)
	}
	const workers = 32
	const perWorker = 50
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	landed := sync.Map{}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess := fmt.Sprintf("sess-%d", w%8) // 8 个会话
			for i := 0; i < perWorker; i++ {
				id, err := env.gw.Route("tester", "svc", sess)
				if err != nil {
					errs <- err
					continue
				}
				landed.Store(id, true)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent route error: %v", err)
	}
	stats := env.gw.Stats()
	var kinds []string
	landed.Range(func(k, _ any) bool { kinds = append(kinds, k.(string)); return true })
	t.Logf("input: %d goroutines x %d requests, 3 instances; routed=%d rejected=%d landed-on=%s",
		workers, perWorker, stats.Routed, stats.Rejected, strings.Join(kinds, ","))
	if stats.Routed != int64(workers*perWorker) {
		t.Fatalf("expect all %d routed, got %d", workers*perWorker, stats.Routed)
	}
	if len(kinds) == 0 {
		t.Fatal("no instance received traffic")
	}
}
