package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/config"
	"github.com/lisama569/service-discovery-gateway-go/ratelimit"
	"github.com/lisama569/service-discovery-gateway-go/registry"
)

// fakeBackend 可控的转发后端：可正常返回、可延迟、可挂起直到 release()。
type fakeBackend struct {
	mu            sync.Mutex
	hang          map[string]bool          // 挂住直到信号
	block         map[string]chan struct{} // 每实例挂起控制
	attempts      map[string]int
	totalAttempts int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		hang:     map[string]bool{},
		block:    map[string]chan struct{}{},
		attempts: map[string]int{},
	}
}

func (f *fakeBackend) forward(ctx context.Context, inst registry.Instance) error {
	f.mu.Lock()
	f.attempts[inst.ID]++
	f.totalAttempts++
	hang := f.hang[inst.ID]
	ch := f.block[inst.ID]
	f.mu.Unlock()
	if hang {
		select {
		case <-ch: // 测试主动释放：模拟后端终于响应/关停
			return nil
		case <-ctx.Done(): // 网关超时取消：模拟后端长时间无响应
			return ErrBackendTimeout
		}
	}
	return nil
}

func (f *fakeBackend) setHang(id string, hang bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hang[id] = hang
	if hang && f.block[id] == nil {
		f.block[id] = make(chan struct{})
	}
}

func (f *fakeBackend) release(id string) {
	f.mu.Lock()
	ch := f.block[id]
	f.mu.Unlock()
	close(ch)
}

func (f *fakeBackend) count(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[id]
}

func newTestRouter(t *testing.T, cfg *config.Config, fb *fakeBackend) (*Router, *registry.Registry) {
	t.Helper()
	reg := registry.NewWithClock(10*time.Second, 0, time.Now)
	h := config.NewHolder(cfg)
	l := ratelimit.New()
	r := New(reg, h, l)
	r.setForward(fb.forward)
	r.SetLogger(func(format string, args ...any) { t.Logf("[route] "+format, args...) })
	return r, reg
}

func baseCfg() *config.Config {
	return &config.Config{
		BackendTimeout: 100 * time.Millisecond,
		MaxRetries:     1,
		Quotas:         map[string]config.CallerQuota{},
		DefaultQuota:   config.CallerQuota{Rate: 100000, Burst: 100000},
	}
}

func regN(t *testing.T, reg *registry.Registry, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("inst-%d", i)
		if err := reg.Register(registry.Instance{ID: id, Service: "svc", Addr: "127.0.0.1:0", Weight: 1}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStickySessionPinned(t *testing.T) {
	fb := newFakeBackend()
	r, reg := newTestRouter(t, baseCfg(), fb)
	regN(t, reg, 3)

	var first string
	for i := 0; i < 5; i++ {
		res, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a", SessionKey: "sess-1"})
		if err != nil {
			t.Fatal(err)
		}
		if first == "" {
			first = res.InstanceID
		} else if res.InstanceID != first {
			t.Fatalf("sticky session moved: %s -> %s", first, res.InstanceID)
		}
	}
	t.Logf("[input/decision] 同一会话 sess-1 5次请求全部固定在 %s", first)
}

func TestLeastLoadedSpreads(t *testing.T) {
	fb := newFakeBackend()
	cfg := baseCfg()
	cfg.BackendTimeout = 5 * time.Second // 请求靠 release 成功，避免超时重试干扰计数
	r, reg := newTestRouter(t, cfg, fb)
	regN(t, reg, 3)

	// 串行请求无法体现“在途负载”，这里让所有后端先挂住，
	// 9 个并发请求必须按当前在途数摊到 3 个实例上（每个 3 个）。
	for i := 1; i <= 3; i++ {
		fb.setHang(fmt.Sprintf("inst-%d", i), true)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 9)
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := r.Do(context.Background(), Request{
				Service: "svc", Caller: "a", SessionKey: fmt.Sprintf("s%d", i),
			})
			errCh <- err
		}(i)
	}
	// 等负载铺开：每实例恰好 3 个在途
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fb.count("inst-1") == 3 && fb.count("inst-2") == 3 && fb.count("inst-3") == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 1; i <= 3; i++ {
		fb.release(fmt.Sprintf("inst-%d", i))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent request failed: %v", err)
		}
	}
	dist := map[string]int{
		"inst-1": fb.count("inst-1"),
		"inst-2": fb.count("inst-2"),
		"inst-3": fb.count("inst-3"),
	}
	for id, n := range dist {
		if n != 3 {
			t.Fatalf("least-loaded must balance 9 concurrent requests 3/3/3, got %s=%d", id, n)
		}
	}
	t.Logf("[input/decision] 9个并发请求按在途负载均衡: %v", dist)
}

func TestNoHealthyInstanceClassified(t *testing.T) {
	fb := newFakeBackend()
	r, reg := newTestRouter(t, baseCfg(), fb)

	// 服务从未注册
	_, err := r.Do(context.Background(), Request{Service: "ghost", Caller: "a"})
	if !errors.Is(err, ErrNoHealthyInstance) {
		t.Fatalf("unknown service: want ErrNoHealthyInstance, got %v", err)
	}
	t.Log("[input/decision] 服务 ghost 从未注册 -> ErrNoHealthyInstance")

	// 注册后全部下线
	regN(t, reg, 2)
	if err := reg.BeginDrain("inst-1"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Deregister("inst-2"); err != nil {
		t.Fatal(err)
	}
	_, err = r.Do(context.Background(), Request{Service: "svc", Caller: "a"})
	if !errors.Is(err, ErrNoHealthyInstance) {
		t.Fatalf("all down: want ErrNoHealthyInstance, got %v", err)
	}
	t.Log("[input/decision] 实例1 draining + 实例2注销 -> ErrNoHealthyInstance")
}

func TestTimeoutFailFastAndRetry(t *testing.T) {
	fb := newFakeBackend()
	cfg := baseCfg() // MaxRetries=1，超时100ms
	r, reg := newTestRouter(t, cfg, fb)
	regN(t, reg, 2)

	start := time.Now()
	fb.setHang("inst-1", true)
	res, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("retry to healthy instance should succeed: %v", err)
	}
	if res.Retried != 1 || len(res.Attempts) != 2 || res.Attempts[0] == res.InstanceID {
		t.Fatalf("expect one retry to a different instance: %+v", res)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("timeout must fail fast, took %s", elapsed)
	}
	t.Logf("[input/decision] inst-1 挂起，%s后超时并在 %s 重试成功，判定依据: attempts=%v",
		elapsed.Round(time.Millisecond), res.InstanceID, res.Attempts)
}

func TestRetryExhaustedNoAmplification(t *testing.T) {
	fb := newFakeBackend()
	cfg := baseCfg() // 最多尝试 2 次
	r, reg := newTestRouter(t, cfg, fb)
	regN(t, reg, 2)
	fb.setHang("inst-1", true)
	fb.setHang("inst-2", true)

	start := time.Now()
	_, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a"})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrRetryExhausted) || !errors.Is(err, ErrBackendTimeout) {
		t.Fatalf("want ErrRetryExhausted wrapping ErrBackendTimeout, got %v", err)
	}
	if fb.totalAttempts != 2 {
		t.Fatalf("MaxRetries=1 caps total attempts at 2, got %d (pressure amplified!)", fb.totalAttempts)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("two bounded timeouts must still fail fast, took %s", elapsed)
	}
	t.Logf("[input/decision] 两实例均挂起: 总尝试=%d（上限2，未放大），耗时=%s，错误=ErrRetryExhausted(ErrBackendTimeout)",
		fb.totalAttempts, elapsed.Round(time.Millisecond))
}

func TestDrainingStickyStillServed(t *testing.T) {
	fb := newFakeBackend()
	r, reg := newTestRouter(t, baseCfg(), fb)
	regN(t, reg, 2)

	// 先让 sess 固定到某实例
	res, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a", SessionKey: "sess"})
	if err != nil {
		t.Fatal(err)
	}
	pinned := res.InstanceID

	// 该实例进入 draining：旧会话仍能路由过去
	if err := reg.BeginDrain(pinned); err != nil {
		t.Fatal(err)
	}
	res, err = r.Do(context.Background(), Request{Service: "svc", Caller: "a", SessionKey: "sess"})
	if err != nil || res.InstanceID != pinned {
		t.Fatalf("old session must still finish on draining instance: res=%+v err=%v", res, err)
	}
	// 全新会话不会选中 draining 实例
	for i := 0; i < 5; i++ {
		res, err = r.Do(context.Background(), Request{Service: "svc", Caller: "b", SessionKey: fmt.Sprintf("new-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if res.InstanceID == pinned {
			t.Fatalf("new session must not land on draining instance")
		}
	}
	// 彻底摘除后，旧会话漂移并重新固定
	if err := reg.Deregister(pinned); err != nil {
		t.Fatal(err)
	}
	res, _ = r.Do(context.Background(), Request{Service: "svc", Caller: "a", SessionKey: "sess"})
	res2, _ := r.Do(context.Background(), Request{Service: "svc", Caller: "a", SessionKey: "sess"})
	if res.InstanceID == pinned || res.InstanceID != res2.InstanceID {
		t.Fatalf("session should migrate and re-pin: %s %s", res.InstanceID, res2.InstanceID)
	}
	t.Logf("[input/decision] %s draining: 旧会话继续走它, 新会话避开; 摘除后旧会话漂移到 %s 并重新固定",
		pinned, res.InstanceID)
}

func TestGracefulShutdownInflightFinishes(t *testing.T) {
	fb := newFakeBackend()
	cfg := baseCfg()
	cfg.BackendTimeout = 5 * time.Second // 不依赖超时，靠 drain 完成
	r, reg := newTestRouter(t, cfg, fb)
	regN(t, reg, 1)
	fb.setHang("inst-1", true)

	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a", SessionKey: "s"})
		done <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // 确保请求已挂在后端上

	r.BeginShutdown()
	// 下线后新请求立即被拒绝（明确错误，不排队）
	_, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a"})
	if !errors.Is(err, ErrGatewayShutting) {
		t.Fatalf("new request during shutdown must be rejected with ErrGatewayShutting, got %v", err)
	}
	// 在途请求尚未结束
	if r.Inflight() != 1 {
		t.Fatalf("inflight request must still be counted, got %d", r.Inflight())
	}
	// 释放挂起的请求，WaitInflight 应观察到收尾完成
	waitDone := make(chan bool, 1)
	go func() { waitDone <- r.WaitInflight(2 * time.Second) }()
	fb.release("inst-1")
	if !<-waitDone {
		t.Fatal("WaitInflight should return true after in-flight finished")
	}
	if err := <-done; err != nil {
		t.Fatalf("in-flight request should finish without gateway error, got %v", err)
	}
	t.Log("[input/decision] BeginShutdown 后新请求 ErrGatewayShutting；挂起的在途请求释放后正常收尾")
}

func TestHotReloadInFlightUsesOldNewRequestsUseNew(t *testing.T) {
	fb := newFakeBackend()
	cfg := baseCfg()
	cfg.BackendTimeout = 5 * time.Second
	r, reg := newTestRouter(t, cfg, fb)
	regN(t, reg, 1)
	fb.setHang("inst-1", true)

	// 旧配置下发一个在途请求
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a"})
		done <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond)

	// 热更新：调用方 a 直接封禁。在途请求不受影响（持旧快照），新请求按新规则拒绝
	newCfg := *cfg
	newCfg.Quotas = map[string]config.CallerQuota{"a": {Rate: 0, Burst: 0}}
	if err := r.cfg.Update(&newCfg); err != nil {
		t.Fatal(err)
	}
	_, err := r.Do(context.Background(), Request{Service: "svc", Caller: "a"})
	if !errors.Is(err, ratelimit.ErrRateLimited) {
		t.Fatalf("new request after hot reload must use new quota, got %v", err)
	}
	t.Log("[input/decision] 热更新封禁调用方a: 新请求 ErrRateLimited")

	fb.release("inst-1")
	if err := <-done; err != nil {
		t.Fatalf("in-flight request that started before reload must finish: %v", err)
	}
	t.Log("[input/decision] 热更新前已在途的请求按旧配置正常收尾")
}
