package ratelimit

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/config"
)

func testCfg(timeout time.Duration, retries int, def config.CallerQuota, quotas ...[2]config.CallerQuota) *config.Config {
	// 简化构造：quotas 用 map 形式不方便，直接在各测试里组配置
	c := &config.Config{
		BackendTimeout: timeout,
		MaxRetries:     retries,
		DefaultQuota:   def,
		Quotas:         map[string]config.CallerQuota{},
	}
	return c
}

func TestBurstThenRefill(t *testing.T) {
	now := time.Now()
	l := New()
	l.now = func() time.Time { return now }
	cfg := testCfg(time.Second, 0, config.CallerQuota{Rate: 10, Burst: 3})

	for i := 0; i < 3; i++ {
		if err := l.Allow("alice", cfg); err != nil {
			t.Fatalf("burst token %d should be allowed: %v", i, err)
		}
	}
	if err := l.Allow("alice", cfg); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("4th request beyond burst must be rejected, got %v", err)
	}
	t.Logf("[input/decision] burst=3: 前3放行, 第4拒绝=%v", errText(l, cfg))

	// 推进 100ms，rate=10/s 恰好补 1 个令牌
	now = now.Add(100 * time.Millisecond)
	if err := l.Allow("alice", cfg); err != nil {
		t.Fatalf("after 100ms one token must refill: %v", err)
	}
	if err := l.Allow("alice", cfg); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("only one token refilled, second must reject")
	}
	t.Log("[input/decision] 推进100ms后补充1令牌: 1放行1拒绝")
}

func errText(l *Limiter, cfg *config.Config) string {
	if err := l.Allow("probe", cfg); err != nil {
		return err.Error()
	}
	return ""
}

func TestRateZeroDeniesAll(t *testing.T) {
	now := time.Now()
	l := New()
	l.now = func() time.Time { return now }
	cfg := testCfg(time.Second, 0, config.CallerQuota{Rate: 100, Burst: 100})
	cfg.Quotas["blocked"] = config.CallerQuota{Rate: 0, Burst: 0}
	for i := 0; i < 5; i++ {
		if err := l.Allow("blocked", cfg); !errors.Is(err, ErrRateLimited) {
			t.Fatalf("Rate=0 must deny every request, iter=%d got %v", i, err)
		}
	}
	if err := l.Allow("normal", cfg); err != nil {
		t.Fatalf("other callers unaffected: %v", err)
	}
	t.Log("[input/decision] Rate=0 调用方5次全拒绝，其他调用方正常")
}

func TestHotReloadAppliesToNewRequests(t *testing.T) {
	now := time.Now()
	l := New()
	l.now = func() time.Time { return now }
	old := testCfg(time.Second, 0, config.CallerQuota{Rate: 100, Burst: 100})
	for i := 0; i < 10; i++ {
		if err := l.Allow("bob", old); err != nil {
			t.Fatalf("under old config all allowed: %v", err)
		}
	}
	// 热更新：bob 降为 burst=2
	updated := testCfg(time.Second, 0, config.CallerQuota{Rate: 100, Burst: 100})
	updated.Quotas["bob"] = config.CallerQuota{Rate: 1, Burst: 2}
	allowed, rejected := 0, 0
	for i := 0; i < 10; i++ {
		if l.Allow("bob", updated) == nil {
			allowed++
		} else {
			rejected++
		}
	}
	// 旧桶余量被 cap 到新 burst=2，同一时刻最多再放 2 个
	if allowed > 2 {
		t.Fatalf("hot reload must clamp old bucket to new burst: allowed=%d", allowed)
	}
	t.Logf("[input/decision] 热更新 bob burst 100->2 后: 放行=%d 拒绝=%d", allowed, rejected)
}

func TestConcurrentCountNoLossNoDouble(t *testing.T) {
	now := time.Now()
	l := New()
	l.now = func() time.Time { return now } // 时钟冻结，令牌数恒定
	const burst = 50
	cfg := testCfg(time.Second, 0, config.CallerQuota{Rate: 1, Burst: burst})
	cfg.Quotas["heavy"] = config.CallerQuota{Rate: 1, Burst: burst} // 冻结时钟：不补充令牌

	var wg sync.WaitGroup
	var allowed atomicI64
	var rejected atomicI64
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 100; k++ {
				if l.Allow("heavy", cfg) == nil {
					allowed.add(1)
				} else {
					rejected.add(1)
				}
			}
		}()
	}
	wg.Wait()
	total := allowed.get() + rejected.get()
	if total != 2000 {
		t.Fatalf("every request must be counted exactly once: total=%d", total)
	}
	if allowed.get() != burst {
		t.Fatalf("frozen clock: exactly burst tokens may be consumed, allowed=%d want=%d",
			allowed.get(), burst)
	}
	t.Logf("[input/decision] 20x100 并发: 总判定=%d (放行=%d 拒绝=%d)，无丢失无重复",
		total, allowed.get(), rejected.get())
}

// 极简原子计数器，避免在测试里再引 sync/atomic 的别名问题
type atomicI64 struct{ v int64 }

func (a *atomicI64) add(n int64) int64 { return addI64(&a.v, n) }
func (a *atomicI64) get() int64        { return loadI64(&a.v) }
