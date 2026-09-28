package limiter

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAllowWithinQuota(t *testing.T) {
	l := New(Config{Rules: map[string]Rule{"alice": {Permits: 3, Interval: time.Minute}}})
	for i := 0; i < 3; i++ {
		if err := l.Allow("alice"); err != nil {
			t.Fatalf("allow %d should pass: %v", i, err)
		}
	}
	err := l.Allow("alice")
	used, limit, _ := l.Snapshot("alice")
	t.Logf("input: 4th allow with quota 3; err=%v used=%d/%d isRateLimited=%v",
		err, used, limit, errors.Is(err, ErrRateLimited))
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expect ErrRateLimited, got %v", err)
	}
	if used != 3 {
		t.Fatalf("rejected request must not consume quota, used=%d", used)
	}
}

func TestUnknownCaller(t *testing.T) {
	l := New(Config{Rules: map[string]Rule{}, AllowUnknown: false})
	err := l.Allow("ghost")
	t.Logf("input: unknown caller with AllowUnknown=false; err=%v isUnknown=%v",
		err, errors.Is(err, ErrUnknownCaller))
	if !errors.Is(err, ErrUnknownCaller) {
		t.Fatalf("expect ErrUnknownCaller, got %v", err)
	}

	l2 := New(Config{AllowUnknown: true, DefaultRule: Rule{Permits: 1, Interval: time.Minute}})
	if err := l2.Allow("ghost"); err != nil {
		t.Fatalf("unknown caller with default rule should pass: %v", err)
	}
	if err := l2.Allow("ghost"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("default rule quota=1, second call should be limited, got %v", err)
	}
}

func TestConcurrentCountingExact(t *testing.T) {
	const quota = 500
	l := New(Config{Rules: map[string]Rule{"alice": {Permits: quota, Interval: time.Minute}}})
	const workers = 32
	const perWorker = 50 // 32*50 = 1600 次尝试，配额 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := l.Allow("alice"); err == nil {
					mu.Lock()
					allowed++
					mu.Unlock()
				} else if !errors.Is(err, ErrRateLimited) {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	used, limit, _ := l.Snapshot("alice")
	t.Logf("input: %d goroutines x %d calls, quota=%d; allowed=%d used=%d/%d",
		workers, perWorker, quota, allowed, used, limit)
	if allowed != quota || used != quota {
		t.Fatalf("concurrent counting must be exact: allowed=%d used=%d quota=%d", allowed, used, quota)
	}
}

func TestHotUpdateKeepsUnchangedRuleWindow(t *testing.T) {
	l := New(Config{Rules: map[string]Rule{
		"alice": {Permits: 2, Interval: time.Minute},
		"bob":   {Permits: 1, Interval: time.Minute},
	}})
	_ = l.Allow("alice") // alice 用掉 1/2
	_ = l.Allow("bob")   // bob 用掉 1/1

	// 热更新：alice 规则不变，bob 配额放宽到 5。
	l.Update(Config{Rules: map[string]Rule{
		"alice": {Permits: 2, Interval: time.Minute},
		"bob":   {Permits: 5, Interval: time.Minute},
	}})

	usedA, limitA, _ := l.Snapshot("alice")
	usedB, limitB, _ := l.Snapshot("bob")
	t.Logf("after update: alice used=%d/%d (window kept), bob used=%d/%d (window reset)",
		usedA, limitA, usedB, limitB)
	if usedA != 1 {
		t.Fatalf("unchanged rule must keep window count, alice used=%d", usedA)
	}
	// alice 只剩 1 次配额。
	if err := l.Allow("alice"); err != nil {
		t.Fatalf("alice should have 1 permit left: %v", err)
	}
	if err := l.Allow("alice"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("alice should be limited after exhausting kept window, got %v", err)
	}
	// bob 规则变了，窗口重置，按新配额 5 判定。
	for i := 0; i < 5; i++ {
		if err := l.Allow("bob"); err != nil {
			t.Fatalf("bob new quota 5, call %d should pass: %v", i, err)
		}
	}
	if err := l.Allow("bob"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("bob should be limited at new quota 5, got %v", err)
	}
}

func TestHotUpdateTightenAppliesToNewRequests(t *testing.T) {
	l := New(Config{Rules: map[string]Rule{"alice": {Permits: 100, Interval: time.Minute}}})
	for i := 0; i < 10; i++ {
		_ = l.Allow("alice")
	}
	// 收紧到 3：规则变化，窗口重置，新请求按新规则。
	l.Update(Config{Rules: map[string]Rule{"alice": {Permits: 3, Interval: time.Minute}}})
	allowed := 0
	for i := 0; i < 10; i++ {
		if l.Allow("alice") == nil {
			allowed++
		}
	}
	t.Logf("input: tighten quota 100->3 after 10 used; allowed after update=%d (expect 3)", allowed)
	if allowed != 3 {
		t.Fatalf("new requests must follow new rule, allowed=%d", allowed)
	}
}

func TestWindowResetAfterInterval(t *testing.T) {
	l := New(Config{Rules: map[string]Rule{"alice": {Permits: 1, Interval: 40 * time.Millisecond}}})
	if err := l.Allow("alice"); err != nil {
		t.Fatal(err)
	}
	if err := l.Allow("alice"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second call in same window should be limited")
	}
	time.Sleep(60 * time.Millisecond)
	err := l.Allow("alice")
	t.Logf("input: call after window interval elapsed; err=%v (expect nil)", err)
	if err != nil {
		t.Fatalf("new window should reset quota: %v", err)
	}
}
