package registry

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestRegistry 关闭后台清理协程，测试里手动 Sweep，避免时序干扰。
func newTestRegistry(t *testing.T, ttl time.Duration) *Registry {
	t.Helper()
	r := newRegistry(ttl, 0)
	t.Cleanup(r.Close)
	return r
}

func TestRegisterIdempotent(t *testing.T) {
	r := newTestRegistry(t, time.Minute)
	inst := Instance{Service: "svc", ID: "i1", Addr: "127.0.0.1:9001", Healthy: true}
	for i := 0; i < 3; i++ {
		if err := r.Register(inst); err != nil {
			t.Fatalf("register attempt %d: input=%+v err=%v", i, inst, err)
		}
	}
	snaps, err := r.List("svc")
	t.Logf("input: register same instance 3 times; list=%d snapshots, err=%v", len(snaps), err)
	if err != nil || len(snaps) != 1 {
		t.Fatalf("expect exactly 1 instance after idempotent re-register, got %d err=%v", len(snaps), err)
	}
	if snaps[0].Status != StatusUp {
		t.Fatalf("expect status UP, got %s", snaps[0].Status)
	}
}

func TestRegisterInvalid(t *testing.T) {
	r := newTestRegistry(t, time.Minute)
	bad := Instance{Service: "", ID: "i1", Addr: "127.0.0.1:1"}
	err := r.Register(bad)
	t.Logf("input: %+v; err=%v; errors.Is(ErrInvalidInstance)=%v", bad, err, errors.Is(err, ErrInvalidInstance))
	if !errors.Is(err, ErrInvalidInstance) {
		t.Fatalf("expect ErrInvalidInstance, got %v", err)
	}
}

func TestDeregisterErrorDistinction(t *testing.T) {
	r := newTestRegistry(t, time.Minute)
	err := r.Deregister("nosvc", "i1")
	t.Logf("deregister missing service: err=%v isServiceNotFound=%v", err, errors.Is(err, ErrServiceNotFound))
	if !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("expect ErrServiceNotFound, got %v", err)
	}
	_ = r.Register(Instance{Service: "svc", ID: "i1", Addr: "a", Healthy: true})
	err = r.Deregister("svc", "ghost")
	t.Logf("deregister missing instance: err=%v isInstanceNotFound=%v", err, errors.Is(err, ErrInstanceNotFound))
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("expect ErrInstanceNotFound, got %v", err)
	}
	if err := r.Deregister("svc", "i1"); err != nil {
		t.Fatalf("deregister existing: %v", err)
	}
	if _, err := r.List("svc"); err != nil {
		t.Fatalf("service should still exist with 0 instances, got %v", err)
	}
}

func TestHeartbeatExpiry(t *testing.T) {
	r := newTestRegistry(t, 50*time.Millisecond)
	inst := Instance{Service: "svc", ID: "i1", Addr: "a", Healthy: true}
	if err := r.Register(inst); err != nil {
		t.Fatal(err)
	}
	if err := r.Heartbeat("svc", "i1"); err != nil {
		t.Fatalf("heartbeat right after register should succeed: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // 超过 ttl
	err := r.Heartbeat("svc", "i1")
	t.Logf("input: heartbeat after ttl; err=%v isHeartbeatExpired=%v", err, errors.Is(err, ErrHeartbeatExpired))
	if !errors.Is(err, ErrHeartbeatExpired) {
		t.Fatalf("expect ErrHeartbeatExpired, got %v", err)
	}
	removed := r.Sweep()
	t.Logf("sweep removed=%v", removed)
	if len(removed) != 1 || removed[0] != "svc/i1" {
		t.Fatalf("expect svc/i1 swept, got %v", removed)
	}
	// 被剔除后心跳应报实例不存在，而不是别的错误。
	err = r.Heartbeat("svc", "i1")
	t.Logf("heartbeat after sweep: err=%v isInstanceNotFound=%v", err, errors.Is(err, ErrInstanceNotFound))
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("expect ErrInstanceNotFound after sweep, got %v", err)
	}
}

func TestHeartbeatKeepsAlive(t *testing.T) {
	r := newTestRegistry(t, 120*time.Millisecond)
	_ = r.Register(Instance{Service: "svc", ID: "i1", Addr: "a", Healthy: true})
	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond) // 每次间隔 < ttl
		if err := r.Heartbeat("svc", "i1"); err != nil {
			t.Fatalf("heartbeat round %d failed: %v", i, err)
		}
	}
	removed := r.Sweep()
	t.Logf("input: 5 heartbeats spaced 50ms with ttl 120ms; swept=%v (expect empty)", removed)
	if len(removed) != 0 {
		t.Fatalf("instance with fresh heartbeats must not be swept: %v", removed)
	}
}

func TestConcurrentRegisterDeregister(t *testing.T) {
	r := newTestRegistry(t, time.Minute)
	const workers = 16
	const perWorker = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			id := fmt.Sprintf("i%d", w)
			inst := Instance{Service: "svc", ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 9000+w), Healthy: true}
			for i := 0; i < perWorker; i++ {
				if err := r.Register(inst); err != nil { // 幂等重注册
					t.Errorf("register %s: %v", id, err)
					return
				}
				if err := r.Heartbeat("svc", id); err != nil {
					t.Errorf("heartbeat %s: %v", id, err)
					return
				}
				if i%2 == 0 {
					_ = r.Deregister("svc", id)
				}
			}
		}(w)
	}
	wg.Wait()
	snaps, err := r.List("svc")
	t.Logf("input: %d workers x %d register/heartbeat/deregister rounds; final instances=%d err=%v",
		workers, perWorker, len(snaps), err)
	if err != nil {
		t.Fatalf("list after concurrent churn: %v", err)
	}
	// 每个 worker 最后一轮：i=199 为奇数，不注销 → 每个 worker 应剩 1 个实例。
	if len(snaps) != workers {
		t.Fatalf("expect %d surviving instances, got %d", workers, len(snaps))
	}
}

func TestDrainLifecycle(t *testing.T) {
	r := newTestRegistry(t, time.Minute)
	_ = r.Register(Instance{Service: "svc", ID: "i1", Addr: "a", Healthy: true})
	if err := r.BeginDrain("svc", "i1"); err != nil {
		t.Fatalf("begin drain: %v", err)
	}
	err := r.BeginDrain("svc", "i1")
	t.Logf("double drain: err=%v isDraining=%v", err, errors.Is(err, ErrInstanceDraining))
	if !errors.Is(err, ErrInstanceDraining) {
		t.Fatalf("expect ErrInstanceDraining on double drain, got %v", err)
	}
	snaps, _ := r.List("svc")
	t.Logf("input: drain then list; status=%s (expect DRAINING)", snaps[0].Status)
	if snaps[0].Status != StatusDraining {
		t.Fatalf("expect DRAINING, got %s", snaps[0].Status)
	}
	// DRAINING 中重注册不得把状态拉回 UP。
	_ = r.Register(Instance{Service: "svc", ID: "i1", Addr: "a", Healthy: true})
	snaps, _ = r.List("svc")
	if snaps[0].Status != StatusDraining {
		t.Fatalf("re-register during drain must keep DRAINING, got %s", snaps[0].Status)
	}
}
