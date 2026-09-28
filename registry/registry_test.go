package registry

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func logf(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("[input/decision] "+format, args...)
}

// newTestRegistry 返回一个时钟可控的注册中心（不启动后台扫描）。
func newTestRegistry(t *testing.T, ttl time.Duration) (*Registry, *fakeClock) {
	t.Helper()
	clk := newFakeClock(time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC))
	r := NewWithClock(ttl, 0, clk.get)
	return r, clk
}

func TestRegisterIdempotent(t *testing.T) {
	r, now := newTestRegistry(t, time.Second)
	inst := Instance{ID: "i1", Service: "svc", Addr: "127.0.0.1:1"}
	if err := r.Register(inst); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// 重复注册同一 ID：幂等，刷新心跳与地址
	now.advance(2 * time.Second) // 此时若靠旧心跳已过期
	inst.Addr = "127.0.0.1:2"
	if err := r.Register(inst); err != nil {
		t.Fatalf("duplicate register should be idempotent: %v", err)
	}
	list, err := r.List("svc")
	if err != nil || len(list) != 1 {
		t.Fatalf("expect 1 instance, got %d err=%v", len(list), err)
	}
	if list[0].Addr != "127.0.0.1:2" || list[0].Status != StatusUp {
		t.Fatalf("idempotent re-register must refresh metadata: %+v", list[0])
	}
	logf(t, "重复注册 i1 后 Addr=%s Status=%s LastBeat=%s", list[0].Addr, list[0].Status, list[0].LastBeat)
}

func TestRegisterInvalid(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	if err := r.Register(Instance{ID: "", Service: "svc"}); !errors.Is(err, ErrInvalidInstance) {
		t.Fatalf("empty id: want ErrInvalidInstance, got %v", err)
	}
	if err := r.Register(Instance{ID: "x", Service: ""}); !errors.Is(err, ErrInvalidInstance) {
		t.Fatalf("empty service: want ErrInvalidInstance, got %v", err)
	}
	logf(t, "非法注册参数被拒绝，错误原因 ErrInvalidInstance")
}

func TestHeartbeatExpiry(t *testing.T) {
	r, now := newTestRegistry(t, 500*time.Millisecond)
	mustReg(t, r, "i1", "svc")
	mustReg(t, r, "i2", "svc")

	// i1 持续心跳、i2 停心跳；推进时间超过 ttl
	now.advance(300 * time.Millisecond)
	if err := r.Heartbeat("i1"); err != nil {
		t.Fatal(err)
	}
	now.advance(300 * time.Millisecond)
	list, err := r.List("svc")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != "i1" {
		t.Fatalf("过期实例不得分到流量: got=%+v", list)
	}
	logf(t, "t=600ms ttl=500ms: i1 有心跳存活，i2 停心跳 -> List 仅返回 %v", list[0].ID)

	// evictExpired 后 i2 状态应为 down；对 down 实例心跳返回 ErrInstanceNotFound
	r.evictExpired()
	st, err := r.Get("i2")
	if err != nil || st.Status != StatusDown {
		t.Fatalf("expired instance should be marked down: %+v err=%v", st, err)
	}
	if err := r.Heartbeat("i2"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("heartbeat to unknown/down instance: want ErrInstanceNotFound, got %v", err)
	}
	logf(t, "扫描后 i2 Status=%s，对其心跳返回 ErrInstanceNotFound", st.Status)

	// 重新注册应恢复 up
	mustReg(t, r, "i2", "svc")
	if st, _ := r.Get("i2"); st.Status != StatusUp {
		t.Fatalf("re-register must bring instance up: %s", st.Status)
	}
}

func TestDeregisterErrors(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	if err := r.Deregister("ghost"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("deregister missing: want ErrInstanceNotFound, got %v", err)
	}
	mustReg(t, r, "i1", "svc")
	if err := r.Deregister("i1"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.List("svc"); !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("after all removed: want ErrServiceNotFound, got %v", err)
	}
	logf(t, "注销不存在 -> ErrInstanceNotFound；服务无实例 -> ErrServiceNotFound")
}

func TestDrainNotInHealthyList(t *testing.T) {
	r, _ := newTestRegistry(t, time.Second)
	mustReg(t, r, "i1", "svc")
	mustReg(t, r, "i2", "svc")
	if err := r.BeginDrain("i1"); err != nil {
		t.Fatal(err)
	}
	list, err := r.List("svc")
	if err != nil || len(list) != 1 || list[0].ID != "i2" {
		t.Fatalf("draining instance must not receive new traffic: %+v", list)
	}
	// draining 中仍允许心跳续期
	if err := r.Heartbeat("i1"); err != nil {
		t.Fatalf("heartbeat during drain should succeed: %v", err)
	}
	if err := r.BeginDrain("ghost"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("drain missing: want ErrInstanceNotFound, got %v", err)
	}
	logf(t, "i1 draining 后健康列表=[i2]，但 i1 心跳仍可续期")
}

func TestConcurrentRegisterDeregister(t *testing.T) {
	r, _ := newTestRegistry(t, 10*time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				id := fmt.Sprintf("i-%d-%d", g, k%20) // 同组内重复注册/注销
				err := r.Register(Instance{ID: id, Service: "svc", Addr: "127.0.0.1:0"})
				if err != nil {
					t.Errorf("register %s: %v", id, err)
					return
				}
				if k%3 == 0 {
					if err := r.Deregister(id); err != nil {
						t.Errorf("deregister %s: %v", id, err)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	list, err := r.List("svc")
	if err != nil {
		t.Fatalf("concurrent ops left registry unusable: %v", err)
	}
	// 并发跑完后所有返回实例必须是 up 状态（不能出现 map 脏读）
	for _, in := range list {
		if in.Status != StatusUp {
			t.Fatalf("concurrent corruption: %+v", in)
		}
	}
	logf(t, "16 goroutine x 200 轮并发注册/注销后，健康实例数=%d 且状态一致", len(list))
}

func TestSweepLoopMarksExpired(t *testing.T) {
	clk := newFakeClock(time.Now())
	r := NewWithClock(80*time.Millisecond, 20*time.Millisecond, clk.get)
	defer r.Close()
	mustReg(t, r, "i1", "svc")
	// 等真实扫描协程跑起来后，把逻辑时钟推过 ttl
	time.Sleep(40 * time.Millisecond)
	clk.advance(200 * time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	r.mu.RLock()
	st := r.instances["i1"].Status
	r.mu.RUnlock()
	if st != StatusDown {
		t.Fatalf("sweeper should mark expired instance down, got %s", st)
	}
	logf(t, "后台扫描 120ms 后 i1 Status=%s（ttl=80ms）", st)
}

func mustReg(t *testing.T, r *Registry, id, service string) {
	t.Helper()
	if err := r.Register(Instance{ID: id, Service: service, Addr: "127.0.0.1:0"}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}
