package registry

import (
	"sync"
	"time"
)

// Registry 是进程内注册中心，所有方法并发安全。
type Registry struct {
	mu        sync.RWMutex
	instances map[string]*Instance // key: instance ID
	ttl       time.Duration        // 心跳超时时间
	now       func() time.Time     // 可注入时钟，便于测试
	stopCh    chan struct{}
	wg        sync.WaitGroup
	stopped   sync.Once
}

// New 创建注册中心；ttl 为心跳超时时间，sweep 为过期扫描间隔。
func New(ttl, sweep time.Duration) *Registry {
	return NewWithClock(ttl, sweep, time.Now)
}

// NewWithClock 创建注册中心并注入时钟函数（测试用），必须在扫描协程启动前注入。
func NewWithClock(ttl, sweep time.Duration, now func() time.Time) *Registry {
	r := &Registry{
		instances: make(map[string]*Instance),
		ttl:       ttl,
		now:       now,
		stopCh:    make(chan struct{}),
	}
	if sweep > 0 {
		r.wg.Add(1)
		go r.sweepLoop(sweep)
	}
	return r
}

// Register 注册实例，重复注册同一 ID 幂等：
// 已存在则刷新地址、权重与心跳，无论之前是 up 还是 down 都恢复为 up。
func (r *Registry) Register(inst Instance) error {
	if inst.ID == "" || inst.Service == "" {
		return ErrInvalidInstance
	}
	if inst.Weight <= 0 {
		inst.Weight = 1
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.instances[inst.ID]; ok {
		// 幂等刷新：同一实例重新注册视为重新上线
		old.Service = inst.Service
		old.Addr = inst.Addr
		old.Weight = inst.Weight
		old.Status = StatusUp
		old.LastBeat = now
		old.UpdatedAt = now
		return nil
	}
	inst.Status = StatusUp
	inst.LastBeat = now
	inst.UpdatedAt = now
	r.instances[inst.ID] = &inst
	return nil
}

// Deregister 主动注销实例；不存在返回 ErrInstanceNotFound。
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.instances[id]; !ok {
		return ErrInstanceNotFound
	}
	delete(r.instances, id)
	return nil
}

// Heartbeat 续期心跳；实例不存在或已 down 返回 ErrInstanceNotFound，
// draining 中的实例仍可续期（优雅下线期间保持存活）。
func (r *Registry) Heartbeat(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst, ok := r.instances[id]
	if !ok || inst.Status == StatusDown {
		return ErrInstanceNotFound
	}
	inst.LastBeat = r.now()
	inst.UpdatedAt = inst.LastBeat
	return nil
}

// BeginDrain 让实例进入优雅下线状态：不再接新流量，旧会话仍可路由到它，
// 等在途请求收尾后调用 Deregister 彻底摘除。
func (r *Registry) BeginDrain(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst, ok := r.instances[id]
	if !ok || inst.Status == StatusDown {
		return ErrInstanceNotFound
	}
	inst.Status = StatusDraining
	inst.UpdatedAt = r.now()
	return nil
}

// Get 返回单个实例的快照，不存在返回 ErrInstanceNotFound。
func (r *Registry) Get(id string) (Instance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	inst, ok := r.instances[id]
	if !ok {
		return Instance{}, ErrInstanceNotFound
	}
	return *inst, nil
}

// healthy 判断实例此刻是否健康：状态 up 且心跳未超时。
func (r *Registry) healthy(inst *Instance, now time.Time) bool {
	if inst.Status != StatusUp {
		return false
	}
	return now.Sub(inst.LastBeat) <= r.ttl
}

// List 返回某服务当前健康（up 且心跳未过期）的实例快照。
// draining/down/过期实例不会出现在结果中。
// 服务名从未注册过时返回 ErrServiceNotFound；
// 有实例但全部不可用时返回空切片，由调用方决定拒绝策略。
func (r *Registry) List(service string) ([]Instance, error) {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := false
	out := make([]Instance, 0)
	for _, inst := range r.instances {
		if inst.Service != service {
			continue
		}
		seen = true
		if r.healthy(inst, now) {
			out = append(out, *inst)
		}
	}
	if !seen {
		return nil, ErrServiceNotFound
	}
	return out, nil
}

// ListForSticky 返回某服务的 up 实例 + 指定实例（若它属于该服务且只是 draining
// 且心跳未过期），用于让优雅下线中的旧会话继续收尾。
func (r *Registry) ListForSticky(service, stickyID string) (healthy []Instance, sticky *Instance) {
	now := r.now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, inst := range r.instances {
		if inst.Service != service {
			continue
		}
		if r.healthy(inst, now) {
			healthy = append(healthy, *inst)
		}
		if inst.ID == stickyID && inst.Status == StatusDraining &&
			now.Sub(inst.LastBeat) <= r.ttl {
			cp := *inst
			sticky = &cp
		}
	}
	return healthy, sticky
}

// sweepLoop 周期性把心跳超时的实例标记为 down。
func (r *Registry) sweepLoop(interval time.Duration) {
	defer r.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			r.evictExpired()
		}
	}
}

// evictExpired 把心跳超过 ttl 的实例标记为 down。
func (r *Registry) evictExpired() {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, inst := range r.instances {
		if inst.Status == StatusDown {
			continue
		}
		if now.Sub(inst.LastBeat) > r.ttl {
			inst.Status = StatusDown
			inst.UpdatedAt = now
		}
	}
}

// Close 停止后台扫描协程。
func (r *Registry) Close() {
	r.stopped.Do(func() { close(r.stopCh) })
	r.wg.Wait()
}
