// Package registry 提供进程内的服务注册中心：
// 实例注册（幂等）、注销、心跳与过期剔除。
package registry

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// 失败原因按类型区分，调用方可用 errors.Is 判定。
var (
	ErrInstanceNotFound = errors.New("registry: instance not found")
	ErrServiceNotFound  = errors.New("registry: service not found")
	ErrInstanceDraining = errors.New("registry: instance is draining")
	ErrInvalidInstance  = errors.New("registry: invalid instance")
	ErrHeartbeatExpired = errors.New("registry: heartbeat expired")
)

// Status 表示实例生命周期状态。
type Status int

const (
	StatusUp       Status = iota
	StatusDraining        // 优雅下线中：不再接新流量，存量会话收尾
	StatusDown
)

func (s Status) String() string {
	switch s {
	case StatusUp:
		return "UP"
	case StatusDraining:
		return "DRAINING"
	case StatusDown:
		return "DOWN"
	default:
		return "UNKNOWN"
	}
}

// Instance 是一个服务实例的注册信息。
type Instance struct {
	Service  string
	ID       string
	Addr     string
	Healthy  bool
	Status   Status
	Metadata map[string]string
}

// Snapshot 是实例在某一时刻的只读视图。
type Snapshot struct {
	Instance
	LastHeartbeat time.Time
}

type record struct {
	inst          Instance
	lastHeartbeat time.Time
}

// Registry 是进程内注册中心，并发安全。
type Registry struct {
	mu  sync.RWMutex
	ttl time.Duration
	// services[service][id] = record
	services map[string]map[string]*record

	now        func() time.Time // 可注入时钟，便于测试
	sweepEvery time.Duration
	stopCh     chan struct{}
	stopped    chan struct{}
}

// New 创建注册中心。heartbeatTTL 为心跳超时时间，后台协程按 ttl/2 周期剔除过期实例。
func New(heartbeatTTL time.Duration) *Registry {
	return newRegistry(heartbeatTTL, heartbeatTTL/2)
}

// newRegistry 允许指定清理周期；sweepInterval<=0 时关闭后台清理（测试手动 Sweep）。
func newRegistry(heartbeatTTL, sweepInterval time.Duration) *Registry {
	r := &Registry{
		ttl:      heartbeatTTL,
		services: make(map[string]map[string]*record),
		now:      time.Now,
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	if sweepInterval > 0 {
		r.sweepEvery = sweepInterval
		go r.sweepLoop()
	} else {
		close(r.stopped)
	}
	return r
}

func validate(inst Instance) error {
	if inst.Service == "" || inst.ID == "" || inst.Addr == "" {
		return fmt.Errorf("%w: service/id/addr must not be empty (service=%q id=%q addr=%q)",
			ErrInvalidInstance, inst.Service, inst.ID, inst.Addr)
	}
	return nil
}

// Register 注册实例，重复注册同一 (Service, ID) 幂等：
// 已存在时刷新地址、健康与心跳，不重复计数、不报错。
func (r *Registry) Register(inst Instance) error {
	if err := validate(inst); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	svc, ok := r.services[inst.Service]
	if !ok {
		svc = make(map[string]*record)
		r.services[inst.Service] = svc
	}
	if inst.Metadata == nil {
		inst.Metadata = map[string]string{}
	}
	if rec, exists := svc[inst.ID]; exists {
		// 幂等重注册：刷新信息，状态恢复为 UP（除非正在下线）
		rec.inst.Addr = inst.Addr
		rec.inst.Healthy = inst.Healthy
		rec.inst.Metadata = inst.Metadata
		if rec.inst.Status != StatusDraining {
			rec.inst.Status = StatusUp
		}
		rec.lastHeartbeat = r.now()
		return nil
	}
	inst.Status = StatusUp
	svc[inst.ID] = &record{inst: inst, lastHeartbeat: r.now()}
	return nil
}

// Deregister 主动注销实例；服务或实例不存在时返回可区分的错误。
func (r *Registry) Deregister(service, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	svc, ok := r.services[service]
	if !ok {
		return fmt.Errorf("%w: %q", ErrServiceNotFound, service)
	}
	if _, ok := svc[id]; !ok {
		return fmt.Errorf("%w: %s/%s", ErrInstanceNotFound, service, id)
	}
	delete(svc, id)
	return nil
}

// Heartbeat 上报心跳；实例不存在或已过期返回对应错误。
func (r *Registry) Heartbeat(service, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getLocked(service, id)
	if err != nil {
		return err
	}
	if rec.inst.Status == StatusDown || r.expiredLocked(rec) {
		return fmt.Errorf("%w: %s/%s last=%s", ErrHeartbeatExpired, service, id,
			rec.lastHeartbeat.Format(time.RFC3339Nano))
	}
	rec.lastHeartbeat = r.now()
	return nil
}

// SetHealthy 更新实例健康状态。
func (r *Registry) SetHealthy(service, id string, healthy bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getLocked(service, id)
	if err != nil {
		return err
	}
	rec.inst.Healthy = healthy
	return nil
}

// BeginDrain 开始优雅下线：实例标记 DRAINING，不再接新流量，等待存量会话收尾后由 Deregister 摘除。
func (r *Registry) BeginDrain(service, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, err := r.getLocked(service, id)
	if err != nil {
		return err
	}
	if rec.inst.Status == StatusDraining {
		return fmt.Errorf("%w: %s/%s already draining", ErrInstanceDraining, service, id)
	}
	rec.inst.Status = StatusDraining
	return nil
}

// List 返回某服务的实例快照；服务不存在返回 ErrServiceNotFound。
func (r *Registry) List(service string) ([]Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, ok := r.services[service]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrServiceNotFound, service)
	}
	out := make([]Snapshot, 0, len(svc))
	for _, rec := range svc {
		out = append(out, Snapshot{Instance: rec.inst, LastHeartbeat: rec.lastHeartbeat})
	}
	return out, nil
}

// Sweep 剔除心跳超时的实例（标记 DOWN 并从列表摘除），返回被剔除的 "service/id"。
func (r *Registry) Sweep() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var removed []string
	for svcName, svc := range r.services {
		for id, rec := range svc {
			if r.expiredLocked(rec) {
				rec.inst.Status = StatusDown
				delete(svc, id)
				removed = append(removed, svcName+"/"+id)
			}
		}
	}
	return removed
}

func (r *Registry) getLocked(service, id string) (*record, error) {
	svc, ok := r.services[service]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrServiceNotFound, service)
	}
	rec, ok := svc[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrInstanceNotFound, service, id)
	}
	return rec, nil
}

func (r *Registry) expiredLocked(rec *record) bool {
	return r.now().Sub(rec.lastHeartbeat) > r.ttl
}

func (r *Registry) sweepLoop() {
	defer close(r.stopped)
	tick := time.NewTicker(r.sweepEvery)
	defer tick.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-tick.C:
			r.Sweep()
		}
	}
}

// Close 停止后台过期清理。
func (r *Registry) Close() {
	select {
	case <-r.stopped:
		return
	default:
	}
	close(r.stopCh)
	<-r.stopped
}
