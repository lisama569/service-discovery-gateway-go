// Package registry 提供进程内的服务注册中心：
// 实例注册、注销、心跳与过期剔除。
package registry

import (
	"errors"
	"time"
)

// 各类失败原因分别定义，调用方可用 errors.Is 区分。
var (
	// ErrInstanceNotFound 实例不存在（可能已注销或已过期被剔除）。
	ErrInstanceNotFound = errors.New("registry: instance not found")
	// ErrServiceNotFound 服务没有任何实例。
	ErrServiceNotFound = errors.New("registry: service not found")
	// ErrInstanceDraining 实例正在优雅下线，不再接受心跳续期以外的流量。
	ErrInstanceDraining = errors.New("registry: instance is draining")
	// ErrInvalidInstance 注册参数非法（ID/服务名为空）。
	ErrInvalidInstance = errors.New("registry: invalid instance (id or service empty)")
)

// Status 表示实例生命周期状态。
type Status string

const (
	StatusUp       Status = "up"       // 正常服务
	StatusDraining Status = "draining" // 优雅下线中：旧会话收尾，不接新流量
	StatusDown     Status = "down"     // 已下线
)

// Instance 是一个服务实例的注册信息。
type Instance struct {
	ID        string    // 实例唯一 ID（幂等键）
	Service   string    // 服务名
	Addr      string    // 回环地址，如 127.0.0.1:9001
	Weight    int       // 负载权重
	Status    Status    // 生命周期状态
	LastBeat  time.Time // 最近一次心跳时间
	UpdatedAt time.Time // 最近一次变更时间
}

// 空实现，后续回合填充。
