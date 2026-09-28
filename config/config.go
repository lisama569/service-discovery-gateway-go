// Package config 提供可热更新的网关配置。
package config

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrInvalidConfig 新配置非法，热更新被拒绝，继续沿用旧配置。
var ErrInvalidConfig = errors.New("config: invalid config")

// CallerQuota 单个调用方的限流配额。
type CallerQuota struct {
	Rate  int // 每秒允许的请求数
	Burst int // 突发桶容量
}

// Config 网关运行配置，整体不可变，热更新时整体替换。
// 正在处理的请求在开始时取一次快照，按旧规则收尾；新请求按新规则判定。
type Config struct {
	BackendTimeout time.Duration          // 后端响应超时，超时快速失败
	MaxRetries     int                    // 最大重试次数（重试不会超过这个数，避免放大压力）
	Quotas         map[string]CallerQuota // 按调用方的配额
	DefaultQuota   CallerQuota            // 未配置调用方的默认配额
}

// Holder 保存当前配置，支持并发读与原子热更新。
type Holder struct {
	cur atomic.Value // 存 *Config
}

// Default 提供一份可用的默认配置。
func Default() *Config {
	return &Config{
		BackendTimeout: 500 * time.Millisecond,
		MaxRetries:     1,
		Quotas:         map[string]CallerQuota{},
		DefaultQuota:   CallerQuota{Rate: 100, Burst: 200},
	}
}

// validate 校验并规整配置，返回一份深拷贝（避免外部 map 被并发修改）。
func validate(c *Config) (*Config, error) {
	if c == nil {
		return nil, ErrInvalidConfig
	}
	if c.BackendTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	if c.MaxRetries < 0 {
		return nil, ErrInvalidConfig
	}
	if c.DefaultQuota.Rate < 0 || c.DefaultQuota.Burst < 0 {
		return nil, ErrInvalidConfig
	}
	cp := *c
	cp.Quotas = make(map[string]CallerQuota, len(c.Quotas))
	for k, q := range c.Quotas {
		if q.Rate < 0 || q.Burst < 0 {
			return nil, ErrInvalidConfig
		}
		cp.Quotas[k] = q
	}
	return &cp, nil
}

// NewHolder 以初始配置创建 Holder；初始配置非法时回退到默认配置。
func NewHolder(c *Config) *Holder {
	h := &Holder{}
	if vc, err := validate(c); err == nil {
		h.cur.Store(vc)
	} else {
		h.cur.Store(Default())
	}
	return h
}

// Get 读取当前配置（无锁原子读）。
func (h *Holder) Get() *Config {
	return h.cur.Load().(*Config)
}

// Update 原子替换配置；非法配置被拒绝（返回 ErrInvalidConfig），旧配置继续生效。
// 已在处理的请求持有的是旧快照，按旧规则收尾；之后的新请求按新规则判定。
func (h *Holder) Update(c *Config) error {
	vc, err := validate(c)
	if err != nil {
		return err
	}
	h.cur.Store(vc)
	return nil
}
