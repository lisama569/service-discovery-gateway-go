// Package ratelimit 实现按调用方的令牌桶限流。
package ratelimit

import (
	"errors"
	"sync"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/config"
)

// ErrRateLimited 调用方超出配额，请求被拒绝。
var ErrRateLimited = errors.New("ratelimit: quota exceeded")

// Limiter 按调用方维护令牌桶；热更新改变某调用方配额时，
// 旧桶按新参数重建，新请求一律按新配置判定。
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

type bucket struct {
	tokens float64
	rate   int
	burst  int
	last   time.Time
}

// New 创建限流器。
func New() *Limiter {
	return &Limiter{buckets: make(map[string]*bucket), now: time.Now}
}

// quota 取调用方配额，未配置则用默认配额。
func quotaFor(caller string, cfg *config.Config) config.CallerQuota {
	if q, ok := cfg.Quotas[caller]; ok {
		return q
	}
	return cfg.DefaultQuota
}

// Allow 判定 caller 的本请求是否放行。
// 判定过程在互斥锁内完成“补充令牌 + 扣减”，
// 因此并发下每个请求恰好被扣一次令牌，不会漏算或重复扣减。
func (l *Limiter) Allow(caller string, cfg *config.Config) error {
	q := quotaFor(caller, cfg)
	// Rate=0 表示完全禁止该调用方。
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[caller]
	if !ok {
		b = &bucket{tokens: float64(q.Burst), rate: q.Rate, burst: q.Burst, last: now}
		l.buckets[caller] = b
	}
	// 配置热更新：配额参数变化时按新参数重建桶，
	// 初始令牌取 min(旧余量, 新桶容量)，既不偷放也不重复计数。
	if b.rate != q.Rate || b.burst != q.Burst {
		tokens := b.tokens
		if tokens > float64(q.Burst) {
			tokens = float64(q.Burst)
		}
		b = &bucket{tokens: tokens, rate: q.Rate, burst: q.Burst, last: now}
		l.buckets[caller] = b
	}

	if q.Rate == 0 {
		return ErrRateLimited
	}
	// 按距上次判定的时长补充令牌。
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * float64(q.Rate)
	if b.tokens > float64(q.Burst) {
		b.tokens = float64(q.Burst)
	}
	if b.tokens < 1 {
		return ErrRateLimited
	}
	b.tokens--
	return nil
}
