// Package limiter 提供按调用方配额的限流器，支持配置热更新。
//
// 算法：每调用方固定窗口计数。窗口内计数达到 Permits 即拒绝。
// 计数器与配置读写同锁，保证并发下计数不丢、不重。
package limiter

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrRateLimited 表示调用方超出配额被拒绝。
	ErrRateLimited = errors.New("limiter: rate limited")
	// ErrUnknownCaller 表示调用方没有配置配额且不允许默认放行。
	ErrUnknownCaller = errors.New("limiter: unknown caller")
)

// Rule 是单个调用方的配额规则：interval 内最多 permits 次。
type Rule struct {
	Permits  int
	Interval time.Duration
}

// Config 是限流配置，可整体热更新。
type Config struct {
	Rules       map[string]Rule
	DefaultRule Rule
	// AllowUnknown 为 true 时未知调用方按 DefaultRule 限流，否则拒绝。
	AllowUnknown bool
}

type window struct {
	start time.Time
	used  int
}

// Limiter 按调用方做固定窗口限流，并发安全。
type Limiter struct {
	mu      sync.Mutex
	cfg     Config
	windows map[string]*window
	now     func() time.Time
}

// New 创建限流器。
func New(cfg Config) *Limiter {
	if cfg.Rules == nil {
		cfg.Rules = map[string]Rule{}
	}
	return &Limiter{
		cfg:     cfg,
		windows: make(map[string]*window),
		now:     time.Now,
	}
}

// Allow 判定 caller 本次请求是否放行。放行即计入一次配额。
func (l *Limiter) Allow(caller string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	rule, ok := l.cfg.Rules[caller]
	if !ok {
		if !l.cfg.AllowUnknown {
			return fmt.Errorf("%w: %q", ErrUnknownCaller, caller)
		}
		rule = l.cfg.DefaultRule
	}
	if rule.Permits <= 0 || rule.Interval <= 0 {
		return fmt.Errorf("%w: caller %q has zero quota", ErrRateLimited, caller)
	}
	now := l.now()
	w, ok := l.windows[caller]
	if !ok || now.Sub(w.start) >= rule.Interval {
		w = &window{start: now}
		l.windows[caller] = w
	}
	if w.used >= rule.Permits {
		return fmt.Errorf("%w: caller %q used %d/%d in window",
			ErrRateLimited, caller, w.used, rule.Permits)
	}
	w.used++
	return nil
}

// Update 热更新配置：新请求按新规则判定。
// 规则未变的调用方保留当前窗口计数（避免热更新被用来刷配额）；
// 规则变化或新增的调用方重置窗口，按新规则重新计数。
func (l *Limiter) Update(cfg Config) {
	if cfg.Rules == nil {
		cfg.Rules = map[string]Rule{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for caller := range l.windows {
		oldRule, hadOld := l.cfg.Rules[caller]
		newRule, hasNew := cfg.Rules[caller]
		if !hadOld || !hasNew || oldRule != newRule {
			delete(l.windows, caller)
		}
	}
	l.cfg = cfg
}

// Snapshot 返回调用方当前窗口用量（用于测试与观测）。
func (l *Limiter) Snapshot(caller string) (used int, limit int, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rule, hasRule := l.cfg.Rules[caller]
	if !hasRule {
		if !l.cfg.AllowUnknown {
			return 0, 0, false
		}
		rule = l.cfg.DefaultRule
	}
	w, hasWin := l.windows[caller]
	if !hasWin {
		return 0, rule.Permits, true
	}
	return w.used, rule.Permits, true
}
