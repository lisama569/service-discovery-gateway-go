package registry

import (
	"sync"
	"time"
)

// fakeClock 是并发安全的可推进时钟，供测试注入。
type fakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }
func (c *fakeClock) get() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
