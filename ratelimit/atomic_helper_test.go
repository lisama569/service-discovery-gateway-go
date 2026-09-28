package ratelimit

import "sync/atomic"

func addI64(p *int64, n int64) int64 { return atomic.AddInt64(p, n) }
func loadI64(p *int64) int64         { return atomic.LoadInt64(p) }
