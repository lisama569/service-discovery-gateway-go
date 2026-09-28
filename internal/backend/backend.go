// Package backend 提供回环地址上的假后端，用于本地模拟真实服务实例。
package backend

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Backend 是一个跑在 127.0.0.1 上的假后端 HTTP 服务。
type Backend struct {
	id       string
	latency  time.Duration
	hang     atomic.Bool
	srv      *http.Server
	ln       net.Listener
	inflight atomic.Int64
}

// Start 在回环地址随机端口启动假后端。
// id 会体现在响应体中；latency 模拟处理耗时；hang 为 true 时请求永不返回（模拟后端无响应）。
func Start(ctx context.Context, id string, latency time.Duration, hang bool) (*Backend, error) {
	b := &Backend{id: id, latency: latency}
	b.hang.Store(hang)
	mux := http.NewServeMux()
	mux.Handle("/work", b.Handler())
	mux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("backend %s listen: %w", id, err)
	}
	b.ln = ln
	b.srv = &http.Server{Handler: mux}
	go func() { _ = b.srv.Serve(ln) }()
	go func() {
		<-ctx.Done()
		_ = b.Close(context.Background())
	}()
	return b, nil
}

// Addr 返回监听地址（host:port）。
func (b *Backend) Addr() string { return b.ln.Addr().String() }

// Handler 返回 http.Handler，便于进程内直接挂载测试。
// 响应体为实例 id；在途请求计数用于验证优雅收尾。
func (b *Backend) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.inflight.Add(1)
		defer b.inflight.Add(-1)
		if b.hang.Load() {
			// 模拟后端无响应：直到连接被客户端取消
			<-r.Context().Done()
			return
		}
		if b.latency > 0 {
			select {
			case <-time.After(b.latency):
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, b.id)
	})
}

// Inflight 返回当前在途请求数。
func (b *Backend) Inflight() int64 { return b.inflight.Load() }

// SetHang 动态切换"无响应"模式，用于测试故障注入。
func (b *Backend) SetHang(hang bool) { b.hang.Store(hang) }

// Close 优雅关闭，等待在途请求结束。
func (b *Backend) Close(ctx context.Context) error {
	return b.srv.Shutdown(ctx)
}
