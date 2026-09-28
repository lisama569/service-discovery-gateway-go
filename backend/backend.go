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

// Server 是一个假后端实例：可配置延迟、可模拟挂起、可优雅关停。
type Server struct {
	id       string
	addr     string
	delay    time.Duration
	hang     bool
	srv      *http.Server
	inflight atomic.Int64
	handled  atomic.Int64
}

// New 创建假后端；delay 为固定响应延迟，hang 为 true 时请求会挂住直到被取消，
// 用来模拟“后端长时间无响应”的超时场景。
func New(id, addr string, delay time.Duration, hang bool) *Server {
	return &Server{id: id, addr: addr, delay: delay, hang: hang}
}

// Start 在回环地址上启动 HTTP 服务。
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	s.srv = &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("backend %s listen: %w", s.id, err)
	}
	s.addr = ln.Addr().String()
	go func() { _ = s.srv.Serve(ln) }()
	return nil
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	defer s.handled.Add(1)

	ctx := r.Context()
	if s.hang {
		// 模拟后端无响应：直到客户端超时断开或服务关停才退出。
		<-ctx.Done()
		return
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return
		}
	}
	w.Header().Set("X-Backend-Id", s.id)
	fmt.Fprintf(w, "served by %s", s.id)
}

// Addr 返回实际监听地址。
func (s *Server) Addr() string { return s.addr }

// Inflight 返回当前在途请求数。
func (s *Server) Inflight() int64 { return s.inflight.Load() }

// Handled 返回已处理完成的请求总数。
func (s *Server) Handled() int64 { return s.handled.Load() }

// Shutdown 优雅关停：等在途请求处理完（挂起的请求会随 ctx 取消而结束）。
func (s *Server) Shutdown() error {
	if s.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
