package gateway

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/ratelimit"
)

// Server 把 Router 暴露为回环地址上的 HTTP 网关。
type Server struct {
	addr   string
	router *Router
	srv    *http.Server
}

// NewServer 创建 HTTP 网关。
// 路径形如 /svc/{service}，调用方由 X-Caller 头给出，会话键由 X-Session 头给出。
func NewServer(addr string, r *Router) *Server {
	return &Server{addr: addr, router: r}
}

// Start 启动监听。
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/svc/", s.handle)
	s.srv = &http.Server{Addr: s.addr, Handler: mux}
	ln, err := s.startListener()
	if err != nil {
		return err
	}
	go func() { _ = s.srv.Serve(ln) }()
	return nil
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Path[len("/svc/"):]
	req := Request{
		Service:    service,
		Caller:     r.Header.Get("X-Caller"),
		SessionKey: r.Header.Get("X-Session"),
	}
	res, err := s.router.Do(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, ErrGatewayShutting):
			w.WriteHeader(http.StatusServiceUnavailable)
		case errors.Is(err, ratelimit.ErrRateLimited):
			w.WriteHeader(http.StatusTooManyRequests)
		case errors.Is(err, ErrNoHealthyInstance):
			w.WriteHeader(http.StatusServiceUnavailable)
		case errors.Is(err, ErrRetryExhausted):
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("X-Instance-Id", res.InstanceID)
	w.Write([]byte("routed to " + res.InstanceID))
}

// Addr 返回监听地址。
func (s *Server) Addr() string {
	if s.srv != nil {
		// Start 后通过监听器拿真实地址
	}
	return s.addr
}

// Shutdown 优雅关闭：先让路由层拒新请求，再等在途收尾，最后关监听。
func (s *Server) Shutdown() error {
	s.router.BeginShutdown()
	if ok := s.router.WaitInflight(3 * time.Second); !ok {
		s.router.log("shutdown: inflight requests still present")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}
