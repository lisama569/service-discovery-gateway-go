// 本地演示：启动假后端、注册实例、发请求观察路由结果。
//
// 运行：go run ./cmd/gateway
// 场景：注册 3 个实例 → 粘性会话路由 → 一个实例心跳过期被剔除 →
// 一个实例优雅下线（存量会话收尾、新会话改道）→ 配置热更新收紧限流。
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/internal/backend"
	"github.com/lisama569/service-discovery-gateway-go/internal/gateway"
	"github.com/lisama569/service-discovery-gateway-go/internal/limiter"
	"github.com/lisama569/service-discovery-gateway-go/internal/registry"
)

func main() {
	ctx := context.Background()
	reg := registry.New(2 * time.Second) // 心跳 TTL 2s，后台 1s 清理一次
	defer reg.Close()

	lim := limiter.New(limiter.Config{
		Rules: map[string]limiter.Rule{"demo-caller": {Permits: 100, Interval: 10 * time.Second}},
	})
	gw := gateway.New(gateway.Options{
		Registry:       reg,
		Limiter:        lim,
		Transport:      gateway.NewHTTPTransport(),
		BackendTimeout: 500 * time.Millisecond,
		MaxRetries:     2,
	})

	// 启动 3 个回环假后端并注册。
	ids := []string{"inst-a", "inst-b", "inst-c"}
	heartbeats := map[string]context.CancelFunc{}
	for _, id := range ids {
		b, err := backend.Start(ctx, id, 20*time.Millisecond, false)
		if err != nil {
			log.Fatalf("start backend: %v", err)
		}
		defer b.Close(ctx)
		must(reg.Register(registry.Instance{Service: "shop", ID: id, Addr: b.Addr(), Healthy: true}))
		hbCtx, cancel := context.WithCancel(ctx)
		heartbeats[id] = cancel
		go heartbeatLoop(hbCtx, reg, id)
	}
	fmt.Println("== 3 instances registered, heartbeating ==")
	routeN(gw, "sess-1", 3)

	fmt.Println("\n== inst-c stops heartbeating; expect sweep in ~2-3s ==")
	heartbeats["inst-c"]()
	time.Sleep(3 * time.Second)
	listInstances(reg)
	routeN(gw, "sess-1", 3)

	fmt.Println("\n== inst-b begins graceful drain ==")
	must(reg.BeginDrain("shop", "inst-b"))
	routeN(gw, "sess-1", 2)   // 存量会话：若绑定 inst-b 仍走 inst-b
	routeN(gw, "sess-new", 4) // 新会话：只落 inst-a
	listInstances(reg)

	fmt.Println("\n== hot-update limiter: quota 100 -> 3 ==")
	lim.Update(limiter.Config{
		Rules: map[string]limiter.Rule{"demo-caller": {Permits: 3, Interval: 10 * time.Second}},
	})
	routeN(gw, "sess-new", 6) // 前 3 个放行，之后被限流

	stats := gw.Stats()
	fmt.Printf("\n== gateway stats: routed=%d rejected=%d retries=%d ==\n",
		stats.Routed, stats.Rejected, stats.Retries)
}

func heartbeatLoop(ctx context.Context, reg *registry.Registry, id string) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := reg.Heartbeat("shop", id); err != nil {
				log.Printf("heartbeat %s: %v", id, err)
			}
		}
	}
}

func routeN(gw *gateway.Gateway, sess string, n int) {
	for i := 0; i < n; i++ {
		id, err := gw.Route("demo-caller", "shop", sess)
		if err != nil {
			fmt.Printf("  route(%s) -> REJECTED: %v\n", sess, err)
		} else {
			fmt.Printf("  route(%s) -> %s\n", sess, id)
		}
	}
}

func listInstances(reg *registry.Registry) {
	snaps, err := reg.List("shop")
	if err != nil {
		fmt.Println("  list:", err)
		return
	}
	for _, s := range snaps {
		fmt.Printf("  instance %s status=%s healthy=%v\n", s.ID, s.Status, s.Healthy)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
