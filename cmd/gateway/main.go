// 本地可跑的服务发现网关示例：
// 启动回环假后端 -> 注册/心跳 -> 发请求观察调度 -> 限流热更新 -> 优雅下线。
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/lisama569/service-discovery-gateway-go/backend"
	"github.com/lisama569/service-discovery-gateway-go/config"
	"github.com/lisama569/service-discovery-gateway-go/gateway"
	"github.com/lisama569/service-discovery-gateway-go/ratelimit"
	"github.com/lisama569/service-discovery-gateway-go/registry"
)

func main() {
	const service = "demo-svc"

	// 1) 起三个回环假后端
	backends := make([]*backend.Server, 0, 3)
	ids := []string{"inst-1", "inst-2", "inst-3"}
	for _, id := range ids {
		b := backend.New(id, "127.0.0.1:0", 10*time.Millisecond, false)
		must(b.Start())
		backends = append(backends, b)
	}
	defer func() {
		for _, b := range backends {
			_ = b.Shutdown()
		}
	}()

	// 2) 注册中心：心跳 TTL 1s，每 200ms 扫描过期
	reg := registry.New(1*time.Second, 200*time.Millisecond)
	defer reg.Close()

	stopBeat := make(chan struct{})
	var beatWG sync.WaitGroup
	for _, b := range backends {
		must(reg.Register(registry.Instance{ID: b.Addr(), Service: service, Addr: b.Addr(), Weight: 1}))
		beatWG.Add(1)
		go func(id string) {
			defer beatWG.Done()
			t := time.NewTicker(300 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-stopBeat:
					return
				case <-t.C:
					_ = reg.Heartbeat(id)
				}
			}
		}(b.Addr())
	}
	// 注册的 ID 用 addr，方便演示；日志里展示人类可读 ID
	nameByAddr := map[string]string{}
	for i, b := range backends {
		nameByAddr[b.Addr()] = ids[i]
	}

	// 3) 网关
	cfg := config.Default()
	holder := config.NewHolder(cfg)
	lim := ratelimit.New()
	r := gateway.New(reg, holder, lim)
	r.SetLogger(logf)
	srv := gateway.NewServer("127.0.0.1:0", r)
	must(srv.Start())

	call := func(caller, session string) (int, string, string) {
		req, _ := http.NewRequestWithContext(context.Background(),
			http.MethodGet, "http://"+srv.Addr()+"/svc/"+service, nil)
		req.Header.Set("X-Caller", caller)
		if session != "" {
			req.Header.Set("X-Session", session)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, "", err.Error()
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("X-Instance-Id"), string(body)
	}
	show := func(caller, session string) {
		code, inst, body := call(caller, session)
		fmt.Printf("caller=%-6s session=%-4s -> %d inst=%s (%s)\n",
			caller, session, code, nameByAddr[inst], body)
	}

	fmt.Println("== 会话亲和：s1 三次请求应固定在同一实例 ==")
	show("alice", "s1")
	show("alice", "s1")
	show("alice", "s1")

	fmt.Println("== 限流热更新：把 bob 的配额降为 2/s, burst=2 ==")
	newCfg := *cfg
	newCfg.Quotas = map[string]config.CallerQuota{"bob": {Rate: 2, Burst: 2}}
	must(holder.Update(&newCfg))
	for i := 0; i < 5; i++ {
		show("bob", "")
	}

	fmt.Println("== 优雅下线 s1 所在实例：旧会话仍走 draining 实例收尾，新会话去别处 ==")
	_, s1Addr, _ := call("alice", "s1")
	target := s1Addr // 下线 s1 当前亲和的实例，才能看出“旧会话收尾”
	must(reg.BeginDrain(target))
	show("alice", "s1") // 若 s1 原本在 inst-1，仍由 draining 实例收尾
	show("carol", "s9") // 新会话不会选中 draining 实例
	must(reg.Deregister(target))
	show("alice", "s1") // 已摘除：s1 漂移到其他健康实例
	show("alice", "s1") // 并重建亲和

	// 4) 网关优雅关闭 + 停止心跳
	fmt.Println("== 网关优雅关闭 ==")
	close(stopBeat)
	beatWG.Wait()
	must(srv.Shutdown())
	fmt.Println("gateway stopped, backend handled counts:",
		backends[0].Handled(), backends[1].Handled(), backends[2].Handled())
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func logf(format string, args ...any) {
	fmt.Printf("  [route] "+format+"\n", args...)
}
