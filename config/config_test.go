package config

import (
	"errors"
	"testing"
)

func TestHotReloadRejectsInvalid(t *testing.T) {
	h := NewHolder(Default())
	good := Default()
	good.MaxRetries = 3
	if err := h.Update(good); err != nil {
		t.Fatal(err)
	}
	if h.Get().MaxRetries != 3 {
		t.Fatal("valid update must take effect")
	}
	bad := Default()
	bad.BackendTimeout = 0
	if err := h.Update(bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid config must be rejected: %v", err)
	}
	if h.Get().MaxRetries != 3 {
		t.Fatalf("rejected update must keep old config, got MaxRetries=%d", h.Get().MaxRetries)
	}
	t.Log("[input/decision] 合法更新(MaxRetries=3)生效；非法更新(timeout=0)被拒，旧配置继续")
}

func TestQuotasMapIsolated(t *testing.T) {
	c := Default()
	c.Quotas["a"] = CallerQuota{Rate: 1, Burst: 1}
	h := NewHolder(c)
	// 外部再改传入的 map 不应影响 Holder 内部快照
	c.Quotas["a"] = CallerQuota{Rate: 999, Burst: 999}
	c.Quotas["b"] = CallerQuota{Rate: 2, Burst: 2}
	if got := h.Get().Quotas["a"]; got.Rate != 1 {
		t.Fatalf("config snapshot must be isolated from caller map mutation: %+v", got)
	}
	if _, ok := h.Get().Quotas["b"]; ok {
		t.Fatal("later mutation of input map must not leak into stored config")
	}
	t.Log("[input/decision] 存储配置深拷贝，外部 map 后续修改不影响网关快照")
}
