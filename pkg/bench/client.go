// pkg/bench/client.go
//
// 实验用的最小 HTTP 客户端。
//
// 为什么不直接用 pkg/client：本文的对照实验需要把"路由开销"与"共识开销"分开归因，
// 因此实验驱动必须能直接打到某个具体节点，也能打到 router。pkg/client 会自己做
// 路由缓存与重定向，属于被测量的对象而不是测量工具。
package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Executor 一次操作的实际执行者。
type Executor interface {
	Do(ctx context.Context, op OpKind, key, value string) error
	Close()
}

// httpExecutor 直接通过 HTTP 访问数据面。
type httpExecutor struct {
	endpoints []string
	client    *http.Client
	rr        uint64
}

// NewHTTPExecutor 创建 HTTP 执行器。
func NewHTTPExecutor(endpoints []string, timeout time.Duration, maxConnsPerHost int) Executor {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if maxConnsPerHost <= 0 {
		maxConnsPerHost = 256
	}
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        maxConnsPerHost * 4,
		MaxIdleConnsPerHost: maxConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &httpExecutor{
		endpoints: normalizeEndpoints(endpoints),
		client:    &http.Client{Transport: tr, Timeout: timeout},
	}
}

func normalizeEndpoints(eps []string) []string {
	out := make([]string, 0, len(eps))
	for _, e := range eps {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if !strings.Contains(e, "://") {
			e = "http://" + e
		}
		out = append(out, strings.TrimRight(e, "/"))
	}
	return out
}

func (h *httpExecutor) pick() string {
	if len(h.endpoints) == 1 {
		return h.endpoints[0]
	}
	h.rr++
	return h.endpoints[int(h.rr)%len(h.endpoints)]
}

func (h *httpExecutor) Do(ctx context.Context, op OpKind, key, value string) error {
	base := h.pick()
	u := base + "/api/v1/key/" + url.PathEscape(key)

	var req *http.Request
	var err error
	switch op {
	case OpPut:
		body, mErr := json.Marshal(map[string]string{"value": value})
		if mErr != nil {
			return mErr
		}
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(body))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
	default:
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	}
	if err != nil {
		return err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	// 必须把响应体读完才能复用连接；否则每个请求都会新建 TCP 连接，
	// 测量结果反映的是连接建立开销而不是系统性能。
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("bench: %s %s -> %s", op, key, resp.Status)
}

func (h *httpExecutor) Close() {
	h.client.CloseIdleConnections()
}

// execute 在 Runner 中执行一次操作。
func (r *Runner) execute(ctx context.Context, op OpKind, key, value string) error {
	if r.exec == nil {
		return fmt.Errorf("bench: no executor configured")
	}
	return r.exec.Do(ctx, op, key, value)
}
