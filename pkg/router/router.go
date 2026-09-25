// pkg/router/router.go
//
// 路由层：无状态网关。客户端可把请求发给任意 router，由 router 按 key 转发到分片 Leader。
//
// 相对原始版本的修复（见 docs/BUGS.md）：
//
//	BUG-16 HandleRequest 从不转发请求，只回显一行 "Routed to shard N leader X" 字符串。
//	       整个项目的读写路径实际上不存在。现在实现真正的反向代理 + 重试。
//	BUG-17 Route 只在 leaderAddr 变化时才复用 ShardClient，但读取 client.leaderAddr
//	       时没有持锁；同时每次变更都 new 一个 http.Client —— 新客户端意味着新的连接池，
//	       在 Leader 频繁切换时会耗尽临时端口（TIME_WAIT 堆积）。
//	       现在每个分片复用一个 http.Client（共享 Transport），只替换 leader 地址。
//	BUG-18 requestCount 字段声明为 int64 但从未使用，也不原子。
//	BUG-19 无 key 校验：空 key、超长 key 会一路传到后端。
//	BUG-20 多副本重试不存在：Leader 切换瞬间返回 502 且不重试。
package router

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
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/distributed-kv/kvstore/pkg/metadata"
)

// 路由层默认限制。
const (
	defaultMaxKeyLen   = 1024
	defaultMaxBodyLen  = 8 << 20 // 8 MiB
	defaultMaxAttempts = 3
	defaultRetryDelay  = 20 * time.Millisecond
)

// Config 路由层配置。
type Config struct {
	// MaxKeyLen 允许的最大 key 长度。
	MaxKeyLen int
	// MaxBodyLen 允许的最大请求体长度。
	MaxBodyLen int64
	// MaxAttempts 一次请求最多尝试的后端数。
	MaxAttempts int
	// DialTimeout / ResponseHeaderTimeout 控制后端连接行为。
	DialTimeout           time.Duration
	ResponseHeaderTimeout time.Duration
	// Logger 为 nil 时使用 zap.NewNop()。
	Logger *zap.Logger
}

func (c Config) withDefaults() Config {
	if c.MaxKeyLen == 0 {
		c.MaxKeyLen = defaultMaxKeyLen
	}
	if c.MaxBodyLen == 0 {
		c.MaxBodyLen = defaultMaxBodyLen
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 500 * time.Millisecond
	}
	if c.ResponseHeaderTimeout == 0 {
		c.ResponseHeaderTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}
	return c
}

// shardClient 一个分片的转发客户端。
//
// http.Client 在整个分片生命周期内复用（BUG-17）；只有 leader 用原子指针替换。
type shardClient struct {
	shardID int
	nodes   []string

	// shared 由同进程所有分片共用，避免每个分片各建一个连接池。
	shared *http.Client

	leader atomic.Pointer[string]
	// lastUpdate 最近一次 Leader 刷新时间（原子，纳秒）。
	lastUpdate atomic.Int64
}

func (sc *shardClient) getLeader() string {
	if p := sc.leader.Load(); p != nil {
		return *p
	}
	return ""
}

func (sc *shardClient) setLeader(addr string) {
	sc.leader.Store(&addr)
	sc.lastUpdate.Store(time.Now().UnixNano())
}

// Router 路由层。
type Router struct {
	cfg Config

	meta metadata.Topology

	mu       sync.RWMutex
	clients  map[int]*shardClient
	allNodes map[string]struct{}

	httpClient *http.Client

	requestCount atomic.Uint64
	routeHits    atomic.Uint64
	routeMisses  atomic.Uint64
	forwards     atomic.Uint64
	retries      atomic.Uint64
	failures     atomic.Uint64

	server *http.Server
}

// NewRouter 创建路由器。
func NewRouter(meta metadata.Topology) *Router {
	return NewRouterWithConfig(meta, Config{})
}

// NewRouterWithConfig 创建路由器（带配置）。
func NewRouterWithConfig(meta metadata.Topology, cfg Config) *Router {
	cfg = cfg.withDefaults()

	transport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		// 长连接复用是千节点场景下的基本要求。
		DisableKeepAlives: false,
	}

	r := &Router{
		cfg:      cfg,
		meta:     meta,
		clients:  make(map[int]*shardClient),
		allNodes: make(map[string]struct{}),
		httpClient: &http.Client{
			Transport: transport,
			// 不设 Client.Timeout：由 per-request context 控制，避免截断慢查询。
		},
	}

	// 拓扑变化时刷新本地分片视图。
	meta.OnShardChange(func(ch metadata.ShardChange) {
		r.mu.Lock()
		defer r.mu.Unlock()
		switch ch.Type {
		case "delete":
			delete(r.clients, ch.ID)
		case "put":
			if sc, ok := r.clients[ch.ID]; ok && ch.Shard != nil {
				sc.setLeader(ch.Shard.Leader)
			}
		}
	})

	r.refresh()
	return r
}

// refresh 从元数据重建分片视图。
func (r *Router) refresh() {
	shards := r.meta.AllShards()

	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[int]struct{}, len(shards))
	nodes := make(map[string]struct{})
	for _, s := range shards {
		seen[s.ID] = struct{}{}
		if sc, ok := r.clients[s.ID]; ok {
			sc.setLeader(s.Leader)
			sc.nodes = append([]string(nil), s.Nodes...)
		} else {
			sc := &shardClient{
				shardID: s.ID,
				nodes:   append([]string(nil), s.Nodes...),
				shared:  r.httpClient,
			}
			sc.setLeader(s.Leader)
			r.clients[s.ID] = sc
		}
		for _, n := range s.Nodes {
			nodes[n] = struct{}{}
		}
	}
	for id := range r.clients {
		if _, ok := seen[id]; !ok {
			delete(r.clients, id)
		}
	}
	r.allNodes = nodes
}

// Route 返回 key 对应的分片客户端（BUG-17：返回副本，调用方不会读到并发修改）。
func (r *Router) Route(key string) (*shardClient, error) {
	shard := r.meta.GetShardForKey(key)
	if shard == nil {
		r.routeMisses.Add(1)
		return nil, fmt.Errorf("router: no shard owns key %q", key)
	}
	r.routeHits.Add(1)

	r.mu.RLock()
	sc, ok := r.clients[shard.ID]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		if sc, ok = r.clients[shard.ID]; !ok {
			sc = &shardClient{
				shardID: shard.ID,
				nodes:   append([]string(nil), shard.Nodes...),
				shared:  r.httpClient,
			}
			sc.setLeader(shard.Leader)
			r.clients[shard.ID] = sc
		}
		r.mu.Unlock()
	} else if cur := sc.getLeader(); cur != shard.Leader {
		sc.setLeader(shard.Leader)
	}
	return sc, nil
}

// candidates 返回该分片可尝试的后端顺序：Leader 优先，其后为其余成员。
func (sc *shardClient) candidates() []string {
	leader := sc.getLeader()
	out := make([]string, 0, len(sc.nodes)+1)
	if leader != "" {
		out = append(out, leader)
	}
	for _, n := range sc.nodes {
		if n != "" && n != leader {
			out = append(out, n)
		}
	}
	return out
}

// invalidateLeader 标记当前 Leader 失效，下一次请求将重新从元数据取。
func (sc *shardClient) invalidateLeader() { sc.setLeader("") }

// HandleKey 处理 /api/v1/key/{key}。
func (r *Router) HandleRequest(w http.ResponseWriter, req *http.Request) {
	r.requestCount.Add(1)

	vars := mux.Vars(req)
	key := vars["key"]

	if key == "" {
		http.Error(w, "router: empty key", http.StatusBadRequest)
		return
	}
	if len(key) > r.cfg.MaxKeyLen {
		http.Error(w, "router: key too long", http.StatusBadRequest)
		return
	}

	// 读取并限制请求体（仅写操作有体）。
	var body []byte
	if req.Body != nil && req.Method != http.MethodGet && req.Method != http.MethodHead {
		limited := io.LimitReader(req.Body, r.cfg.MaxBodyLen+1)
		b, err := io.ReadAll(limited)
		if err != nil {
			http.Error(w, "router: read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if int64(len(b)) > r.cfg.MaxBodyLen {
			http.Error(w, "router: body too large", http.StatusRequestEntityTooLarge)
			return
		}
		body = b
	}

	// key 可能包含需要转义的字符；用 PathEscape 保证后端正确解析（BUG-19 相关）。
	escaped := url.PathEscape(key)

	sc, err := r.Route(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	attempts := r.cfg.MaxAttempts
	cands := sc.candidates()
	if len(cands) == 0 {
		r.failures.Add(1)
		http.Error(w, "router: shard has no known members", http.StatusServiceUnavailable)
		return
	}
	if attempts > len(cands) {
		attempts = len(cands)
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		target := cands[i]
		if i > 0 {
			r.retries.Add(1)
		}
		status, hdr, respBody, err := r.forward(req.Context(), req, target, escaped, body)
		if err != nil {
			lastErr = err
			sc.invalidateLeader()
			r.cfg.Logger.Debug("forward failed", zap.String("target", target), zap.Error(err))
			select {
			case <-time.After(defaultRetryDelay):
			case <-req.Context().Done():
				http.Error(w, "router: client cancelled", http.StatusRequestTimeout)
				return
			}
			continue
		}

		// 后端明确报告"不是 Leader"时换一个后端重试。
		if status == http.StatusServiceUnavailable || status == http.StatusMovedPermanently {
			lastErr = fmt.Errorf("router: backend %s is not leader (status %d)", target, status)
			if i+1 < len(cands) {
				// 采用后端给出的 Leader 提示（若存在）。
				if hint := hdr.Get("X-Raft-Leader"); hint != "" {
					cands[i+1] = hint
					sc.setLeader(hint)
				}
			}
			continue
		}

		r.forwards.Add(1)
		for k, vs := range hdr {
			if k == "X-Raft-Leader" {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
		if len(respBody) > 0 {
			_, _ = w.Write(respBody)
		}
		return
	}

	r.failures.Add(1)
	msg := "router: all backends failed"
	if lastErr != nil {
		msg = "router: " + lastErr.Error()
	}
	http.Error(w, msg, http.StatusBadGateway)
}

// forward 把请求转发到单节点。
func (r *Router) forward(ctx context.Context, req *http.Request, target, escapedKey string, body []byte) (int, http.Header, []byte, error) {
	host := target
	if !strings.Contains(host, "://") {
		host = "http://" + host
	}
	u := strings.TrimRight(host, "/") + "/api/v1/key/" + escapedKey

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	out, err := http.NewRequestWithContext(ctx, req.Method, u, rdr)
	if err != nil {
		return 0, nil, nil, err
	}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		out.Header.Set("Content-Type", ct)
	}
	if body != nil {
		out.ContentLength = int64(len(body))
	}

	resp, err := r.httpClient.Do(out)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()

	// 限制后端响应读取，防止失控响应打爆路由层内存。
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, r.cfg.MaxBodyLen))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, respBody, nil
}

// NewHTTPServer 构造 HTTP 服务。
func (r *Router) NewHTTPServer(addr string) *http.Server {
	m := mux.NewRouter()
	m.HandleFunc("/api/v1/key/{key}", r.HandleRequest).
		Methods(http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete)
	m.HandleFunc("/healthz", r.handleHealth).Methods(http.MethodGet)
	m.HandleFunc("/stats", r.handleStats).Methods(http.MethodGet)

	srv := &http.Server{
		Addr:              addr,
		Handler:           m,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	r.server = srv
	return srv
}

// Health 健康检查。
func (r *Router) handleHealth(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	n := len(r.clients)
	r.mu.RUnlock()

	status := http.StatusOK
	state := "ok"
	if n == 0 {
		status = http.StatusServiceUnavailable
		state = "no-shards"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":     state,
		"shards":     n,
		"topology_v": r.meta.Version(),
		"requests":   r.requestCount.Load(),
	})
}

// Stats 路由统计，用于实验观测。
func (r *Router) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"requests":    r.requestCount.Load(),
		"route_hits":  r.routeHits.Load(),
		"route_miss":  r.routeMisses.Load(),
		"forwards":    r.forwards.Load(),
		"retries":     r.retries.Load(),
		"failures":    r.failures.Load(),
		"topology_v":  r.meta.Version(),
	})
}

// Stats 返回统计快照（测试与实验用）。
func (r *Router) Stats() map[string]uint64 {
	return map[string]uint64{
		"requests":   r.requestCount.Load(),
		"route_hits": r.routeHits.Load(),
		"route_miss": r.routeMisses.Load(),
		"forwards":   r.forwards.Load(),
		"retries":    r.retries.Load(),
		"failures":   r.failures.Load(),
	}
}

// Refresh 强制从元数据刷新分片视图（拓扑事件丢失时的兜底）。
func (r *Router) Refresh() { r.refresh() }

// Shutdown 优雅关闭。
func (r *Router) Shutdown(ctx context.Context) error {
	if r.server == nil {
		return nil
	}
	return r.server.Shutdown(ctx)
}
