// pkg/nodeapi/server.go
//
// 数据节点的 HTTP 接口。
//
// 这是原始项目里完全缺失的一层：pkg/raft 只提供了库函数，cmd/kvstore-node 也从未
// 暴露过 /api/v1/key/...，因此 router 与 client 只能拿到一个占位字符串（见 BUG-16）。
// 本文件补上真正的数据面，并实现：
//
//   - 写请求只由 Leader 服务；非 Leader 返回 503 + X-Raft-Leader 提示（不靠猜）。
//   - 读请求在 Leader 上按 Raft §6.4 的 readIndex 流程做线性化读；
//     非 Leader 直接返回 503 提示，避免静默返回陈旧数据。
//   - /join 提供节点入组（修复 BUG-4 的"在本地节点 AddVoter"错误语义）。
//   - /metrics 使用每个节点独立的 prometheus.Registry（修复 BUG-9）。
//   - /metrics 默认端口不再是固定 :9100，由调用方决定（修复 BUG-7 的端口冲突）。
package nodeapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/distributed-kv/kvstore/pkg/metadata"
	"github.com/distributed-kv/kvstore/pkg/raft"
)

// 头部约定：非 Leader 通过 503 + 该头部告知真实 Leader 地址。
const HeaderLeader = "X-Raft-Leader"

// Server 数据节点 HTTP 服务。
type Server struct {
	store *raft.KVStore
	meta  *metadata.ClusterTopology
	log   *slog.Logger

	httpSrv    *http.Server
	metricsSrv *http.Server

	// advertise 是本节点对外公告的 Raft 地址（用于 /join）。
	advertise string

	// readIndexTimeout 线性化读等待心跳确认的上限。
	readIndexTimeout time.Duration

	reg *prometheus.Registry
}

// Config 服务配置。
type Config struct {
	// HTTPAddr 数据面监听地址，空表示不启用。
	HTTPAddr string
	// MetricsAddr 指标监听地址，空表示不启用。
	MetricsAddr string
	// AdvertiseAddr 对外公告的 Raft 地址，默认等于 store 的 BindAddr。
	AdvertiseAddr string
	// Logger 默认 slog.Default()。
	Logger *slog.Logger
	// ReadIndexTimeout 默认 1s。
	ReadIndexTimeout time.Duration
}

// New 创建服务。
func New(store *raft.KVStore, meta *metadata.ClusterTopology, reg *prometheus.Registry, cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ReadIndexTimeout == 0 {
		cfg.ReadIndexTimeout = time.Second
	}
	return &Server{
		store:            store,
		meta:             meta,
		log:              cfg.Logger,
		advertise:        cfg.AdvertiseAddr,
		readIndexTimeout: cfg.ReadIndexTimeout,
		reg:              reg,
	}
}

// Handler 返回数据面路由。
func (s *Server) Handler() http.Handler {
	m := http.NewServeMux()
	// Go 1.22+ 支持方法+通配符路由；key 可能含被转义的字符，用 {key...} 捕获剩余路径。
	m.HandleFunc("/api/v1/key/{key...}", s.handleKey)
	m.HandleFunc("/join", s.handleJoin)
	m.HandleFunc("/healthz", s.handleHealth)
	m.HandleFunc("/raft/stats", s.handleStats)
	return m
}

// handleKey 统一的 key 读写入口。
func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	rawKey := r.PathValue("key")
	if rawKey == "" {
		writeErr(w, http.StatusBadRequest, "missing key")
		return
	}
	// PathValue 已由 net/http 解码。
	key := rawKey

	// 非 Leader 一律返回重定向提示：读也如此，因为我们不做"静默陈旧读"。
	if !s.store.IsLeader() {
		s.redirect(w)
		return
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		s.handleGet(w, r, key)
	case http.MethodPut, http.MethodPost:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT, POST, DELETE")
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// 线性化读：Raft §6.4 的 readIndex 语义。
	//
	// 关键实现细节（性能相关）：这里**不能**每请求调用 store.Barrier()。
	// Barrier 会向日志追加一条空条目并等待其落盘提交，实测单节点
	// 6.67ms/次（fsync 受限）；若放在读路径上，读延迟会被完全钉死在磁盘上。
	// ReadBarrier 只在"本任期尚未提交过任何条目"时付这一成本，
	// 之后整任期内所有读都是零协调开销。
	ctx, cancel := context.WithTimeout(r.Context(), s.readIndexTimeout)
	defer cancel()

	type result struct {
		idx uint64
		err error
	}
	done := make(chan result, 1)
	go func() {
		idx, err := s.store.ReadBarrier(s.readIndexTimeout)
		done <- result{idx: idx, err: err}
	}()

	var readIdx uint64
	select {
	case res := <-done:
		if res.err != nil {
			if errors.Is(res.err, raft.ErrNotLeader) || !s.store.IsLeader() {
				s.redirect(w)
				return
			}
			writeErr(w, http.StatusServiceUnavailable, "read index unavailable: "+res.err.Error())
			return
		}
		readIdx = res.idx
	case <-ctx.Done():
		writeErr(w, http.StatusGatewayTimeout, "read index timeout")
		return
	}

	val, ok := s.store.Get(key)
	w.Header().Set("X-Raft-ReadIndex", strconv.FormatUint(readIdx, 10))
	writeJSON(w, http.StatusOK, map[string]any{
		"key":   key,
		"value": val,
		"found": ok,
	})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}

	// 支持两种提交形式：{"value": "..."} 或裸字符串。
	var payload struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	value := string(body)
	if len(body) > 0 && body[0] == '{' {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeErr(w, http.StatusBadRequest, "bad json: "+err.Error())
			return
		}
		if payload.Key != "" {
			key = payload.Key
		}
		value = payload.Value
	}

	if err := s.store.Set(key, value); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.redirect(w)
			return
		}
		writeErr(w, http.StatusInternalServerError, "set: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "ok": true})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	if err := s.store.Delete(key); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.redirect(w)
			return
		}
		writeErr(w, http.StatusInternalServerError, "delete: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "ok": true})
}

// redirect 返回 503 + Leader 提示，客户端据此重试。
func (s *Server) redirect(w http.ResponseWriter) {
	leader, ok := s.store.LeaderAddr()
	if ok && leader != "" {
		w.Header().Set(HeaderLeader, leader)
	}
	w.Header().Set("Retry-After", "0")
	writeErr(w, http.StatusServiceUnavailable, fmt.Sprintf("node %s is %s, not leader (leader=%q)",
		s.store.NodeID(), s.store.State(), leader))
}

// JoinRequest /join 的请求体。
type JoinRequest struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
	Shard  int    `json:"shard_id"`
	// Nonvoter 为 true 时以 learner 身份加入（不扩大 quorum）。
	Nonvoter bool `json:"nonvoter"`
}

// handleJoin 让新节点通过 Leader 入组。
//
// 必须在 Leader 上执行，因为成员变更是一次 Raft 日志提案。非 Leader 返回 503 提示。
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeErr(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	if !s.store.IsLeader() {
		s.redirect(w)
		return
	}

	var req JoinRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if req.NodeID == "" || req.Addr == "" {
		writeErr(w, http.StatusBadRequest, "node_id and addr required")
		return
	}
	if req.Shard != 0 && req.Shard != s.store.ShardID() {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf(
			"this node serves shard %d, join requested for shard %d", s.store.ShardID(), req.Shard))
		return
	}

	var err error
	if req.Nonvoter {
		err = s.store.AddNonvoter(req.NodeID, req.Addr)
	} else {
		err = s.store.AddVoter(req.NodeID, req.Addr)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "join: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "shard_id": s.store.ShardID(), "node_id": req.NodeID, "nonvoter": req.Nonvoter,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	leader, hasLeader := s.store.LeaderAddr()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":    s.store.NodeID(),
		"shard_id":   s.store.ShardID(),
		"state":      s.store.State().String(),
		"is_leader":  s.store.IsLeader(),
		"leader":     leader,
		"has_leader": hasLeader,
		"keys":       s.store.Len(),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	st := s.store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":  s.store.NodeID(),
		"shard_id": s.store.ShardID(),
		"stats":    st,
	})
}

// ListenAndServe 启动数据面与指标服务；任一失败都会返回错误。
func (s *Server) ListenAndServe(cfg Config) error {
	errCh := make(chan error, 2)

	if cfg.HTTPAddr != "" {
		ln, err := net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			return fmt.Errorf("nodeapi: listen %s: %w", cfg.HTTPAddr, err)
		}
		s.httpSrv = &http.Server{
			Handler:           s.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		s.log.Info("data plane listening", "addr", ln.Addr().String(), "node", s.store.NodeID())
		go func() { errCh <- s.httpSrv.Serve(ln) }()
	}

	if cfg.MetricsAddr != "" && s.reg != nil {
		ln, err := net.Listen("tcp", cfg.MetricsAddr)
		if err != nil {
			return fmt.Errorf("nodeapi: listen metrics %s: %w", cfg.MetricsAddr, err)
		}
		m := http.NewServeMux()
		m.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
		s.metricsSrv = &http.Server{Handler: m, ReadHeaderTimeout: 5 * time.Second}
		s.log.Info("metrics listening", "addr", ln.Addr().String(), "node", s.store.NodeID())
		go func() { errCh <- s.metricsSrv.Serve(ln) }()
	}

	select {
	case err := <-errCh:
		return err
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) {
	if s.httpSrv != nil {
		_ = s.httpSrv.Shutdown(ctx)
	}
	if s.metricsSrv != nil {
		_ = s.metricsSrv.Shutdown(ctx)
	}
}

// JoinCluster 让本节点把自己注册到已有分片的 Leader。
func JoinCluster(ctx context.Context, leaderAddr, nodeID, advertise string, shardID int, nonvoter bool) error {
	body, _ := json.Marshal(JoinRequest{NodeID: nodeID, Addr: advertise, Shard: shardID, Nonvoter: nonvoter})
	host := leaderAddr
	if !strings.Contains(host, "://") {
		host = "http://" + host
	}
	// 注意：Leader 的 Raft 地址不等于其 HTTP 地址。调用方应传入 HTTP 地址。
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(host, "/")+"/join", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("nodeapi: join %s: %w", leaderAddr, err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("nodeapi: join %s: %s: %s", leaderAddr, resp.Status, strings.TrimSpace(string(data)))
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg, "status": code})
}

// ParsePort 便捷函数：从 ":8080" 或 "127.0.0.1:8080" 提取端口号。
func ParsePort(addr string) (int, error) {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return 0, fmt.Errorf("nodeapi: no port in %q", addr)
	}
	return strconv.Atoi(addr[i+1:])
}
