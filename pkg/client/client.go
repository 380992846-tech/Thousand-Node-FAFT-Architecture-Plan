// pkg/client/client.go
//
// 客户端 SDK：本地缓存路由（key 区间 → 分片 → Leader），失效后自动刷新并重定向。
//
// 相对原始版本的修复（见 docs/BUGS.md）：
//
//	BUG-21 Get/Set/Delete 在出错时直接递归调用自身，没有任何重试上限或退避。
//	       Leader 持续不可用时会无限递归，最终栈溢出崩溃（不是返回错误）。
//	       现在改为有界的重定向循环 + 指数退避。
//	BUG-22 URL 拼接 "http://%s/api/v1/key/%s" 未对 key 做转义。key 中含 '/'
//	       会被后端当成路径分隔符，含 '?' '#' '%' 会截断或破坏 URL。
//	BUG-23 路由缓存以"单个 key"为键。亿万级 key 空间下缓存永不命中且无限增长。
//	       现在以"分片区间"为键（区间数 = 分片数），缓存规模与 key 基数无关。
//	BUG-24 不存在 singleflight：冷启动时并发的 N 个请求会各自查一次元数据。
//	BUG-25 TTL 固定 10s 且无抖动，所有客户端会在同一时刻集体失效（惊群）。
//	BUG-26 路由缓存从不随拓扑变更主动失效，只能等 TTL 过期。
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-kv/kvstore/pkg/metadata"
)

// 默认参数。
const (
	defaultRouteTTL   = 10 * time.Second
	defaultTTLJitter  = 2 * time.Second
	defaultMaxAttempt = 4
	defaultBackoff    = 25 * time.Millisecond
	maxBackoff        = 400 * time.Millisecond
	shardCacheMax     = 4096
)

// ErrKeyNotFound key 不属于任何分片。
var ErrKeyNotFound = errors.New("client: no shard owns key")

// RouteInfo 一条路由记录。
type RouteInfo struct {
	ShardID    int
	LeaderAddr string
	Nodes      []string
	StartKey   string
	EndKey     string
	ExpireAt   time.Time
}

func (r *RouteInfo) expired(now time.Time) bool { return now.After(r.ExpireAt) }

// Config 客户端配置。
type Config struct {
	// RouteTTL 路由缓存有效期。实际值会叠加 [0, TTLJitter) 的随机抖动。
	RouteTTL time.Duration
	// TTLJitter 抖动上限，用于打散失效时刻（BUG-25）。
	TTLJitter time.Duration
	// MaxAttempts 单次操作最多尝试的后端数。
	MaxAttempts int
	// MaxKeyLen 允许的 key 长度。
	MaxKeyLen int
	// DialTimeout 连接超时。
	DialTimeout time.Duration
	// RequestTimeout 单次 HTTP 请求超时。
	RequestTimeout time.Duration
	// EnableCache 为 false 时完全禁用路由缓存（用于对照实验）。
	EnableCache bool
}

func (c Config) withDefaults() Config {
	if c.RouteTTL == 0 {
		c.RouteTTL = defaultRouteTTL
	}
	if c.TTLJitter == 0 {
		c.TTLJitter = defaultTTLJitter
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultMaxAttempt
	}
	if c.MaxKeyLen == 0 {
		c.MaxKeyLen = 1024
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 500 * time.Millisecond
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	return c
}

// Client SDK 客户端。
type Client struct {
	cfg  Config
	meta metadata.Topology
	http *http.Client

	mu sync.RWMutex
	// byShard 以分片为键缓存区间信息（BUG-23）。
	byShard map[int]*RouteInfo

	// inflight 实现 per-shard singleflight（BUG-24）。
	inflightMu sync.Mutex
	inflight   map[int]*call

	stats Stats
}

type call struct {
	done chan struct{}
	info *RouteInfo
	err  error
}

// Stats 客户端计数器。
type Stats struct {
	Gets        atomic.Uint64
	Sets        atomic.Uint64
	Deletes     atomic.Uint64
	CacheHits   atomic.Uint64
	CacheMisses atomic.Uint64
	Retries     atomic.Uint64
	Redirects   atomic.Uint64
	Failures    atomic.Uint64
	MetaLookups atomic.Uint64
}

// StatsSnapshot 可序列化的统计快照。
type StatsSnapshot struct {
	Gets, Sets, Deletes          uint64
	CacheHits, CacheMisses       uint64
	Retries, Redirects, Failures uint64
	MetaLookups                  uint64
}

// NewClient 创建客户端。
func NewClient(endpoints []string) (*Client, error) {
	return NewClientWithConfig(endpoints, Config{})
}

// NewClientWithConfig 创建客户端（带配置）。
func NewClientWithConfig(endpoints []string, cfg Config) (*Client, error) {
	meta, err := metadata.NewClusterTopology(endpoints)
	if err != nil {
		return nil, err
	}
	return NewClientWithTopology(meta, cfg), nil
}

// NewClientWithTopology 用任意拓扑实现创建客户端（进程内实验用）。
func NewClientWithTopology(meta metadata.Topology, cfg Config) *Client {
	cfg = cfg.withDefaults()

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}

	c := &Client{
		cfg:      cfg,
		meta:     meta,
		http:     &http.Client{Transport: transport, Timeout: cfg.RequestTimeout},
		byShard:  make(map[int]*RouteInfo),
		inflight: make(map[int]*call),
	}

	// BUG-26：拓扑变更时主动失效受影响分片的缓存。
	meta.OnShardChange(func(ch metadata.ShardChange) {
		c.mu.Lock()
		delete(c.byShard, ch.ID)
		c.mu.Unlock()
	})

	return c
}

// Stats 返回统计快照。
func (c *Client) Stats() StatsSnapshot {
	return StatsSnapshot{
		Gets: c.stats.Gets.Load(), Sets: c.stats.Sets.Load(), Deletes: c.stats.Deletes.Load(),
		CacheHits: c.stats.CacheHits.Load(), CacheMisses: c.stats.CacheMisses.Load(),
		Retries: c.stats.Retries.Load(), Redirects: c.stats.Redirects.Load(),
		Failures: c.stats.Failures.Load(), MetaLookups: c.stats.MetaLookups.Load(),
	}
}

// jitter 返回本次 TTL。
func (c *Client) jitterTTL() time.Duration {
	if c.cfg.TTLJitter <= 0 {
		return c.cfg.RouteTTL
	}
	// 使用纳秒时间低位作为廉价抖动源，避免为每个条目分配 rand。
	n := time.Now().UnixNano()
	j := time.Duration(n % int64(c.cfg.TTLJitter))
	return c.cfg.RouteTTL + j
}

// getRoute 取 key 的路由，命中缓存则直接返回。
func (c *Client) getRoute(key string) (*RouteInfo, error) {
	if !c.cfg.EnableCache {
		return c.lookupMeta(key)
	}

	shard := c.meta.GetShardForKey(key)
	if shard == nil {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}

	now := time.Now()
	c.mu.RLock()
	ri, ok := c.byShard[shard.ID]
	c.mu.RUnlock()
	if ok && !ri.expired(now) {
		c.stats.CacheHits.Add(1)
		return ri, nil
	}
	c.stats.CacheMisses.Add(1)

	// singleflight：同一分片只允许一个 goroutine 去查元数据（BUG-24）。
	c.inflightMu.Lock()
	if cl, running := c.inflight[shard.ID]; running {
		c.inflightMu.Unlock()
		<-cl.done
		if cl.err != nil {
			return nil, cl.err
		}
		return cl.info, nil
	}
	cl := &call{done: make(chan struct{})}
	c.inflight[shard.ID] = cl
	c.inflightMu.Unlock()

	ri, err := c.lookupMeta(key)

	c.inflightMu.Lock()
	delete(c.inflight, shard.ID)
	c.inflightMu.Unlock()

	cl.info, cl.err = ri, err
	close(cl.done)

	return ri, err
}

func (c *Client) lookupMeta(key string) (*RouteInfo, error) {
	c.stats.MetaLookups.Add(1)
	shard := c.meta.GetShardForKey(key)
	if shard == nil {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}
	ri := &RouteInfo{
		ShardID:    shard.ID,
		LeaderAddr: shard.Leader,
		Nodes:      append([]string(nil), shard.Nodes...),
		StartKey:   shard.StartKey,
		EndKey:     shard.EndKey,
		ExpireAt:   time.Now().Add(c.jitterTTL()),
	}

	c.mu.Lock()
	if len(c.byShard) >= shardCacheMax {
		// 简单容量保护：清空比无限增长安全。
		c.byShard = make(map[int]*RouteInfo, shardCacheMax)
	}
	c.byShard[shard.ID] = ri
	c.mu.Unlock()
	return ri, nil
}

func (c *Client) invalidateShard(shardID int) {
	c.mu.Lock()
	delete(c.byShard, shardID)
	c.mu.Unlock()
}

// candidates 返回该分片可尝试的后端列表：已知 Leader 优先，其余成员随后。
func (ri *RouteInfo) candidates() []string {
	out := make([]string, 0, len(ri.Nodes)+1)
	if ri.LeaderAddr != "" {
		out = append(out, ri.LeaderAddr)
	}
	for _, n := range ri.Nodes {
		if n != "" && n != ri.LeaderAddr {
			out = append(out, n)
		}
	}
	if len(out) == 0 && ri.LeaderAddr != "" {
		out = append(out, ri.LeaderAddr)
	}
	return out
}

func (c *Client) validateKey(key string) error {
	if key == "" {
		return errors.New("client: empty key")
	}
	if len(key) > c.cfg.MaxKeyLen {
		return fmt.Errorf("client: key too long (%d > %d)", len(key), c.cfg.MaxKeyLen)
	}
	return nil
}

// keyURL 构造带正确转义的请求 URL（BUG-22）。
func keyURL(addr, key string) string {
	host := addr
	if !strings.Contains(host, "://") {
		host = "http://" + host
	}
	return strings.TrimRight(host, "/") + "/api/v1/key/" + url.PathEscape(key)
}

// do 执行一次带重定向重试的操作（BUG-21：有界循环，不再递归）。
func (c *Client) do(ctx context.Context, method, key string, body []byte) ([]byte, error) {
	if err := c.validateKey(key); err != nil {
		return nil, err
	}

	backoff := defaultBackoff
	var lastErr error

	for attempt := 0; attempt < c.cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ri, err := c.getRoute(key)
		if err != nil {
			return nil, err
		}

		cands := ri.candidates()
		var target string
		if attempt < len(cands) {
			target = cands[attempt]
		} else if len(cands) > 0 {
			target = cands[len(cands)-1]
		}
		if target == "" {
			return nil, fmt.Errorf("client: shard %d has no known members", ri.ShardID)
		}

		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, keyURL(target, key), rdr)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = int64(len(body))
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			c.stats.Retries.Add(1)
			c.invalidateShard(ri.ShardID)
			sleepCtx(ctx, backoff)
			backoff = minDur(backoff*2, maxBackoff)
			continue
		}

		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			c.stats.Retries.Add(1)
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			return data, nil
		case resp.StatusCode == http.StatusNotFound:
			lastErr = fmt.Errorf("client: key not found: %s", key)
			return nil, lastErr
		case resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusBadGateway:
			// Leader 变更：失效缓存并重试。
			c.stats.Redirects.Add(1)
			c.stats.Retries.Add(1)
			c.invalidateShard(ri.ShardID)
			if hint := resp.Header.Get("X-Raft-Leader"); hint != "" {
				c.mu.Lock()
				if cur, ok := c.byShard[ri.ShardID]; ok {
					cp := *cur
					cp.LeaderAddr = hint
					cp.ExpireAt = time.Now().Add(c.jitterTTL())
					c.byShard[ri.ShardID] = &cp
				}
				c.mu.Unlock()
			}
			sleepCtx(ctx, backoff)
			backoff = minDur(backoff*2, maxBackoff)
			continue
		default:
			lastErr = fmt.Errorf("client: %s %s: %s: %s", method, key, resp.Status, truncate(data, 256))
			return nil, lastErr
		}
	}

	c.stats.Failures.Add(1)
	if lastErr == nil {
		lastErr = errors.New("client: exhausted attempts")
	}
	return nil, fmt.Errorf("client: %s %q failed after %d attempts: %w", method, key, c.cfg.MaxAttempts, lastErr)
}

// Get 读取一个键。
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	c.stats.Gets.Add(1)
	data, err := c.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return "", err
	}
	var out struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		// 允许后端直接返回裸值。
		return strings.TrimSpace(string(data)), nil
	}
	if !out.Found && out.Value == "" {
		return "", fmt.Errorf("client: key not found: %s", key)
	}
	return out.Value, nil
}

// Set 写入一个键。
func (c *Client) Set(ctx context.Context, key, value string) error {
	c.stats.Sets.Add(1)
	payload, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPut, key, payload)
	return err
}

// Delete 删除一个键。
func (c *Client) Delete(ctx context.Context, key string) error {
	c.stats.Deletes.Add(1)
	_, err := c.do(ctx, http.MethodDelete, key, nil)
	return err
}

// Close 关闭客户端。若底层拓扑实现了 io.Closer 则一并关闭。
func (c *Client) Close() error {
	if cl, ok := c.meta.(interface{ Close() error }); ok {
		return cl.Close()
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
