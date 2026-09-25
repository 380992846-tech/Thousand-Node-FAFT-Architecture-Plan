// cmd/kvstore-node/main.go
//
// kvstore-node 数据节点进程：一个进程承载一个分片中的一个 Raft 副本。
//
// 用法示例：
//
//	# 分片 0 的首节点（引导）
//	kvstore-node --node-id node-0-0 --shard-id 0 \
//	    --raft-addr 127.0.0.1:8000 --http-addr 127.0.0.1:18000 \
//	    --raft-dir /data/node-0-0 \
//	    --metadata http://localhost:2379 --bootstrap
//
//	# 其余副本（向分片 Leader 的数据面 HTTP 地址入组）
//	kvstore-node --node-id node-0-1 --shard-id 0 \
//	    --raft-addr 127.0.0.1:8001 --http-addr 127.0.0.1:18001 \
//	    --raft-dir /data/node-0-1 \
//	    --metadata http://localhost:2379 --join http://127.0.0.1:18000
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/distributed-kv/kvstore/pkg/metadata"
	"github.com/distributed-kv/kvstore/pkg/nodeapi"
	"github.com/distributed-kv/kvstore/pkg/raft"
)

func main() {
	var (
		nodeID      = flag.String("node-id", "", "本节点 ID（集群内唯一）")
		shardID     = flag.Int("shard-id", 0, "所属分片 ID")
		raftAddr    = flag.String("raft-addr", "", "Raft 监听地址 host:port")
		httpAddr    = flag.String("http-addr", "", "数据面 HTTP 监听地址 host:port")
		metricsAddr = flag.String("metrics-addr", "", "Prometheus 指标监听地址（默认关闭）")
		raftDir     = flag.String("raft-dir", "", "Raft 数据目录")
		advertise   = flag.String("advertise-addr", "", "对外公告的 Raft 地址（默认等于 --raft-addr）")
		metadataEP  = flag.String("metadata", "", "etcd 端点（逗号分隔，留空则不注册拓扑）")
		bootstrap   = flag.Bool("bootstrap", false, "作为分片首节点引导集群（幂等）")
		joinHTTP    = flag.String("join", "", "分片 Leader 的数据面 HTTP 地址，用于入组")
		nonvoter    = flag.Bool("nonvoter", false, "以 learner 身份加入（不参与投票）")

		heartbeat = flag.Duration("heartbeat-timeout", 100*time.Millisecond, "Raft 心跳超时")
		election  = flag.Duration("election-timeout", 500*time.Millisecond, "Raft 选举超时（须 >= heartbeat）")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if *nodeID == "" || *raftAddr == "" || *raftDir == "" {
		fmt.Fprintln(os.Stderr, "错误：--node-id、--raft-addr、--raft-dir 为必填参数")
		flag.Usage()
		os.Exit(2)
	}
	if *election < *heartbeat {
		fmt.Fprintf(os.Stderr, "错误：--election-timeout (%s) 必须 >= --heartbeat-timeout (%s)\n",
			*election, *heartbeat)
		os.Exit(2)
	}
	adv := *advertise
	if adv == "" {
		adv = *raftAddr
	}

	store, err := raft.NewKVStore(raft.Config{
		NodeID:           *nodeID,
		ShardID:          *shardID,
		BindAddr:         *raftAddr,
		RaftDir:          *raftDir,
		HeartbeatTimeout: *heartbeat,
		ElectionTimeout:  *election,
		LogOutput:        os.Stderr,
	})
	if err != nil {
		logger.Error("create raft node failed", "err", err)
		os.Exit(1)
	}

	// 1. 引导（幂等）。
	if *bootstrap {
		res, err := store.Bootstrap()
		if err != nil {
			logger.Error("bootstrap failed", "err", err)
			os.Exit(1)
		}
		switch {
		case res.Bootstrapped:
			logger.Info("bootstrapped shard", "shard", *shardID, "node", *nodeID)
		case res.AlreadyInitialized:
			logger.Info("already bootstrapped, continuing", "shard", *shardID, "voters", res.ExistingVoters)
		}
		if _, err := store.WaitForLeader(10 * time.Second); err != nil {
			logger.Warn("no leader yet after bootstrap", "err", err)
		}
	}

	// 2. 元数据集群（可选）。
	var meta *metadata.ClusterTopology
	if *metadataEP != "" {
		meta, err = metadata.NewClusterTopology(splitTrim(*metadataEP))
		if err != nil {
			logger.Error("connect metadata failed", "err", err)
			os.Exit(1)
		}
		defer meta.Close()
	}

	// 3. 数据面 / 指标。
	srv := nodeapi.New(store, meta, store.Registry(), nodeapi.Config{
		HTTPAddr:      *httpAddr,
		MetricsAddr:   *metricsAddr,
		AdvertiseAddr: adv,
		Logger:        logger,
	})
	if err := srv.ListenAndServe(nodeapi.Config{
		HTTPAddr:      *httpAddr,
		MetricsAddr:   *metricsAddr,
		AdvertiseAddr: adv,
		Logger:        logger,
	}); err != nil {
		logger.Error("start http server failed", "err", err)
		_ = store.Shutdown()
		os.Exit(1)
	}

	// 4. 向 Leader 入组。
	if *joinHTTP != "" {
		if err := waitAndJoin(context.Background(), *joinHTTP, *nodeID, adv, *shardID, *nonvoter, logger); err != nil {
			logger.Error("join failed", "err", err)
			os.Exit(1)
		}
		logger.Info("joined shard", "shard", *shardID, "node", *nodeID, "nonvoter", *nonvoter)
	}

	// 5. 拓扑上报（仅 Leader 上报，避免多写入方互相覆盖）。
	if meta != nil {
		go reportTopology(store, meta, *shardID, adv, logger)
	}

	// 6. 等待退出。
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	logger.Info("shutting down", "node", *nodeID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	if err := store.Shutdown(); err != nil {
		logger.Warn("raft shutdown returned error", "err", err)
	}
}

func waitAndJoin(ctx context.Context, leaderHTTP, nodeID, adv string, shardID int, nonvoter bool, logger *slog.Logger) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = nodeapi.JoinCluster(cctx, leaderHTTP, nodeID, adv, shardID, nonvoter)
		cancel()
		if lastErr == nil {
			return nil
		}
		logger.Warn("join attempt failed, retrying", "err", lastErr)
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("nodeapi: join gave up after 30s: %w", lastErr)
}

// reportTopology 由分片 Leader 周期性上报自己的分片视图。
func reportTopology(store *raft.KVStore, meta *metadata.ClusterTopology, shardID int, adv string, logger *slog.Logger) {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	for range t.C {
		if !store.IsLeader() {
			continue
		}
		cfg := store.Configuration()
		nodes := make([]string, 0, len(cfg))
		for _, s := range cfg {
			nodes = append(nodes, string(s.Address))
		}
		if err := meta.RegisterShard(&metadata.ShardInfo{
			ID:      shardID,
			Nodes:   nodes,
			Leader:  adv,
			Status:  metadata.ShardActive,
			Term:    store.Term(),
		}); err != nil {
			logger.Debug("topology report failed", "err", err)
		}
	}
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
