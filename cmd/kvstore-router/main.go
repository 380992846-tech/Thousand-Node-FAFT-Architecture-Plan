// cmd/kvstore-router/main.go
//
// 路由层进程（无状态网关）：按 key 把请求转发到对应分片的 Leader。
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/distributed-kv/kvstore/pkg/metadata"
	"github.com/distributed-kv/kvstore/pkg/router"
	"go.uber.org/zap"
)

func main() {
	listen := flag.String("listen", envOr("LISTEN_ADDR", ":8080"), "路由层监听地址")
	endpoints := flag.String("metadata", envOr("METADATA_ENDPOINTS", "http://127.0.0.1:2379"),
		"etcd 端点（逗号分隔）")
	maxAttempts := flag.Int("max-attempts", 3, "单请求最多尝试的后端数")
	verbose := flag.Bool("v", false, "输出调试日志")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	eps := splitTrim(*endpoints)
	if len(eps) == 0 {
		logger.Error("no metadata endpoints configured")
		os.Exit(2)
	}

	meta, err := metadata.NewClusterTopology(eps)
	if err != nil {
		logger.Error("connect metadata failed", "err", err, "endpoints", eps)
		os.Exit(1)
	}
	defer meta.Close()

	r := router.NewRouterWithConfig(meta, router.Config{
		MaxAttempts: *maxAttempts,
		Logger:      buildZapLogger(*verbose),
	})

	srv := r.NewHTTPServer(*listen)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen failed", "err", err, "addr", *listen)
		os.Exit(1)
	}

	logger.Info("router listening", "addr", ln.Addr().String(), "metadata", eps, "shards", meta.ShardCount())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("router server error", "err", err)
			os.Exit(1)
		}
	case <-ch:
		logger.Info("shutting down router")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func buildZapLogger(verbose bool) *zap.Logger {
	lvl := zap.InfoLevel
	if verbose {
		lvl = zap.DebugLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.OutputPaths = []string{"stderr"}
	l, err := cfg.Build()
	if err != nil {
		return zap.NewNop()
	}
	return l
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
