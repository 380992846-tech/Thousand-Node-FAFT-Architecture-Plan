// cmd/kvstore-client/main.go
//
// 客户端 CLI。
//
// 用法：
//
//	kvstore-client --metadata http://127.0.0.1:2379 set <key> <value>
//	kvstore-client --metadata http://127.0.0.1:2379 get <key>
//	kvstore-client --metadata http://127.0.0.1:2379 del <key>
//	kvstore-client --metadata http://127.0.0.1:2379 stats
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/distributed-kv/kvstore/pkg/client"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "用法: kvstore-client [--metadata e1,e2,...] <set|get|del|stats> <key> [value]\n")
	}
	metadataEP := flag.String("metadata", "http://127.0.0.1:2379", "etcd 端点（逗号分隔）")
	timeout := flag.Duration("timeout", 10*time.Second, "整体超时")
	noCache := flag.Bool("no-cache", false, "禁用路由缓存（用于对照测试）")
	flag.Parse()

	c, err := client.NewClientWithConfig(splitTrim(*metadataEP), client.Config{
		EnableCache: !*noCache,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建客户端失败: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	op := args[0]

	if op == "stats" {
		s := c.Stats()
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(s)
		return
	}

	if len(args) < 2 {
		flag.Usage()
		os.Exit(2)
	}
	key := args[1]

	switch op {
	case "set":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "set 需要 value 参数")
			os.Exit(2)
		}
		if err := c.Set(ctx, key, args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "set 失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("ok")
	case "get":
		val, err := c.Get(ctx, key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "get 失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(val)
	case "del":
		if err := c.Delete(ctx, key); err != nil {
			fmt.Fprintf(os.Stderr, "del 失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("ok")
	default:
		fmt.Fprintf(os.Stderr, "未知操作: %s\n", op)
		os.Exit(2)
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
