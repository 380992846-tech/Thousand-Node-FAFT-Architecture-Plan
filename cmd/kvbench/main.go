// cmd/kvbench/main.go
//
// kvbench：一体化实验驱动。
//
// 它把"起集群 → 起路由 → 打负载 → 出结果"收敛成一个进程，目的是让实验可复现：
// 同一条命令 + 同一个种子 => 同一份结果文件。原始项目的 scripts/*.sh 依赖
// 1000 个 OS 进程、1000 个端口和 sleep，无法复现也无法归因。
//
// 用法：
//
//	# 单机扫描：分片数 × 副本数
//	kvbench -spec 4x3 -duration 10s -concurrency 64 -mode closed
//	kvbench -spec 16x5 -duration 10s -concurrency 256 -mode open -rate 20000
//
//	# 故障注入：测量任意一个副本被硬停后的可用性与恢复时间
//	kvbench -spec 8x5 -fault -fault-at 5s
//
//	# 只做分析模型，不起集群
//	kvbench -model -shards 200 -replicas 5 -payload 256
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/distributed-kv/kvstore/pkg/bench"
	"github.com/distributed-kv/kvstore/pkg/cluster"
	"github.com/distributed-kv/kvstore/pkg/model"
	"github.com/distributed-kv/kvstore/pkg/router"
	"go.uber.org/zap"
)

func main() {
	var (
		spec        = flag.String("spec", "4x3", "分片规格 SHARDSxREPLICAS，例如 200x5")
		learners    = flag.Int("learners", 0, "每分片额外的 learner（非投票副本）数")
		dir         = flag.String("dir", "", "Raft 数据目录（默认临时目录）")
		baseRaft    = flag.Int("raft-port", 18000, "起始 Raft 端口")
		baseHTTP    = flag.Int("http-port", 28000, "起始数据面端口")
		routerAddr  = flag.String("router", "127.0.0.1:38080", "路由层监听地址")
		directNodes = flag.Bool("direct", false, "绕过路由层直接打节点（用于归因对比）")

		duration    = flag.Duration("duration", 10*time.Second, "测量时长")
		warmup      = flag.Duration("warmup", 3*time.Second, "预热时长")
		concurrency = flag.Int("concurrency", 64, "并发/worker 数")
		mode        = flag.String("mode", "closed", "发压模式 closed|open")
		rate        = flag.Int("rate", 0, "open 模式目标速率 ops/s（0=尽力而为）")
		ops         = flag.Int("ops", 0, "测量阶段目标操作数（0=只按时长）")

		readRatio = flag.Float64("read-ratio", 0.5, "读操作占比")
		keys      = flag.Int("keys", 100000, "键空间大小")
		valueSize = flag.Int("value-size", 128, "值长度（字节）")
		dist      = flag.String("dist", "zipf", "访问分布 uniform|zipf|latest|sequential")
		zipfTheta = flag.Float64("zipf-theta", 0.99, "Zipf 指数")

		seed    = flag.Int64("seed", 42, "随机种子")
		label   = flag.String("label", "", "本次运行标签")
		notes   = flag.String("notes", "", "备注（例如 \"fsync 关闭\"）")
		outJSON = flag.String("out-json", "", "结果 JSON 输出路径")
		outCSV  = flag.String("out-csv", "", "结果 CSV 输出路径（追加表头+一行）")
		sample  = flag.Duration("sample-interval", 0, "时间序列采样间隔（0=关闭）")

		fault     = flag.Bool("fault", false, "注入故障：硬停一个副本")
		faultAt   = flag.Duration("fault-at", 5*time.Second, "故障注入时刻（相对测量开始）")
		faultShard = flag.Int("fault-shard", 0, "在哪个分片注入故障")

		modelOnly = flag.Bool("model", false, "只运行分析模型，不起集群")
		payload   = flag.Int("payload", 256, "分析模型的负载字节数")
		nicGbps   = flag.Float64("nic-gbps", 1.0, "分析模型的 leader 网卡速率（Gb/s）")
		fsyncUs   = flag.Float64("fsync-us", 100, "分析模型的 fsync 延迟（微秒）")
		batch     = flag.Int("batch", 64, "分析模型的批大小")
		wanRTT    = flag.Float64("wan-rtt-ms", 0, "分析模型的跨域 RTT（ms，0=局域网）")

		verbose = flag.Bool("v", false, "输出调试日志")
	)
	flag.Parse()

	if *modelOnly {
		runModel(*spec, *payload, *nicGbps, *fsyncUs, *batch, *wanRTT)
		return
	}

	logger := zap.NewNop()
	if *verbose {
		cfg := zap.NewDevelopmentConfig()
		logger, _ = cfg.Build()
	}

	shards, replicas, err := cluster.ParseShardSpec(*spec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误：%v\n", err)
		os.Exit(2)
	}

	workDir := *dir
	if workDir == "" {
		workDir, err = os.MkdirTemp("", "kvbench-")
		if err != nil {
			fmt.Fprintf(os.Stderr, "无法创建临时目录: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(workDir)
	}

	totalNodes := shards * (replicas + *learners)
	fmt.Printf("=== kvbench: %d 分片 × (%d 投票 + %d learner) = %d 副本 ===\n",
		shards, replicas, *learners, totalNodes)
	fmt.Printf("数据目录: %s\n", workDir)

	start := time.Now()
	cl, err := cluster.New(cluster.Config{
		Shards:        shards,
		Replicas:      replicas,
		Learners:      *learners,
		BaseRaftPort:  *baseRaft,
		BaseHTTPPort:  *baseHTTP,
		Dir:           workDir,
		CleanDir:      true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动集群失败: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()
	fmt.Printf("集群就绪，用时 %s\n", time.Since(start).Round(time.Millisecond))

	if err := cl.WaitForLeaders(20 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "等待 Leader 失败: %v\n", err)
		os.Exit(1)
	}

	// 2. 路由层。
	rt := router.NewRouterWithConfig(cl.Topology(), router.Config{Logger: logger})
	var srv *routerHandle
	if !*directNodes {
		srv, err = startRouter(rt, *routerAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "启动路由层失败: %v\n", err)
			os.Exit(1)
		}
		defer srv.Close()
		fmt.Printf("路由层监听 %s（分片数 %d）\n", *routerAddr, cl.Topology().ShardCount())
	}

	// 3. 目标地址。
	var endpoints []string
	if *directNodes {
		// 直连模式：分别打各分片 Leader，避免单点成为瓶颈。
		for _, addr := range cl.Leaders() {
			endpoints = append(endpoints, addr)
		}
		fmt.Printf("直连 %d 个分片 Leader（绕过路由层）\n", len(endpoints))
	} else {
		endpoints = []string{*routerAddr}
	}

	if *label == "" {
		*label = fmt.Sprintf("%dx%d", shards, replicas)
		if *learners > 0 {
			*label += fmt.Sprintf("L%d", *learners)
		}
	}

	cfg := bench.Config{
		Shards: shards, Replicas: replicas, Learners: *learners,
		Workload: bench.Workload{
			ReadRatio: *readRatio, Keys: *keys, ValueSize: *valueSize,
			Dist: bench.Distribution(*dist), ZipfTheta: *zipfTheta,
		},
		Mode:           bench.Mode(*mode),
		Concurrency:    *concurrency,
		TargetRate:     *rate,
		Duration:       *duration,
		Ops:            *ops,
		Warmup:         *warmup,
		Endpoints:      endpoints,
		Seed:           *seed,
		Label:          *label,
		Notes:          *notes,
		SampleInterval: *sample,
	}

	ctx, cancel := signalContext()
	defer cancel()

	runner := bench.NewRunner(cfg)
	defer runner.Close()

	// 4. 可选：故障注入。
	var faultDone chan *faultRecord
	if *fault {
		faultDone = make(chan *faultRecord, 1)
		go func() {
			select {
			case <-time.After(*warmup + *faultAt):
			case <-ctx.Done():
				return
			}
			rec := injectFault(cl, *faultShard)
			faultDone <- rec
		}()
	}

	fmt.Printf("开始测量：mode=%s concurrency=%d duration=%s warmup=%s\n",
		*mode, *concurrency, *duration, *warmup)
	res, err := runner.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "实验失败: %v\n", err)
		os.Exit(1)
	}

	if faultDone != nil {
		select {
		case rec := <-faultDone:
			if rec != nil {
				res.Notes = strings.TrimSpace(res.Notes + " | " + rec.String())
				fmt.Printf("故障注入: %s\n", rec.String())
			}
		case <-time.After(time.Second):
		}
	}

	// 5. 输出。
	fmt.Println()
	fmt.Println(res.SummaryLine())
	fmt.Printf("  总操作 %d，错误 %d（%.4f%%）\n", res.Ops, res.Errors, res.ErrorRate*100)
	fmt.Printf("  put: p50=%.2fms p99=%.2fms | get: p50=%.2fms p99=%.2fms\n",
		res.LatencyByOp["put"].P50Ms, res.LatencyByOp["put"].P99Ms,
		res.LatencyByOp["get"].P50Ms, res.LatencyByOp["get"].P99Ms)

	if *outJSON != "" {
		f, err := os.Create(*outJSON)
		if err != nil {
			fmt.Fprintf(os.Stderr, "写 JSON 失败: %v\n", err)
		} else {
			if err := res.WriteJSON(f); err != nil {
				fmt.Fprintf(os.Stderr, "写 JSON 失败: %v\n", err)
			}
			_ = f.Close()
			fmt.Printf("结果已写入 %s\n", *outJSON)
		}
	}
	if *outCSV != "" {
		needHeader := true
		if st, err := os.Stat(*outCSV); err == nil && st.Size() > 0 {
			needHeader = false
		}
		f, err := os.OpenFile(*outCSV, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "写 CSV 失败: %v\n", err)
		} else {
			if needHeader {
				if err := res.WriteCSV(f); err != nil {
					fmt.Fprintf(os.Stderr, "写 CSV 失败: %v\n", err)
				}
			} else {
				// 已有表头，只追加数据行。
				if err := appendCSVRow(f, res); err != nil {
					fmt.Fprintf(os.Stderr, "写 CSV 失败: %v\n", err)
				}
			}
			_ = f.Close()
			fmt.Printf("结果已追加到 %s\n", *outCSV)
		}
	}

	// 6. 安全性自检。
	if dups := cl.DuplicateLeaders(); len(dups) > 0 {
		fmt.Fprintf(os.Stderr, "!!! 安全性违规：同一任期出现多个 Leader: %v\n", dups)
		os.Exit(3)
	}
}

type faultRecord struct {
	Shard   int
	Node    string
	At      time.Time
	WasLeader bool
	Recovered time.Duration
	Err     error
}

func (f *faultRecord) String() string {
	if f == nil {
		return ""
	}
	s := fmt.Sprintf("fault: killed %s (shard %d, was_leader=%v)", f.Node, f.Shard, f.WasLeader)
	if f.Recovered > 0 {
		s += fmt.Sprintf(", new leader elected in %s", f.Recovered.Round(time.Millisecond))
	}
	if f.Err != nil {
		s += fmt.Sprintf(", err=%v", f.Err)
	}
	return s
}

// injectFault 硬停一个副本并从拓扑中移除其端点，测量重新选主时间。
func injectFault(cl *cluster.Cluster, shard int) *faultRecord {
	members := cl.Members(shard)
	if len(members) == 0 {
		return &faultRecord{Shard: shard, Err: fmt.Errorf("no members in shard %d", shard)}
	}
	// 优先打掉 Leader，这才是值得测的场景。
	victim := members[0]
	for _, m := range members {
		if m.Store.IsLeader() {
			victim = m
			break
		}
	}
	rec := &faultRecord{
		Shard: shard, Node: victim.NodeID, At: time.Now(),
		WasLeader: victim.Store.IsLeader(),
	}
	if err := cl.StopMember(victim.NodeID); err != nil {
		rec.Err = err
		return rec
	}

	// 等待该分片出现新的 Leader。
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m.NodeID != victim.NodeID && m.Store.IsLeader() {
				rec.Recovered = time.Since(rec.At)
				cl.RefreshTopology()
				return rec
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec.Err = fmt.Errorf("no new leader within 15s")
	return rec
}

func runModel(spec string, payload int, nicGbps, fsyncUs float64, batch int, wanRTT float64) {
	shards, replicas, err := cluster.ParseShardSpec(spec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误：%v\n", err)
		os.Exit(2)
	}
	_ = shards

	m := model.Config{
		Replicas:        replicas,
		PayloadBytes:    payload,
		LeaderNICGbps:   nicGbps,
		FsyncLatencyUs:  fsyncUs,
		BatchSize:       batch,
		CrossZoneRTTMs:  wanRTT,
	}
	rep := model.Analyze(m)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(rep)
}

type routerHandle struct {
	srv  *http.Server
	ln   net.Listener
	rt   *router.Router
	addr string
}

// startRouter 在指定地址启动路由层。
func startRouter(rt *router.Router, addr string) (*routerHandle, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := rt.NewHTTPServer(addr)
	go func() { _ = srv.Serve(ln) }()
	return &routerHandle{srv: srv, ln: ln, rt: rt, addr: ln.Addr().String()}, nil
}

func (h *routerHandle) Close() {
	if h == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = h.srv.Shutdown(ctx)
	_ = h.ln.Close()
}

// appendCSVRow 向已存在的 CSV 追加一行数据（不写表头）。
func appendCSVRow(f *os.File, res *bench.Result) error {
	var sb strings.Builder
	var tmp bytes.Buffer
	if err := res.WriteCSV(&tmp); err != nil {
		return err
	}
	// WriteCSV 输出"表头 + 数据行"，这里丢掉第一行。
	lines := strings.SplitN(tmp.String(), "\n", 2)
	if len(lines) == 2 {
		sb.WriteString(lines[1])
	}
	_, err := f.WriteString(sb.String())
	return err
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}
