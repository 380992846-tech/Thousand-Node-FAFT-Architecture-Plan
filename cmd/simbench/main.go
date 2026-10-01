// cmd/simbench：离散事件模拟器的规模外推与校准。
//
// ⚠️ 定位（决定了结果能不能写进论文）：
//
//	本工具建模的是**成本结构**（消息数 / fsync / 字节 / 可用性），
//	不是性能预测。它不建模 CPU 调度、页面缓存、TCP 拥塞、锁竞争、GC。
//
//	因此它的输出只能回答"趋势与量级"，不能回答"能跑多少 QPS"。
//	所有输出都随附 Caveats，不得省略。
//
// 用法：
//
//	simbench scale       # 分片数扫描到 1000 节点（RQ3）
//	simbench calib       # 与内置的实测样本对照，给出误差与偏离点
//	simbench batch       # 批大小对写吞吐上限的影响
//	simbench binding     # 绑定约束随规模如何转移
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/distributed-kv/kvstore/pkg/sim"
)

// measuredSamples 本项目的实测样本。
//
// 全部来自 cmd/kvbench 在本机的真实运行（2 分片 × 3 副本，单机多进程）。
// 这些是**能实测的规模**，模拟器必须在这一区间上与它们对齐。
//
// 数值来源见 docs/ROADMAP.md 与各次提交的 commit message。
var measuredSamples = []sim.Measured{
	{
		Label: "2x3-readheavy", Shards: 2, Replicas: 3,
		ReadRatio: 0.9,
		Throughput: 2014, LatencyP50Ms: 4.09, LatencyP99Ms: 33.40,
		BatchSize: 1,
		Note:      "读占比 0.9，32 并发，批处理关闭",
	},
	{
		Label: "2x3-writeheavy", Shards: 2, Replicas: 3,
		ReadRatio: 0.1,
		Throughput: 2105, LatencyP50Ms: 17.72, LatencyP99Ms: 22.92,
		BatchSize: 128,
		Note:      "读占比 0.1，32 并发，批处理开启（摊薄 9.9 命令/fsync）",
	},
}

func main() {
	mode := "scale"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	base := sim.Config{
		Shards: 2, Replicas: 3, Domains: 3,
		LogBytes: 256, FsyncLatencyUs: 5400, BatchSize: 128,
		LeaderNICGbps: 1, HeartbeatIntervalMs: 100, CrossDomainRTTMs: 0.1,
		ReplicaFailProb: 1e-4, ReadRatio: 0.5,
	}

	switch mode {
	case "scale":
		fmt.Println("=== 规模外推：分片数 → 1000 节点（RQ3）===")
		fmt.Println()
		fmt.Println("⚠️ 成本结构模型，不是性能预测。估计值 = 上限 × 实测拟合的开销因子。")
		fmt.Println()

		// 先从实测拟合开销因子，再应用到规模扫描上。
		rep := sim.Calibrate(measuredSamples, base, 3.0)
		f := sim.FitOverheadFactor(rep)
		fmt.Print(sim.FormatFittedFactor(f))
		fmt.Println()

		rows := sim.Scale(base, []int{1, 2, 4, 16, 64, 200, 334})
		rows = sim.ApplyFactor(rows, f)
		fmt.Print(sim.FormatScale(rows))
		fmt.Println()
		printCaveats()

	case "calib":
		fmt.Println("=== 模拟器校准：与实测对照 ===")
		fmt.Println()
		rep := sim.Calibrate(measuredSamples, base, 3.0)
		fmt.Print(sim.FormatCalibration(rep))

		// 同时输出 JSON 便于写入结果文件。
		if len(os.Args) > 2 && os.Args[2] == "-json" {
			b, _ := json.MarshalIndent(rep, "", "  ")
			fmt.Println(string(b))
		}

	case "batch":
		fmt.Println("=== 批大小对**写**吞吐上限的影响（单分片 leader）===")
		fmt.Println()
		fmt.Printf("%-10s %-18s %-18s %-16s %s\n",
			"批大小", "出口带宽上限", "磁盘fsync上限", "有效写上限", "绑定约束")
		fmt.Println("----------------------------------------------------------------------")
		for _, b := range []int{1, 8, 32, 128, 512, 2048} {
			c := base
			c.BatchSize = b
			r := sim.Run(c)
			fmt.Printf("%-10d %-18.0f %-18.0f %-16.0f %s\n",
				b, r.PerLeaderEgressCeiling, r.PerLeaderDiskCeiling,
				r.PerLeaderWriteCeiling, r.BindingConstraint)
		}
		fmt.Println()
		fmt.Println("说明：出口带宽上限与批大小无关（∝ 1/(n-1)，见 Mencius OSDI'08 §7），")
		fmt.Println("      磁盘上限 ∝ 批大小。批足够大时绑定约束从磁盘转向带宽。")
		fmt.Println("      注意本表是**写路径**；读路径不落盘，另有独立的读上限。")

	case "binding":
		fmt.Println("=== 绑定约束随副本数的转移（批大小 128）===")
		fmt.Println()
		fmt.Printf("%-8s %-18s %-18s %-16s %s\n",
			"副本数", "出口带宽上限", "磁盘fsync上限", "有效写上限", "绑定约束")
		fmt.Println("----------------------------------------------------------------------")
		for _, n := range []int{3, 5, 7, 11, 21, 101, 1001} {
			c := base
			c.Replicas = n
			c.Shards = 1
			r := sim.Run(c)
			fmt.Printf("%-8d %-18.0f %-18.0f %-16.0f %s\n",
				n, r.PerLeaderEgressCeiling, r.PerLeaderDiskCeiling,
				r.PerLeaderWriteCeiling, r.BindingConstraint)
		}
		fmt.Println()
		fmt.Println("说明：n 增大时出口带宽上限 ∝ 1/(n-1) 下降，磁盘上限不变；")
		fmt.Println("      因此大规模下绑定约束必然转向 leader 出口带宽。")

	default:
		fmt.Fprintf(os.Stderr, "未知模式 %q，可用: scale calib batch binding\n", mode)
		os.Exit(2)
	}
}

func printCaveats() {
	fmt.Println("必须随结果披露的 Caveats：")
	for _, c := range sim.Run(sim.Config{}).Caveats {
		fmt.Println("  - " + c)
	}
}
