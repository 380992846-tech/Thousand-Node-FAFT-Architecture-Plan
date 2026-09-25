// cmd/faftbench 计算 FAFT 的收益上界与代价，用于判断方向是否值得投入。
//
// 用法：
//
//	faftbench scale    # 副本数扫描：省多少消息、控制路径容错是否退化
//	faftbench cost     # 副本粒度可用性：弹性 quorum 的真实代价
//	faftbench domain   # 按故障域组织 quorum 能否恢复控制路径可用性
//	faftbench n1000    # n=1000 单点详算
//	faftbench curve    # n=1000 的完整权衡曲线
//	faftbench avail    # 独立失效 vs 相关失效的可用性高估量
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

func main() {
	mode := "scale"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "scale":
		sc := faft.ScaleConfig{Domains: 5, CrossDomainRTTMs: 2.0, DataFaults: 1, ControlFaults: 1}
		fmt.Println("=== FAFT 收益与代价：副本数扫描（5 个故障域，跨域 RTT 2ms）===")
		fmt.Println()
		_, rows := faft.MaxBenefit(sc, []int{3, 5, 7, 9, 11, 21, 51, 101})
		fmt.Print(faft.FormatScaleTable(rows))

	case "cost":
		// 最关键的一张表：把"省消息"与"选举需要多少副本在线"放在一起看。
		fmt.Println("=== 弹性 quorum 的真实代价（副本粒度可用性，单副本失效率 p=1e-4）===")
		fmt.Println()
		const p = 1e-4
		fmt.Printf("%-7s %-6s %-6s %-11s %-18s %-18s %s\n",
			"n", "|Q2|", "|Q1|", "写消息/op", "数据路径可用性", "控制路径可用性", "选举需在线副本")
		fmt.Println(strings.Repeat("-", 108))
		for _, n := range []int{5, 9, 21, 101, 1000} {
			m := n/2 + 1
			base := faft.ComputeControlCost(n, m, p)
			fmt.Printf("%-7d %-6d %-6d %-11d %-18.12f %-18.12f %d  (基线多数 quorum)\n",
				n, base.DataQuorum, base.ControlQuorum, 2*base.DataQuorum,
				base.DataAvailability, base.ControlAvailability, base.ReplicasNeededToElect)

			faftC := faft.ComputeControlCost(n, 2, p)
			fmt.Printf("%-7s %-6d %-6d %-11d %-18.12f %-18.12f %d  (|Q2|=2 极限)\n",
				"", faftC.DataQuorum, faftC.ControlQuorum, 2*faftC.DataQuorum,
				faftC.DataAvailability, faftC.ControlAvailability, faftC.ReplicasNeededToElect)

			q2 := n / 4
			if q2 < 2 {
				q2 = 2
			}
			mid := faft.ComputeControlCost(n, q2, p)
			fmt.Printf("%-7s %-6d %-6d %-11d %-18.12f %-18.12f %d  (|Q2|=n/4 折中)\n",
				"", mid.DataQuorum, mid.ControlQuorum, 2*mid.DataQuorum,
				mid.DataAvailability, mid.ControlAvailability, mid.ReplicasNeededToElect)
			fmt.Println()
		}

	case "domain":
		// 核心验证：quorum 按故障域组织时，控制路径的可用性会怎样？
		//
		// 设定：n=1000 副本摊到 D 个故障域，每域 n/D 个副本。
		// 故障模型：域内失效完全相关（整域一起挂），域间独立，
		// 单域失效率 p_d。一个跨 d 个域的 quorum 只要还有 q 个域在线即可用。
		fmt.Println("=== 按故障域组织 quorum：控制路径可用性能否恢复 ===")
		fmt.Println()
		fmt.Println("模型：1000 副本摊到 D 个域；域内失效完全相关，域间独立；单域失效率 p_d=1e-3")
		fmt.Println()
		const n = 1000
		const pd = 1e-3
		fmt.Printf("%-8s %-12s %-12s %-14s %-22s %s\n",
			"域数 D", "每域副本", "|Q2|(域)", "|Q1|(域)", "控制路径可用性", "选举需在线域数")
		fmt.Println(strings.Repeat("-", 96))
		for _, D := range []int{3, 5, 7, 9, 15} {
			for _, q2d := range []int{2, 3} {
				q1d := D - q2d + 1
				if q1d > D {
					q1d = D
				}
				avail := faft.AvailabilityBound(D, D-q1d, pd)
				fmt.Printf("%-8d %-12d %-12d %-14d %-22.15f %d\n",
					D, n/D, q2d, q1d, avail, q1d)
			}
			fmt.Println()
		}
		fmt.Println("对照：不按域组织（副本粒度，p=1e-4）时，|Q2|=2 需要 999/1000 副本在线，")
		fmt.Println("控制路径可用性降到 0.995325232148 —— 即约 13 倍的可达性劣化。")

	case "n1000":
		sc := faft.ScaleConfig{
			Replicas: 1000, Domains: 5, CrossDomainRTTMs: 2.0,
			DataFaults: 1, ControlFaults: 1,
		}
		row := faft.AnalyzeScale(sc)
		b, _ := json.MarshalIndent(row, "", "  ")
		fmt.Println("=== n=1000, 5 个故障域 ===")
		fmt.Println(string(b))

	case "curve":
		sc := faft.ScaleConfig{Replicas: 1000, Domains: 5, CrossDomainRTTMs: 2.0}
		fmt.Println("=== n=1000 的完整权衡曲线（按写消息数升序）===")
		fmt.Println()
		fmt.Print(faft.FormatTradeoff(faft.TradeoffCurve(sc)))

	case "avail":
		fmt.Println("=== 独立失效 vs 相关失效：可用性被高估了多少 ===")
		fmt.Println()
		fmt.Printf("%-8s %-10s %-18s %-18s %s\n", "域数", "容错度", "独立模型P[可用]", "相关模型P[可用]", "高估")
		for _, d := range []int{3, 5, 7} {
			for _, tol := range []int{1, 2} {
				if tol >= d {
					continue
				}
				ind := faft.AvailabilityBound(d, tol, 0.001)
				cor := faft.CorrelatedAvailability(d, tol, 0.001, 0.0001, 2)
				fmt.Printf("%-8d %-10d %-18.12f %-18.12f %.8f\n", d, tol, ind, cor, ind-cor)
			}
		}
	}
}
