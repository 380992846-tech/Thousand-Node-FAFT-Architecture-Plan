// pkg/sim/calib.go
//
// 模拟器校准：把模拟输出与实测对照，给出误差曲线与**开始偏离的规模点**。
//
// 为什么这一步不能省（docs/DESIGN.md §4.6）：
//
// 模拟器的危险不在于"不准"，而在于"看起来准"。若不校准，
// 1000 节点上的模拟结果会被当作实测结论写进论文，而审稿人无法验证。
//
// 校准要做三件事：
//  1. 在**能实测的规模**上逐点对比模拟与实测；
//  2. 给出误差随规模变化的曲线；
//  3. 明确指出从哪个规模开始模拟不再可信。
//
// 本文件只提供对照的**框架与数据结构**；具体的实测数据由调用方注入
// （来自 cmd/kvbench 的结果文件），因为实测必须由真实运行产生，
// 不能由本包伪造。
package sim

import (
	"fmt"
	"math"
	"sort"
)

// Measured 一条实测样本。
type Measured struct {
	// Label 样本标识（例如 "2x3"）。
	Label string `json:"label"`
	// Shards / Replicas 规模。
	Shards   int `json:"shards"`
	Replicas int `json:"replicas"`
	// ReadRatio 实测的读操作占比。用于选择对照哪个上限：
	// 读密集应与读上限比，写密集应与写上限比。
	// 混用会在任一场景下都得到无意义的比值（本项目实测过：
	// 读密集吞吐达到写路径上限的 5.4 倍，纯属对照对象错误）。
	ReadRatio float64 `json:"read_ratio"`
	// Throughput 实测吞吐（ops/s）。
	Throughput float64 `json:"throughput"`
	// LatencyP50Ms 实测 p50 延迟。
	LatencyP50Ms float64 `json:"latency_p50_ms"`
	// LatencyP99Ms 实测 p99 延迟。
	LatencyP99Ms float64 `json:"latency_p99_ms"`
	// BatchSize 实测时的批大小（0 视为 1）。
	BatchSize int `json:"batch_size"`
	// Note 实测条件说明（例如"单机多进程"）。
	Note string `json:"note"`
}

// Comparison 一条"模拟 vs 实测"的对照。
type Comparison struct {
	Label    string `json:"label"`
	Shards   int    `json:"shards"`
	Replicas int    `json:"replicas"`

	// ComparedAgainst 本次对照的是哪条上限："read" 或 "write"。
	ComparedAgainst string `json:"compared_against"`

	MeasuredThroughput float64 `json:"measured_throughput"`
	SimulatedCeiling   float64 `json:"simulated_ceiling"`
	// ThroughputRatio = 实测 / 模拟上限。恒 ≤ 1 才有意义；
	// 若 > 1 说明对照对象选错或模拟漏算了能力。
	ThroughputRatio float64 `json:"throughput_ratio"`

	MeasuredP50Ms  float64 `json:"measured_p50_ms"`
	SimulatedP50Ms float64 `json:"simulated_p50_ms"`
	// LatencyRatio = 实测 / 模拟。> 1 表示实测比模拟慢。
	LatencyRatio float64 `json:"latency_ratio"`

	// Verdict 对该点的判断。
	Verdict string `json:"verdict"`
}

// CalibrationReport 校准报告。
type CalibrationReport struct {
	Comparisons []Comparison `json:"comparisons"`

	// MaxThroughputRatioErr / MaxLatencyRatioErr 最大偏差倍数。
	MaxThroughputRatioErr float64 `json:"max_throughput_ratio_err"`
	MaxLatencyRatioErr    float64 `json:"max_latency_ratio_err"`

	// DivergencePoint 从哪个规模开始，实测与模拟的偏差超过阈值。
	// 0 表示在已测范围内未观察到明显偏离。
	DivergencePoint int `json:"divergence_point"`

	// Calibrated 是否通过校准（所有点都在阈值内）。
	Calibrated bool `json:"calibrated"`

	// Summary 供结果表与论文使用的一段话。
	Summary string `json:"summary"`
	// Warnings 明确列出模拟器不能外推的地方。
	Warnings []string `json:"warnings"`
}

// Calibrate 把实测样本与模拟对照。
//
// 对照规则（关键）：
//   - 读占比 > 0.5 的样本与**读上限**比；否则与**写上限**比。
//     混用会得到无意义的比值。
//   - 吞吐用"实测 / 模拟上限"。模拟给的是上限，因此该比值应 ≤ 1；
//     越接近 1 说明系统越接近其成本上界。
//   - 延迟用"实测 / 模拟"。> 1 表示实测比模拟慢，差额来自未建模开销。
//
// tolerance 为允许的偏差倍数（例如 3.0 表示允许 3 倍以内）。
func Calibrate(samples []Measured, base Config, tolerance float64) CalibrationReport {
	if tolerance <= 0 {
		tolerance = 3.0
	}
	rep := CalibrationReport{}

	for _, m := range samples {
		c := base
		c.Shards = m.Shards
		c.Replicas = m.Replicas
		c.ReadRatio = m.ReadRatio
		if m.BatchSize > 0 {
			c.BatchSize = m.BatchSize
		}
		r := Run(c)

		cmp := Comparison{
			Label:              m.Label,
			Shards:             m.Shards,
			Replicas:           m.Replicas,
			MeasuredThroughput: m.Throughput,
			MeasuredP50Ms:      m.LatencyP50Ms,
			SimulatedP50Ms:     r.WriteLatencyMs,
		}

		// 按读占比选择对照对象。
		if m.ReadRatio > 0.5 {
			cmp.ComparedAgainst = "read"
			cmp.SimulatedCeiling = r.ClusterReadCeiling
		} else {
			cmp.ComparedAgainst = "write"
			cmp.SimulatedCeiling = r.ClusterWriteCeiling
		}
		if cmp.SimulatedCeiling > 0 {
			cmp.ThroughputRatio = m.Throughput / cmp.SimulatedCeiling
		}
		if r.WriteLatencyMs > 0 {
			cmp.LatencyRatio = m.LatencyP50Ms / r.WriteLatencyMs
		}

		// 判断该点是否在容差内。
		thrOK := cmp.ThroughputRatio <= tolerance
		latOK := cmp.LatencyRatio >= 1.0/tolerance && cmp.LatencyRatio <= tolerance

		switch {
		case thrOK && latOK:
			cmp.Verdict = "在容差内"
		case cmp.ThroughputRatio > tolerance:
			cmp.Verdict = fmt.Sprintf("实测吞吐超过模拟上限 %.1f 倍 —— 对照对象选错或模拟漏算能力",
				cmp.ThroughputRatio)
			markDivergence(&rep, m.Shards)
		case cmp.LatencyRatio > tolerance:
			cmp.Verdict = fmt.Sprintf("实测延迟是模拟的 %.1f 倍 —— 未建模开销占主导",
				cmp.LatencyRatio)
			markDivergence(&rep, m.Shards)
		default:
			cmp.Verdict = "实测快于模拟 —— 模拟偏保守"
		}

		// 记录最大偏差倍数。
		dev := math.Max(cmp.ThroughputRatio, math.Max(cmp.LatencyRatio, 1/cmp.LatencyRatio))
		if dev > rep.MaxThroughputRatioErr {
			rep.MaxThroughputRatioErr = dev
		}
		if cmp.LatencyRatio > rep.MaxLatencyRatioErr {
			rep.MaxLatencyRatioErr = cmp.LatencyRatio
		}

		rep.Comparisons = append(rep.Comparisons, cmp)
	}

	sort.Slice(rep.Comparisons, func(i, j int) bool {
		return rep.Comparisons[i].Shards < rep.Comparisons[j].Shards
	})

	rep.Calibrated = rep.DivergencePoint == 0 && len(rep.Comparisons) > 0

	if len(rep.Comparisons) == 0 {
		rep.Summary = "无实测样本，未执行校准。**此时模拟结果不得写入论文。**"
	} else if rep.Calibrated {
		rep.Summary = fmt.Sprintf(
			"在已测规模（分片 %d–%d）内，模拟与实测偏差均在 %.1f 倍以内。",
			rep.Comparisons[0].Shards, rep.Comparisons[len(rep.Comparisons)-1].Shards, tolerance)
	} else {
		rep.Summary = fmt.Sprintf(
			"**校准未通过**：模拟从分片数 %d 起与实测偏差超过 %.1f 倍。"+
				"该规模以上的模拟结果不得作为绝对性能声明，只能作为趋势参考。",
			rep.DivergencePoint, tolerance)
	}

	// 无论是否通过，都必须附带这些警告。
	rep.Warnings = []string{
		"模拟只建模成本结构（消息数 / fsync / 字节 / 可用性），不建模 CPU、缓存、拥塞、锁竞争、HTTP 栈",
		"已测规模之上无法验证；外推结果只能作为趋势，不能作为绝对性能",
		"单机多进程实测与真实多机部署的差异未被建模（无真实网络分区、共享页面缓存）",
	}
	if rep.DivergencePoint > 0 {
		rep.Warnings = append(rep.Warnings,
			fmt.Sprintf("分片数 ≥ %d 后偏离已实测范围，论文中必须披露", rep.DivergencePoint))
	}
	return rep
}

// FittedFactor 从实测样本拟合出的开销因子。
//
// 存在的理由：模拟给出的是**成本上限**，而实测只到上限的一小部分 ——
// 本机实测中写路径只到上限的 4.4%、读路径到 10.1%。差额来自模型外的
// 固定开销（HTTP 栈、JSON 序列化、Go 运行时、单机多进程争用）。
//
// 若不拟合这个因子，模拟器在 1000 节点上给出的绝对数字会高出实际
// 一到两个数量级，写进论文就是错的。
// 拟合后模拟器预测的是"在同等的模型外开销下"的性能 ——
// 这仍是**外推**，但至少把已知的系统性偏差消掉了。
type FittedFactor struct {
	// Samples 参与拟合的样本数。
	Samples int `json:"samples"`
	// MeanRatio 实测/模拟上限的均值。
	MeanRatio float64 `json:"mean_ratio"`
	// MinRatio / MaxRatio 观测范围，用于给出外推的不确定区间。
	MinRatio float64 `json:"min_ratio"`
	MaxRatio float64 `json:"max_ratio"`
	// Note 使用说明。
	Note string `json:"note"`
}

// FitOverheadFactor 从校准结果拟合开销因子。
//
// 只使用"实测/上限 ≤ 1"的样本（比值 > 1 说明对照对象错或有其他异常）。
func FitOverheadFactor(rep CalibrationReport) FittedFactor {
	f := FittedFactor{MinRatio: math.Inf(1)}
	var sum float64
	for _, c := range rep.Comparisons {
		r := c.ThroughputRatio
		if r <= 0 || r > 1 {
			continue
		}
		f.Samples++
		sum += r
		if r < f.MinRatio {
			f.MinRatio = r
		}
		if r > f.MaxRatio {
			f.MaxRatio = r
		}
	}
	if f.Samples == 0 {
		f.MinRatio = 0
		f.Note = "无可用样本，未拟合。模拟器只能作为趋势参考，不得给出绝对数字。"
		return f
	}
	f.MeanRatio = sum / float64(f.Samples)
	f.Note = fmt.Sprintf(
		"由 %d 个实测样本拟合：实测吞吐平均为模拟上限的 %.1f%%（范围 %.1f%%–%.1f%%）。"+
			"该因子只消掉系统性的模型外开销，不消除规模相关的偏差 —— "+
			"外推结果仍须标注为估计值并给出区间。",
		f.Samples, f.MeanRatio*100, f.MinRatio*100, f.MaxRatio*100)
	return f
}

// ApplyFactor 把开销因子应用到规模扫描结果上，给出"预计吞吐"与区间。
//
// 保守起见区间取 [下限, 上限] 由 MinRatio/MaxRatio 决定。
func ApplyFactor(rows []ScaleRow, f FittedFactor) []ScaleRow {
	if f.Samples == 0 {
		return rows
	}
	out := make([]ScaleRow, len(rows))
	copy(out, rows)
	for i := range out {
		out[i].EstimatedWriteThroughput = out[i].ClusterWriteCeiling * f.MeanRatio
		out[i].EstimatedWriteLow = out[i].ClusterWriteCeiling * f.MinRatio
		out[i].EstimatedWriteHigh = out[i].ClusterWriteCeiling * f.MaxRatio
		out[i].EstimatedReadThroughput = out[i].ClusterReadCeiling * f.MeanRatio
	}
	return out
}

// FormatFittedFactor 渲染拟合结果。
func FormatFittedFactor(f FittedFactor) string {
	if f.Samples == 0 {
		return "开销因子：未拟合（无可用样本）\n" + f.Note + "\n"
	}
	s := fmt.Sprintf("开销因子（从实测拟合）：样本 %d 个，均值 %.4f（%.2f%%），范围 %.4f–%.4f（%.2f%%–%.2f%%）\n",
		f.Samples, f.MeanRatio, f.MeanRatio*100, f.MinRatio, f.MaxRatio, f.MinRatio*100, f.MaxRatio*100)
	s += f.Note + "\n"
	return s
}

func markDivergence(rep *CalibrationReport, shards int) {
	if rep.DivergencePoint == 0 || shards < rep.DivergencePoint {
		rep.DivergencePoint = shards
	}
}

// FormatCalibration 渲染校准表。
func FormatCalibration(rep CalibrationReport) string {
	out := ""
	out += fmt.Sprintf("%-12s %-8s %-16s %-18s %-12s %-12s %-10s %s\n",
		"样本", "分片", "实测吞吐", "模拟上限", "吞吐比", "实测p50(ms)", "模拟p50", "判断")
	out += repeatStr("-", 128) + "\n"
	for _, c := range rep.Comparisons {
		out += fmt.Sprintf("%-12s %-8d %-16.0f %-18.0f %-12.3f %-12.2f %-10.2f %s\n",
			c.Label, c.Shards, c.MeasuredThroughput, c.SimulatedCeiling,
			c.ThroughputRatio, c.MeasuredP50Ms, c.SimulatedP50Ms, c.Verdict)
	}
	out += "\n" + rep.Summary + "\n"
	out += "\n必须随结果披露的警告：\n"
	for _, w := range rep.Warnings {
		out += "  - " + w + "\n"
	}
	return out
}

func repeatStr(s string, n int) string {
	b := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}
