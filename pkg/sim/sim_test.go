package sim

import (
	"math"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 基本模型
// ---------------------------------------------------------------------------

// TestRunBasics 基本字段自洽。
func TestRunBasics(t *testing.T) {
	r := Run(Config{
		Shards: 2, Replicas: 3, Domains: 3,
		LogBytes: 256, FsyncLatencyUs: 5400, BatchSize: 128,
		LeaderNICGbps: 1, ReplicaFailProb: 1e-4,
	})

	if r.Config.Replicas != 3 {
		t.Fatalf("Replicas = %d，期望 3", r.Config.Replicas)
	}
	// 多数 quorum n=3 => |Q2|=2 => 写消息 = 4
	if r.WriteMsgsPerOp != 4 {
		t.Errorf("写消息 = %d，期望 2*(3/2+1)=4", r.WriteMsgsPerOp)
	}
	if r.FsyncPerOp != 1.0/128 {
		t.Errorf("FsyncPerOp = %v，期望 1/128", r.FsyncPerOp)
	}
	if r.PerLeaderWriteCeiling != math.Min(r.PerLeaderEgressCeiling, r.PerLeaderDiskCeiling) {
		t.Errorf("单分片写上限 %v != min(出口 %v, 磁盘 %v)",
			r.PerLeaderWriteCeiling, r.PerLeaderEgressCeiling, r.PerLeaderDiskCeiling)
	}
	if math.Abs(r.ClusterWriteCeiling-r.PerLeaderWriteCeiling*2) > 1e-9 {
		t.Errorf("集群写上限 %v != 单分片 %v × 2 分片",
			r.ClusterWriteCeiling, r.PerLeaderWriteCeiling)
	}
}

// TestEgressCeilingIndependentOfBatch 出口带宽上限与批大小无关。
//
// 出处：Mencius OSDI'08 §7 —— throughput ∝ 1/(n-1)。
// 这条性质是"批处理只能抬高磁盘上限、抬不动带宽上限"的定量形式，
// 也是大规模下绑定约束必然转向带宽的原因。
func TestEgressCeilingIndependentOfBatch(t *testing.T) {
	base := Config{Shards: 1, Replicas: 5, LogBytes: 256, LeaderNICGbps: 1, FsyncLatencyUs: 5400}

	first := Run(base)
	for _, b := range []int{8, 64, 512, 4096} {
		c := base
		c.BatchSize = b
		r := Run(c)
		if math.Abs(r.PerLeaderEgressCeiling-first.PerLeaderEgressCeiling) > 1e-9 {
			t.Errorf("批大小 %d 时出口带宽上限从 %v 变成 %v —— 不应受批大小影响",
				b, first.PerLeaderEgressCeiling, r.PerLeaderEgressCeiling)
		}
		// 磁盘上限应随批大小线性增长
		wantDisk := float64(b) / (5400e-6)
		if math.Abs(r.PerLeaderDiskCeiling-wantDisk) > 1e-6 {
			t.Errorf("批大小 %d 时磁盘上限 %v，期望 %v", b, r.PerLeaderDiskCeiling, wantDisk)
		}
	}
}

// TestEgressScalesInverseFollowers 出口带宽上限 ∝ 1/(n-1)。
func TestEgressScalesInverseFollowers(t *testing.T) {
	for _, n := range []int{3, 5, 11, 101, 1001} {
		c := Config{Shards: 1, Replicas: n, LogBytes: 256, LeaderNICGbps: 1}
		r := Run(c)
		inv := r.PerLeaderEgressCeiling * float64(n-1)
		const want = 125000000.0 / 256
		if math.Abs(inv-want)/want > 1e-9 {
			t.Errorf("n=%d: 上限 × (n-1) = %.4f，期望常数 %.4f", n, inv, want)
		}
	}
}

// TestBindingConstraintShifts 批足够大时绑定约束从磁盘转向带宽。
func TestBindingConstraintShifts(t *testing.T) {
	base := Config{Shards: 1, Replicas: 5, LogBytes: 256, LeaderNICGbps: 1, FsyncLatencyUs: 5400}

	small := Run(withBatch(base, 1))
	if small.BindingConstraint != "leader_disk_fsync" {
		t.Errorf("batch=1 时绑定约束 = %s，期望 leader_disk_fsync", small.BindingConstraint)
	}

	large := Run(withBatch(base, 4096))
	if large.BindingConstraint != "leader_egress_bandwidth" {
		t.Errorf("batch=4096 时绑定约束 = %s，期望 leader_egress_bandwidth", large.BindingConstraint)
	}
}

func withBatch(c Config, b int) Config {
	c.BatchSize = b
	return c
}

// TestReadCeilingSeparateFromWrite 读上限与写上限必须是两套模型。
//
// 这是校准暴露出来的修正：初版用写路径的 fsync 上限去预测读吞吐，
// 而 readIndex 语义下的读**不落盘**。实测中读密集吞吐达到写路径上限的
// 5.4 倍，直接证明该对照是错的。
func TestReadCeilingSeparateFromWrite(t *testing.T) {
	r := Run(Config{
		Shards: 2, Replicas: 3, CrossDomainRTTMs: 0.1,
		FsyncLatencyUs: 5400, BatchSize: 1,
	})
	// 读上限只取决于 RTT 与分片数：2 * 1000/0.1 = 20000
	wantRead := 2 * (1000.0 / 0.1)
	if math.Abs(r.ClusterReadCeiling-wantRead) > 1e-6 {
		t.Errorf("读上限 = %v，期望 %v", r.ClusterReadCeiling, wantRead)
	}
	// 写上限受 fsync 限制，应显著低于读上限
	if !(r.ClusterWriteCeiling < r.ClusterReadCeiling) {
		t.Errorf("batch=1 时写上限 %.0f 应低于读上限 %.0f",
			r.ClusterWriteCeiling, r.ClusterReadCeiling)
	}
	// 两者必须是不同的模型，不能相等
	if r.ClusterReadCeiling == r.ClusterWriteCeiling {
		t.Error("读上限与写上限相等，说明用了同一个模型")
	}
}

// TestScaleMonotonic 规模扫描必须单调：分片越多，集群上限越高、心跳越多。
func TestScaleMonotonic(t *testing.T) {
	rows := Scale(Config{Replicas: 3, Domains: 3, BatchSize: 128}, []int{1, 2, 4, 16, 64})
	if len(rows) != 5 {
		t.Fatalf("行数 = %d，期望 5", len(rows))
	}

	prevCeil, prevHB := 0.0, 0.0
	for _, r := range rows {
		if r.ClusterWriteCeiling <= prevCeil {
			t.Errorf("分片 %d 时写上限 %.0f 未高于前一档 %.0f", r.Shards, r.ClusterWriteCeiling, prevCeil)
		}
		if r.HeartbeatBytesPerSec <= prevHB {
			t.Errorf("分片 %d 时心跳字节 %.0f 未高于前一档 %.0f",
				r.Shards, r.HeartbeatBytesPerSec, prevHB)
		}
		if r.Nodes != r.Shards*r.Replicas {
			t.Errorf("分片 %d: 节点数 %d != 分片 × 副本", r.Shards, r.Nodes)
		}
		prevCeil, prevHB = r.ClusterWriteCeiling, r.HeartbeatBytesPerSec
	}
}

// TestClusterAvailabilityDecreasesWithShards 分片越多，全集群同时可用的概率越低。
//
// 注意区分两个量：
//   - DataAvailability 是**单分片**可用性，与分片数无关；
//   - ClusterAvailability = 单分片可用性^分片数，随分片数指数下降。
//
// 这是"分片间独立"假设的直接推论，也是大规模部署的真实痛点：
// 单分片 99.8% 看着不错，1000 个分片同时可用的概率只有 13%。
func TestClusterAvailabilityDecreasesWithShards(t *testing.T) {
	rows := Scale(Config{Replicas: 3, Domains: 3, ReplicaFailProb: 1e-3}, []int{1, 10, 100, 1000})

	prev := 2.0
	for _, r := range rows {
		c := Run(Config{Shards: r.Shards, Replicas: 3, Domains: 3, ReplicaFailProb: 1e-3})
		if c.ClusterAvailability >= prev {
			t.Errorf("分片 %d 时全集群可用性 %.9g 未下降（前一档 %.9g）",
				r.Shards, c.ClusterAvailability, prev)
		}
		prev = c.ClusterAvailability
	}

	// 单分片可用性应与分片数无关。
	first := rows[0].DataAvailability
	for _, r := range rows {
		if math.Abs(r.DataAvailability-first) > 1e-12 {
			t.Errorf("分片 %d 时单分片可用性 %.12g 与分片 1 的 %.12g 不同 —— 不应随分片数变化",
				r.Shards, r.DataAvailability, first)
		}
	}

	// 全集群可用性应随分片数指数衰减。
	c1 := Run(Config{Shards: 1, Replicas: 3, Domains: 3, ReplicaFailProb: 1e-3})
	c1000 := Run(Config{Shards: 1000, Replicas: 3, Domains: 3, ReplicaFailProb: 1e-3})
	if !(c1000.ClusterAvailability < c1.ClusterAvailability) {
		t.Errorf("1000 分片全集群可用性 %.9g 应低于单分片 %.9g",
			c1000.ClusterAvailability, c1.ClusterAvailability)
	}
	t.Logf("单分片可用性 %.9f（与分片数无关）", first)
	t.Logf("全集群可用性：1 分片 %.9f → 1000 分片 %.6f",
		c1.ClusterAvailability, c1000.ClusterAvailability)
}

// ---------------------------------------------------------------------------
// 校准
// ---------------------------------------------------------------------------

// TestCalibrateDetectsDivergence 校准必须能在偏差过大时报"未通过"。
//
// 这是模拟器最重要的性质：它必须敢于报告自己不可信。
// 若一个模拟器在任何输入下都返回"校准通过"，那它没有校准能力。
func TestCalibrateDetectsDivergence(t *testing.T) {
	base := Config{Replicas: 3, Domains: 3, FsyncLatencyUs: 5400, BatchSize: 128}

	// 实测吞吐远超写上限 => 必然报偏离
	bad := []Measured{{
		Label: "impossible", Shards: 2, Replicas: 3, ReadRatio: 0.1,
		Throughput: 1e9, LatencyP50Ms: 1,
	}}
	rep := Calibrate(bad, base, 3.0)
	if rep.Calibrated {
		t.Error("实测吞吐达模拟上限的百倍以上，却报告校准通过")
	}
	if rep.DivergencePoint == 0 {
		t.Error("未给出偏离点")
	}
	if !strings.Contains(rep.Summary, "未通过") {
		t.Errorf("Summary 未标明未通过：%q", rep.Summary)
	}
	if len(rep.Warnings) == 0 {
		t.Error("未通过时必须附带警告")
	}
}

// TestCalibratePassesWithinTolerance 在容差内时必须报"通过"。
func TestCalibratePassesWithinTolerance(t *testing.T) {
	base := Config{Replicas: 3, Domains: 3, FsyncLatencyUs: 5400, BatchSize: 128,
		CrossDomainRTTMs: 5.0}

	// 先算一次拿到上限，再构造落在容差内的样本。
	probe := Run(base)
	samples := []Measured{{
		Label: "within", Shards: probe.Config.Shards, Replicas: probe.Config.Replicas,
		ReadRatio: 0.1, Throughput: probe.ClusterWriteCeiling / 2,
		LatencyP50Ms: probe.WriteLatencyMs,
	}}
	rep := Calibrate(samples, base, 3.0)
	if !rep.Calibrated {
		t.Errorf("样本在上限的一半、延迟等于模拟值，却报告未通过：%s", rep.Summary)
	}
}

// TestCalibrateSelectsCorrectCeiling 读密集样本必须与读上限对照。
//
// 参数选择：要让读上限与写上限**确实不同**，否则测不出对照对象是否选对。
//
// 同时注意 base.Shards 必须与样本的 Shards 一致 ——
// 否则 probe 算出的上限与 Calibrate 内部算出的上限差一个分片倍数，
// 对照关系会被稀释（本项目实测：base.Shards=1 而样本 Shards=2 时，
// 期望比值 0.05 实际得到 0.025，正好差 2 倍）。
func TestCalibrateSelectsCorrectCeiling(t *testing.T) {
	const shards = 2
	base := Config{Shards: shards, Replicas: 3, Domains: 3, CrossDomainRTTMs: 5.0,
		FsyncLatencyUs: 5400, BatchSize: 1024, LeaderNICGbps: 1}

	// 先确认两条上限确实不同。
	probe := Run(base)
	if probe.ClusterReadCeiling == probe.ClusterWriteCeiling {
		t.Fatalf("测试前提不成立：读上限与写上限同为 %.0f，无法检验对照对象",
			probe.ClusterReadCeiling)
	}
	t.Logf("分片 %d：读上限 %.0f，写上限 %.0f（相差 %.1f 倍）",
		shards, probe.ClusterReadCeiling, probe.ClusterWriteCeiling,
		probe.ClusterWriteCeiling/probe.ClusterReadCeiling)

	samples := []Measured{
		{Label: "read", Shards: shards, Replicas: 3, ReadRatio: 0.9, Throughput: 10, LatencyP50Ms: 1},
		{Label: "write", Shards: shards, Replicas: 3, ReadRatio: 0.1, Throughput: 10, LatencyP50Ms: 1},
	}
	rep := Calibrate(samples, base, 3.0)

	byLabel := map[string]Comparison{}
	for _, c := range rep.Comparisons {
		byLabel[c.Label] = c
	}
	if got := byLabel["read"].ComparedAgainst; got != "read" {
		t.Errorf("读密集样本对照对象 = %q，期望 read", got)
	}
	if got := byLabel["write"].ComparedAgainst; got != "write" {
		t.Errorf("写密集样本对照对象 = %q，期望 write", got)
	}
	if byLabel["read"].SimulatedCeiling != probe.ClusterReadCeiling {
		t.Errorf("读密集样本对照的上限 = %.0f，期望读上限 %.0f",
			byLabel["read"].SimulatedCeiling, probe.ClusterReadCeiling)
	}
	if byLabel["write"].SimulatedCeiling != probe.ClusterWriteCeiling {
		t.Errorf("写密集样本对照的上限 = %.0f，期望写上限 %.0f",
			byLabel["write"].SimulatedCeiling, probe.ClusterWriteCeiling)
	}
}

// TestCalibrateEmptySamples 无样本时必须明确拒绝，而不是静默通过。
func TestCalibrateEmptySamples(t *testing.T) {
	rep := Calibrate(nil, Config{}, 3.0)
	if rep.Calibrated {
		t.Error("无样本时不应报告校准通过")
	}
	if !strings.Contains(rep.Summary, "不得写入论文") {
		t.Errorf("无样本时 Summary 应明确禁止使用：%q", rep.Summary)
	}
}

// TestFitOverheadFactor 开销因子拟合必须落在观测区间内。
func TestFitOverheadFactor(t *testing.T) {
	base := Config{Replicas: 3, Domains: 3, FsyncLatencyUs: 5400, BatchSize: 128}
	probe := Run(base)

	// 构造三个已知比值的样本
	ratios := []float64{0.05, 0.10, 0.15}
	samples := make([]Measured, 0, len(ratios))
	for i, r := range ratios {
		samples = append(samples, Measured{
			Label: "s", Shards: probe.Config.Shards, Replicas: probe.Config.Replicas,
			ReadRatio: 0.1, Throughput: probe.ClusterWriteCeiling * r,
			LatencyP50Ms: probe.WriteLatencyMs,
		})
		_ = i
	}

	rep := Calibrate(samples, base, 100.0) // 放大容差以免被判偏离
	f := FitOverheadFactor(rep)

	if f.Samples != len(ratios) {
		t.Fatalf("参与拟合的样本数 = %d，期望 %d", f.Samples, len(ratios))
	}
	// 均值应约为 0.10
	if math.Abs(f.MeanRatio-0.10) > 0.02 {
		t.Errorf("均值 = %.4f，期望约 0.10", f.MeanRatio)
	}
	if math.Abs(f.MinRatio-0.05) > 0.01 {
		t.Errorf("最小值 = %.4f，期望约 0.05", f.MinRatio)
	}
	if math.Abs(f.MaxRatio-0.15) > 0.01 {
		t.Errorf("最大值 = %.4f，期望约 0.15", f.MaxRatio)
	}
	if f.Note == "" {
		t.Error("Note 不应为空")
	}
}

// TestFitOverheadFactorIgnoresRatiosAboveOne 比值 > 1 的样本不参与拟合。
//
// 比值 > 1 说明对照对象选错或实测条件异常，纳入拟合会污染因子。
//
// 本测试用两个样本：一个比值 0.05（正常），一个比值 5（异常）。
// 异常样本被排除后，拟合均值应恰好是 0.05 —— 若被污染则会是更大的值。
//
// 注意 base.Shards 必须与样本 Shards 一致，否则 probe 的上限与实际对照的
// 上限差一个分片倍数，期望比值会被稀释。
func TestFitOverheadFactorIgnoresRatiosAboveOne(t *testing.T) {
	const shards = 2
	base := Config{Shards: shards, Replicas: 3, Domains: 3,
		FsyncLatencyUs: 5400, BatchSize: 128}
	probe := Run(base)

	const normalRatio = 0.05
	samples := []Measured{
		{Label: "normal", Shards: shards, Replicas: 3, ReadRatio: 0.1,
			Throughput: probe.ClusterWriteCeiling * normalRatio, LatencyP50Ms: probe.WriteLatencyMs},
		{Label: "anomalous", Shards: shards, Replicas: 3, ReadRatio: 0.1,
			Throughput: probe.ClusterWriteCeiling * 5, LatencyP50Ms: probe.WriteLatencyMs},
	}
	rep := Calibrate(samples, base, 100.0)
	f := FitOverheadFactor(rep)

	if f.Samples != 1 {
		t.Fatalf("参与拟合的样本数 = %d，期望 1（比值 5 的样本应被排除）", f.Samples)
	}
	if math.Abs(f.MeanRatio-normalRatio) > 1e-9 {
		t.Errorf("均值 = %.6f，期望 %.6f（未被异常样本污染）", f.MeanRatio, normalRatio)
	}
	if math.Abs(f.MaxRatio-normalRatio) > 1e-9 {
		t.Errorf("最大值 = %.6f，期望 %.6f（异常样本不应进入区间）", f.MaxRatio, normalRatio)
	}
}

// TestFitOverheadFactorEmpty 无可用样本时不得崩溃或给出假因子。
func TestFitOverheadFactorEmpty(t *testing.T) {
	f := FitOverheadFactor(CalibrationReport{})
	if f.Samples != 0 {
		t.Errorf("样本数 = %d，期望 0", f.Samples)
	}
	if f.MeanRatio != 0 {
		t.Errorf("无样本时均值 = %v，期望 0", f.MeanRatio)
	}
	if f.Note == "" {
		t.Error("无样本时也应给出说明")
	}
	// 应用到规模结果上不应改变上限
	rows := Scale(Config{}, []int{1, 2})
	applied := ApplyFactor(rows, f)
	for i := range rows {
		if applied[i].EstimatedWriteThroughput != 0 {
			t.Errorf("无因子时不应给出估计值，得到 %v", applied[i].EstimatedWriteThroughput)
		}
		if applied[i].ClusterWriteCeiling != rows[i].ClusterWriteCeiling {
			t.Error("无因子时不应改变上限")
		}
	}
}

// TestApplyFactorProducesInterval 应用因子后必须给出区间，且区间包住均值。
func TestApplyFactorProducesInterval(t *testing.T) {
	rows := Scale(Config{Replicas: 3, BatchSize: 128}, []int{1, 2, 4})
	f := FittedFactor{Samples: 2, MeanRatio: 0.1, MinRatio: 0.05, MaxRatio: 0.2, Note: "test"}

	out := ApplyFactor(rows, f)
	for i, r := range out {
		if r.EstimatedWriteThroughput <= 0 {
			t.Errorf("第 %d 行估计值为 0", i)
		}
		if !(r.EstimatedWriteLow <= r.EstimatedWriteThroughput &&
			r.EstimatedWriteThroughput <= r.EstimatedWriteHigh) {
			t.Errorf("第 %d 行区间 [%v, %v] 未包住均值 %v",
				i, r.EstimatedWriteLow, r.EstimatedWriteThroughput, r.EstimatedWriteHigh)
		}
		// 估计值 = 上限 × 均值因子
		want := r.ClusterWriteCeiling * f.MeanRatio
		if math.Abs(r.EstimatedWriteThroughput-want) > 1e-9 {
			t.Errorf("第 %d 行估计值 %v，期望 %v", i, r.EstimatedWriteThroughput, want)
		}
	}
}

// TestCaveatsAlwaysPresent 任何结果都必须随附 Caveats。
//
// 这是模拟器最容易被误用的地方：有人会把模拟数字当实测写进论文。
// Caveats 是防止这种误用的最后一道防线。
func TestCaveatsAlwaysPresent(t *testing.T) {
	r := Run(Config{})
	if len(r.Caveats) == 0 {
		t.Fatal("Caveats 为空 —— 模拟结果必须随附使用限制")
	}
	joined := strings.Join(r.Caveats, " ")
	for _, kw := range []string{"未建模", "成本结构", "不是性能预测"} {
		if !strings.Contains(joined, kw) {
			t.Errorf("Caveats 应包含关键字 %q，实际：%s", kw, joined)
		}
	}
}

// TestNilZeroConfigWorks 零值配置必须可用。
func TestNilZeroConfigWorks(t *testing.T) {
	r := Run(Config{})
	if r.Config.Shards <= 0 || r.Config.Replicas <= 0 || r.Config.Domains <= 0 {
		t.Errorf("零值配置未被补齐：%+v", r.Config)
	}
	if r.ClusterWriteCeiling <= 0 {
		t.Error("零值配置下写上限不合理")
	}
	if len(r.Caveats) == 0 {
		t.Error("零值配置下 Caveats 为空")
	}
}
