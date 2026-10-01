package model

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"testing"
)

// approxEq 相对误差比较。
func approxEq(a, b, relTol float64) bool {
	if b == 0 {
		return math.Abs(a) < relTol
	}
	return math.Abs(a-b)/math.Abs(b) < relTol
}

// TestLeaderEgressCeiling 验证 leader 出口带宽上限公式。
//
// 公式：C / ((n-1) * β)
// 出处：Mencius, OSDI 2008 §7 —— "its throughput is in proportion to 1/(n-1)"
//
// 这个式子是论文里最常被引用的数字，必须算对。
func TestLeaderEgressCeiling(t *testing.T) {
	cases := []struct {
		n        int
		nicGbps  float64
		payload  int
		want     float64
	}{
		// 1 Gb/s = 125 MB/s；n=5 => 4 个 follower；β=256
		// 125e6 / (4*256) = 122070.3125
		{5, 1, 256, 125000000.0 / (4 * 256)},
		// n=3 => 2 个 follower
		{3, 1, 256, 125000000.0 / (2 * 256)},
		// n=101 => 100 个 follower
		{101, 1, 256, 125000000.0 / (100 * 256)},
		// n=1000 => 999 个 follower
		{1000, 1, 256, 125000000.0 / (999 * 256)},
		// 10 Gb/s
		{5, 10, 256, 1250000000.0 / (4 * 256)},
		// 载荷翻倍 => 吞吐减半
		{5, 1, 512, 125000000.0 / (4 * 512)},
	}
	for _, c := range cases {
		r := Analyze(Config{
			Replicas: c.n, PayloadBytes: c.payload, LeaderNICGbps: c.nicGbps,
			BatchSize: 1, FsyncLatencyUs: 100,
		})
		var got float64
		for _, cl := range r.Ceilings {
			if cl.Name == "leader_egress_bandwidth" {
				got = cl.Value
			}
		}
		if !approxEq(got, c.want, 1e-9) {
			t.Errorf("n=%d nic=%.0fGbps β=%d: egress = %.4f，期望 %.4f",
				c.n, c.nicGbps, c.payload, got, c.want)
		}
	}
}

// TestEgressScalesInverseFollowers egress 上限 ∝ 1/(n-1)。
//
// 这是"leader 瓶颈"论断的定量形式。若这条不成立，论文的核心动机就垮了。
func TestEgressScalesInverseFollowers(t *testing.T) {
	base := 0.0
	for _, n := range []int{3, 5, 11, 101, 1001} {
		r := Analyze(Config{Replicas: n, PayloadBytes: 256, LeaderNICGbps: 1, BatchSize: 1})
		var v float64
		for _, cl := range r.Ceilings {
			if cl.Name == "leader_egress_bandwidth" {
				v = cl.Value
			}
		}
		// C/((n-1)β) * (n-1) 应恒等于 C/β，即与 n 无关。
		inv := v * float64(n-1)
		const wantInv = 125000000.0 / 256
		if !approxEq(inv, wantInv, 1e-9) {
			t.Errorf("n=%d: v*(n-1) = %.6f，期望常数 %.6f", n, inv, wantInv)
		}
		if base > 0 && v >= base {
			t.Errorf("n=%d 时 egress 未随 n 下降（%.2f >= %.2f）", n, v, base)
		}
		base = v
	}
}

// TestDiskCeilingBatchAmortization 磁盘上限 = b / t_fsync，随批大小线性增长。
func TestDiskCeilingBatchAmortization(t *testing.T) {
	for _, b := range []int{1, 8, 64, 512} {
		r := Analyze(Config{Replicas: 5, BatchSize: b, FsyncLatencyUs: 100})
		var got float64
		for _, c := range r.Ceilings {
			if c.Name == "leader_disk_fsync" {
				got = c.Value
			}
		}
		want := float64(b) / 100e-6
		if !approxEq(got, want, 1e-9) {
			t.Errorf("b=%d: disk = %.2f，期望 %.2f", b, got, want)
		}
	}
}

// TestBindingConstraintPicksMin 有效吞吐 = 各上限的最小值，且标记正确。
func TestBindingConstraintPicksMin(t *testing.T) {
	r := Analyze(Config{
		Replicas: 5, PayloadBytes: 256, LeaderNICGbps: 1,
		BatchSize: 1, FsyncLatencyUs: 100, CPUCostPerEntryNs: 200,
	})

	minV := math.Inf(1)
	for _, c := range r.Ceilings {
		if c.Value < minV {
			minV = c.Value
		}
	}
	if !approxEq(r.ThroughputBound, minV, 1e-12) {
		t.Errorf("ThroughputBound = %.4f，期望最小值 %.4f", r.ThroughputBound, minV)
	}

	bindingCount := 0
	for _, c := range r.Ceilings {
		if c.Binding {
			bindingCount++
			if c.Name != r.Binding {
				t.Errorf("Binding 标记为 %s 但 Binding=true 的是 %s", r.Binding, c.Name)
			}
			if !approxEq(c.Value, r.ThroughputBound, 1e-12) {
				t.Errorf("Binding 约束 %s 的值 %.4f != ThroughputBound %.4f",
					c.Name, c.Value, r.ThroughputBound)
			}
		}
	}
	if bindingCount != 1 {
		t.Errorf("恰好应有一个 binding 约束，实际 %d 个", bindingCount)
	}
}

// TestDiskBecomesBindingWithBatching 批处理把磁盘上限抬上去后，绑定约束应转移。
//
// 这是本项目的一个关键实测发现：本机 fsync≈5.4ms、batch=1 时真正的瓶颈是磁盘
// （约 185 ops/s），而不是 leader 带宽（122,070 ops/s）——相差三个数量级。
// 模型必须能反映这个转移，否则论文会用错故事解释数据。
func TestDiskBecomesBindingWithBatching(t *testing.T) {
	// batch=1、t_fsync=5.4ms：磁盘 185 ops/s，远低于 egress 122070
	r1 := Analyze(Config{
		Replicas: 5, PayloadBytes: 256, LeaderNICGbps: 1,
		BatchSize: 1, FsyncLatencyUs: 5400, CPUCostPerEntryNs: 200,
	})
	if r1.Binding != "leader_disk_fsync" {
		t.Errorf("batch=1 时绑定约束应为磁盘，实际 %s（bound=%.1f, egress=%.1f）",
			r1.Binding, r1.ThroughputBound, egressOf(r1))
	}
	// 磁盘上限约 1/0.0054 = 185.2
	if !approxEq(r1.ThroughputBound, 1.0/0.0054, 1e-6) {
		t.Errorf("batch=1 时吞吐上界 = %.2f，期望 %.2f", r1.ThroughputBound, 1.0/0.0054)
	}

	// batch=128：磁盘抬到 23,704，仍低于 egress 122070
	r2 := Analyze(Config{
		Replicas: 5, PayloadBytes: 256, LeaderNICGbps: 1,
		BatchSize: 128, FsyncLatencyUs: 5400, CPUCostPerEntryNs: 200,
	})
	if r2.Binding != "leader_disk_fsync" {
		t.Errorf("batch=128 时绑定约束应为磁盘，实际 %s", r2.Binding)
	}

	// batch=2048：磁盘 379,259 > egress 122070 => 绑定转向 leader 带宽
	r3 := Analyze(Config{
		Replicas: 5, PayloadBytes: 256, LeaderNICGbps: 1,
		BatchSize: 2048, FsyncLatencyUs: 5400, CPUCostPerEntryNs: 200,
	})
	if r3.Binding != "leader_egress_bandwidth" {
		t.Errorf("批足够大时绑定约束应转为 leader 带宽，实际 %s（bound=%.1f）",
			r3.Binding, r3.ThroughputBound)
	}
	t.Logf("绑定约束转移：b=1 -> %s (%.1f) ; b=128 -> %s (%.1f) ; b=2048 -> %s (%.1f)",
		r1.Binding, r1.ThroughputBound, r2.Binding, r2.ThroughputBound,
		r3.Binding, r3.ThroughputBound)
}

func egressOf(r Report) float64 {
	for _, c := range r.Ceilings {
		if c.Name == "leader_egress_bandwidth" {
			return c.Value
		}
	}
	return 0
}

// TestLamportBounds 验证 Lamport 的三个精确界在 n=1000 处的取值。
//
// 出处：Lamport, "Lower Bounds on Consensus", MSR 2000 /
// Distributed Computing 19:104-125 (2006), Theorem 2 与 Theorem 3。
//   - 2 个延迟最优:  n(m-1)
//   - 消息数最少:     m + n - 2，但需要 m 个消息延迟
//   - 经典 synod:    2m + n - 3，3 个消息延迟
//
// n=1000, m=501 时依次是 500000 / 1499 / 1999。
func TestLamportBounds(t *testing.T) {
	r := Analyze(Config{Replicas: 1000})
	if r.MajoritySize != 501 {
		t.Fatalf("多数集合大小 = %d，期望 501", r.MajoritySize)
	}

	want := map[string]struct {
		value  int64
		delays int
	}{
		"lamport_2_delay_optimal": {1000 * 500, 2},
		"lamport_min_messages":    {501 + 1000 - 2, 501},
		"lamport_classic_synod":   {2*501 + 1000 - 3, 3},
		"multipaxos_steady_state": {2 * 501, 1},
	}
	for _, m := range r.Messages {
		w, ok := want[m.Label]
		if !ok {
			continue
		}
		if m.Value != w.value {
			t.Errorf("%s = %d，期望 %d", m.Label, m.Value, w.value)
		}
		if m.Delays != w.delays {
			t.Errorf("%s 延迟 = %d，期望 %d", m.Label, m.Delays, w.delays)
		}
		delete(want, m.Label)
	}
	for label := range want {
		t.Errorf("缺少消息界: %s", label)
	}
}

// TestFlexiblePaxosConstraint 验证 |Q1| + |Q2| > N 约束成立。
//
// 出处：Howard, Malkhi, Spiegelman, "Flexible Paxos: Quorum Intersection
// Revisited", OPODIS 2016, §4.2。
//
// 测试解析 Note 文本里**实际写出**的数值，而不是从 label 的 f 推导 ——
// 后者只是用同一套公式自证，测不出 Note 与公式不一致。
func TestFlexiblePaxosConstraint(t *testing.T) {
	const n = 100
	r := Analyze(Config{Replicas: n})

	// Note 形如：|Q2|=3（容忍 2 个故障），需 |Q1|=98 > N-|Q2|；选主需 98/100 节点在线
	re := regexp.MustCompile(`\|Q2\|=(\d+).*\|Q1\|=(\d+)`)
	reFaults := regexp.MustCompile(`容忍 (\d+) 个故障`)

	found := 0
	for _, m := range r.Messages {
		sub := re.FindStringSubmatch(m.Note)
		if sub == nil {
			continue
		}
		found++
		q2, _ := strconv.Atoi(sub[1])
		q1, _ := strconv.Atoi(sub[2])

		if q1+q2 <= n {
			t.Errorf("%s: |Q1|(%d) + |Q2|(%d) = %d 不满足 > N(%d)",
				m.Label, q1, q2, q1+q2, n)
		}
		if m.Value != int64(2*q2) {
			t.Errorf("%s: 消息数 %d != 2*|Q2| = %d", m.Label, m.Value, 2*q2)
		}
		if m.Delays != 1 {
			t.Errorf("%s: 稳态延迟应为 1，实际 %d", m.Label, m.Delays)
		}
		// label 里的 f 与 Note 里的"容忍 f 个故障"必须一致。
		var fFromLabel int
		if _, err := fmtSscanf(m.Label, "flexible_paxos_f%d", &fFromLabel); err == nil {
			if fm := reFaults.FindStringSubmatch(m.Note); fm != nil {
				fFromNote, _ := strconv.Atoi(fm[1])
				if fFromLabel != fFromNote {
					t.Errorf("%s: label 的 f=%d 与 Note 的 f=%d 不一致",
						m.Label, fFromLabel, fFromNote)
				}
				if q2 != fFromNote+1 {
					t.Errorf("%s: |Q2|=%d 应为 f+1=%d", m.Label, q2, fFromNote+1)
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("未生成任何 flexible_paxos 方案（Note 解析失败或未生成）")
	}
	if found > 5 {
		t.Errorf("flexible_paxos 方案数 = %d，期望最多 5（f=1..5）", found)
	}
	t.Logf("验证了 %d 个 Flexible Paxos 方案，N=%d", found, n)
}

// TestAvailabilityOversizing n=1000 时容错被过度配置。
func TestAvailabilityOversizing(t *testing.T) {
	r := Analyze(Config{Replicas: 1000})

	// 多数 quorum = 501，可容忍 499 个故障。
	if r.Availability.CrashTolerated != 499 {
		t.Errorf("可容忍故障 = %d，期望 499", r.Availability.CrashTolerated)
	}
	if r.Availability.QuorumNeeded != 501 {
		t.Errorf("需要副本 = %d，期望 501", r.Availability.QuorumNeeded)
	}
	// 而 f=5 只需要 6 个副本 => 冗余倍数约 83.5
	if r.Availability.OversizingFactor < 80 || r.Availability.OversizingFactor > 90 {
		t.Errorf("冗余倍数 = %.2f，期望约 83.5", r.Availability.OversizingFactor)
	}
}

// TestCrossZoneLatency 跨域 RTT 应体现在稳态延迟里。
func TestCrossZoneLatency(t *testing.T) {
	local := Analyze(Config{Replicas: 5, FsyncLatencyUs: 1000})
	wan := Analyze(Config{Replicas: 5, FsyncLatencyUs: 1000, CrossZoneRTTMs: 50})

	if wan.SteadyStateLatencyMs <= local.SteadyStateLatencyMs {
		t.Fatalf("跨域延迟 %.2fms 未高于局域网 %.2fms",
			wan.SteadyStateLatencyMs, local.SteadyStateLatencyMs)
	}
	// 跨域时应约等于 RTT + fsync = 50 + 1 = 51ms
	if !approxEq(wan.SteadyStateLatencyMs, 51.0, 0.02) {
		t.Errorf("跨域稳态延迟 = %.3fms，期望约 51ms", wan.SteadyStateLatencyMs)
	}
}

// TestHeartbeatBandwidth 心跳带宽随分片数与副本数增长。
func TestHeartbeatBandwidth(t *testing.T) {
	one := Analyze(Config{Replicas: 5, Groups: 1, HeartbeatIntervalMs: 100})
	many := Analyze(Config{Replicas: 5, Groups: 100, HeartbeatIntervalMs: 100})

	if many.HeartbeatBytesPerSec <= one.HeartbeatBytesPerSec {
		t.Fatal("心跳带宽未随分片数增长")
	}
	// 应严格线性。
	ratio := many.HeartbeatBytesPerSec / one.HeartbeatBytesPerSec
	if !approxEq(ratio, 100, 1e-9) {
		t.Errorf("心跳带宽比 = %.4f，期望 100（分片数）", ratio)
	}

	// 心跳更频繁 => 带宽更高
	fast := Analyze(Config{Replicas: 5, Groups: 1, HeartbeatIntervalMs: 50})
	if !approxEq(fast.HeartbeatBytesPerSec/one.HeartbeatBytesPerSec, 2, 1e-9) {
		t.Errorf("心跳间隔减半未使带宽翻倍：%.2f", fast.HeartbeatBytesPerSec/one.HeartbeatBytesPerSec)
	}
}

// TestWANBytesPerOp 单次写的跨域字节数应约等于 (n-1)*β + β。
func TestWANBytesPerOp(t *testing.T) {
	r := Analyze(Config{Replicas: 5, PayloadBytes: 256})
	want := int64(4*256 + 256)
	if r.WANBytesPerOp != want {
		t.Errorf("WANBytesPerOp = %d，期望 %d", r.WANBytesPerOp, want)
	}
}

// TestDisseminateIDsOnly 只散布 id 时应大幅降低出口成本。
func TestDisseminateIDsOnly(t *testing.T) {
	full := Analyze(Config{Replicas: 5, PayloadBytes: 256, LeaderNICGbps: 1})
	ids := Analyze(Config{
		Replicas: 5, PayloadBytes: 256, LeaderNICGbps: 1,
		DisseminateIDsOnly: true, IDBytes: 24,
	})
	egFull, egIDs := egressOf(full), egressOf(ids)
	if egIDs <= egFull {
		t.Fatal("只散布 id 未提高出口上限")
	}
	want := float64(256) / float64(24)
	if !approxEq(egIDs/egFull, want, 1e-6) {
		t.Errorf("提升倍数 = %.3f，期望 %.3f", egIDs/egFull, want)
	}
	t.Logf("只散布 id: egress %.0f -> %.0f（%.1fx）", egFull, egIDs, egIDs/egFull)
}

// TestZeroValueConfigWorks 零值配置必须可用（默认值补齐）。
func TestZeroValueConfigWorks(t *testing.T) {
	r := Analyze(Config{})
	if r.MajoritySize <= 0 {
		t.Error("零值配置下多数集合大小不合理")
	}
	if r.ThroughputBound <= 0 {
		t.Error("零值配置下吞吐上界不合理")
	}
	if len(r.Ceilings) != 3 {
		t.Errorf("上限数量 = %d，期望 3", len(r.Ceilings))
	}
	if r.Binding == "" {
		t.Error("未标记绑定约束")
	}
}

// TestScaleTableMonotonic ScaleTable 应产生随 n 单调递减的 egress 上限。
func TestScaleTableMonotonic(t *testing.T) {
	rows := ScaleTable(Config{PayloadBytes: 256, LeaderNICGbps: 1}, []int{3, 5, 11, 101, 1001})
	if len(rows) != 5 {
		t.Fatalf("行数 = %d，期望 5", len(rows))
	}
	prev := math.Inf(1)
	for _, r := range rows {
		v := egressOf(r)
		if v >= prev {
			t.Errorf("n=%d 时 egress %.2f 未低于前一档 %.2f", r.Config.Replicas, v, prev)
		}
		prev = v
	}
}

// TestFormatCeilingsRenders 表格渲染不 panic 且含关键表头。
func TestFormatCeilingsRenders(t *testing.T) {
	rows := ScaleTable(Config{PayloadBytes: 256}, []int{5, 101})
	out := FormatCeilings(rows)
	if out == "" {
		t.Fatal("FormatCeilings 输出为空")
	}
	for _, want := range []string{"binding", "egress", "disk", "cpu", "bound"} {
		if !contains(out, want) {
			t.Errorf("表格缺少列 %q", want)
		}
	}
}

// TestRatioHandlesZero 除零不应 panic。
func TestRatioHandlesZero(t *testing.T) {
	if got := Ratio(0, 0); got != 1 {
		t.Errorf("Ratio(0,0) = %v，期望 1", got)
	}
	if got := Ratio(1, 0); !math.IsInf(got, 1) {
		t.Errorf("Ratio(1,0) = %v，期望 +Inf", got)
	}
	if got := Ratio(6, 3); got != 2 {
		t.Errorf("Ratio(6,3) = %v，期望 2", got)
	}
}

// --- 小工具 ---

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// fmtSscanf 是 fmt.Sscanf 的薄封装，避免测试文件里再多一个 import 名字冲突。
func fmtSscanf(s, format string, args ...any) (int, error) {
	return fmt.Sscanf(s, format, args...)
}
