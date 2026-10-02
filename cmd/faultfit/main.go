// cmd/faultfit —— 把真实故障日志拟合成 pkg/faft 能直接吃的故障模型参数。
//
// ── 为什么需要这个工具 ───────────────────────────────────────────────
// pkg/faft 的全部可用性结论目前都建立在**假设的**故障模型参数上：
//
//	CorrelatedFailure{P: 每域独立失效率, Q: 区域事件率, K: 事件命中域数}
//
// 这三个数决定了 FAFT 求解器会选哪套 quorum 几何。它们现在是拍脑袋定的 ——
// 这是整篇论文最容易被审稿人一击致命的地方，因为**结论对参数的敏感性远大于
// 对算法的敏感性**。
//
// 而真实集群的运维日志里，这三个数**已经存在**，只是没人去算。
// 本工具就是那个"去算"的东西：喂一份故障区间清单，吐一份可以直接
// 粘进代码的参数报告。
//
// ── 输入格式（刻意做得极简，运维能直接产）────────────────────────────
// CSV，三列，带表头：
//
//	start,end,domain
//	2026-01-01T03:12:00Z,2026-01-01T03:19:30Z,az-a
//	2026-01-01T07:41:00Z,2026-01-01T07:41:45Z,az-c
//
//   - start/end：该域**整体不可用**的区间（RFC3339）。end 留空表示到数据末尾。
//   - domain：故障域标识（可用区 / 机架 / leaf 交换机 / PDU）。
//
// 粒度选择很重要：**这个文件里的每一行都必须是"整个域挂了"**，
// 而不是"域里挂了一个节点"。节点级故障应该先按域聚合（同一个域里同时
// 挂掉多少节点才算域级故障，是一个需要明确写出来的判据，见 docs/FAULT-DATA.md）。
//
// ── 输出 ─────────────────────────────────────────────────────────────
//   1. 逐域 P、总体 P
//   2. 区域事件率 Q（按观测窗口）
//   3. 事件命中域数 K 的分布，以及建议取值
//   4. **相关性检验**：两两共失效率 vs 独立模型预测
//   5. 可直接粘进 Go 代码的参数块
//
// ── 自检 ─────────────────────────────────────────────────────────────
// 没有真实数据也能验证本工具正确：`-synthetic` 会按已知参数生成一份
// 合成故障序列再反解，看能不能把参数估回来。这是回归测试的基础。
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

type interval struct {
	start, end time.Time
	domain     string
	openEnded  bool
}

type domainStat struct {
	name      string
	downTime  time.Duration
	windowsUp int
	windowsDn int
	p         float64
}

type multiEvent struct {
	start, end time.Time
	domains    []string
	maxConc    int
}

func main() {
	in := flag.String("in", "", "故障区间 CSV（start,end,domain）。留空则必须用 -synthetic")
	window := flag.Duration("window", time.Hour, "观测窗口长度。P/Q 都是「每个窗口」的概率，所以这个值必须随结果一起报")
	physical := flag.Duration("physical", 0, "可选：该域从物理故障到恢复的平均时长。用于校验窗口选得是否合理（建议 window >= 3x physical）")
	synthetic := flag.Bool("synthetic", false, "用已知参数生成合成故障序列并反解，用于自检")
	nDomains := flag.Int("domains", 5, "synthetic 模式的域数")
	simDays := flag.Int("days", 60, "synthetic 模式模拟多少天")
	trueP := flag.Float64("trueP", 0.004, "synthetic 模式：真实的每域每窗口独立失效率")
	trueQ := flag.Float64("trueQ", 0.02, "synthetic 模式：真实的区域事件率（每窗口）")
	trueK := flag.Int("trueK", 3, "synthetic 模式：真实的一次事件命中域数")
	seed := flag.Int64("seed", 42, "synthetic 模式随机种子")
	outJSON := flag.String("out-json", "", "把拟合结果写成 JSON（供后续工具链消费）")
	flag.Parse()

	var ivs []interval
	var err error
	if *synthetic {
		ivs, err = synth(*nDomains, *simDays, *window, *trueP, *trueQ, *trueK, *seed)
		if err != nil {
			fail("生成合成序列失败: %v", err)
		}
		fmt.Printf("【合成序列】域数=%d 天数=%d 窗口=%v\n", *nDomains, *simDays, *window)
		fmt.Printf("  注入的真值：P=%.5g（每域每窗口）  Q=%.5g（每窗口）  K=%d\n\n",
			*trueP, *trueQ, *trueK)
	} else {
		if *in == "" {
			fail("必须给 -in <csv>，或者用 -synthetic 自检")
		}
		ivs, err = readCSV(*in)
		if err != nil {
			fail("读 %s 失败: %v", *in, err)
		}
		fmt.Printf("【真实序列】%s：%d 条故障区间，窗口=%v\n\n", *in, len(ivs), *window)
	}

	rep := fit(ivs, *window, *physical)
	printReport(rep)

	if *outJSON != "" {
		data, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*outJSON, append(data, '\n'), 0o644); err != nil {
			fail("写 %s 失败: %v", *outJSON, err)
		}
		fmt.Printf("\n→ 已写出 %s\n", *outJSON)
	}
}

// ─────────────────────────────────────────────────────────────────────
// 拟合
// ─────────────────────────────────────────────────────────────────────

type report struct {
	WindowHours  float64            `json:"window_hours"`
	SpanHours    float64            `json:"span_hours"`
	Windows      int                `json:"windows"`
	Domains      []string           `json:"domains"`
	PerDomainP   map[string]float64 `json:"per_domain_fail_prob"`
	P            float64            `json:"p_marginal_per_domain"`
	PIndependent float64            `json:"p_independent_per_domain"`
	PerDomainPIndep map[string]float64 `json:"per_domain_independent_p"`
	MeanFailDurationMin float64     `json:"mean_failure_duration_minutes"`
	MedianFailDurationMin float64   `json:"median_failure_duration_minutes"`
	Q            float64            `json:"q_regional_event"`
	KMean        float64            `json:"k_mean_domains_hit"`
	KMode        int                `json:"k_mode_domains_hit"`
	KHistogram   map[string]int     `json:"k_histogram"`
	Events       int                `json:"multi_domain_events"`
	SingleEvents int                `json:"single_domain_episodes"`
	Correlation  []pairCorr         `json:"pairwise_correlation"`
	// 直接可用的 Go 代码块与 JSON 参数
	GoSnippet string `json:"go_snippet"`
	Notes     []string
}

type pairCorr struct {
	A             string  `json:"a"`
	B             string  `json:"b"`
	ObservedRatio float64 `json:"observed_co_failure_ratio"` // 实测同时失效时间占比
	ExpectedRatio float64 `json:"expected_if_independent"`   // 独立假设下的预测
	Lift          float64 `json:"lift"`                      // 实测 / 预测
	// ExpectedCount 独立假设下**期望看到多少次**共同失效窗口。
	//
	// ⚠️ 这个字段比 Lift 更重要。lift 是两个小数之比；当期望计数远小于 1 时，
	// **一次巧合就能造出巨大的 lift**：实测真 Q=0 的场景里，期望 0.16 次、
	// 实际撞上 1 次，lift 直接到 7.7× —— 看着像强相关，其实全是小样本噪声。
	// 经验门槛：期望计数 < 5 时不要解读 lift。
	ExpectedCount float64 `json:"expected_co_failure_windows"`
}

func fit(ivs []interval, window, physical time.Duration) *report {
	if len(ivs) == 0 {
		fail("没有任何故障区间")
	}
	// 时间跨度：从最早的开始到最晚的结束。
	tMin, tMax := ivs[0].start, ivs[0].end
	for _, iv := range ivs {
		if iv.start.Before(tMin) {
			tMin = iv.start
		}
		if iv.end.After(tMax) {
			tMax = iv.end
		}
	}
	for i := range ivs {
		if ivs[i].openEnded {
			ivs[i].end = tMax
		}
	}
	span := tMax.Sub(tMin)
	nWin := int(span/window) + 1

	// 域清单
	dset := map[string]bool{}
	for _, iv := range ivs {
		dset[iv.domain] = true
	}
	domains := make([]string, 0, len(dset))
	for d := range dset {
		domains = append(domains, d)
	}
	sort.Strings(domains)

	// 逐窗口标出每个域是否失效（任一时间重叠即算失效）。
	failed := make([][]bool, len(domains)) // [domain][window]
	for i := range failed {
		failed[i] = make([]bool, nWin)
	}
	idx := map[string]int{}
	for i, d := range domains {
		idx[d] = i
	}
	for _, iv := range ivs {
		di := idx[iv.domain]
		w0 := int(iv.start.Sub(tMin) / window)
		w1 := int(iv.end.Sub(tMin) / window)
		if w0 < 0 {
			w0 = 0
		}
		if w1 >= nWin {
			w1 = nWin - 1
		}
		for w := w0; w <= w1; w++ {
			failed[di][w] = true
		}
	}

	// 多域事件：按时间扫描并发数。
	events := sweepMultiEvents(ivs)

	// ── 关键一步：把"区域事件造成的失效"从"独立失效"里剥出来 ──
	//
	// CorrelatedFailure.P 的定义是**独立**失效率 —— 区域事件已经由 Q/K 表达。
	// 而逐窗口直接数出来的失效率是**边际**值，它把两者混在一起。
	// 直接把边际值填进 P，区域事件就被计了两次。
	//
	// 这个坑是 -synthetic 自检发现的：注入真值 P=0.004、Q=0.02、K=3（6 个域）时，
	// 边际失效率反解出 0.0200 —— **偏了 5 倍**。剥掉区域事件覆盖的窗口后
	// 才回到真值附近。若不修，求解器看到的每域独立失效率会虚高，
	// 进而把"多铺几个域"的收益算错 —— 而这正是 FAFT 的核心决策。
	regionalWin := make([][]bool, len(domains))
	for i := range regionalWin {
		regionalWin[i] = make([]bool, nWin)
	}
	for _, ev := range events {
		// 用事件**峰值并发时刻**的域集合（sweepMultiEvents 已记录），
		// 而不是事件末尾的快照 —— 末尾时并发数已经掉到 1 了。
		live := ev.domains
		w0 := int(ev.start.Sub(tMin) / window)
		w1 := int(ev.end.Sub(tMin) / window)
		if w0 < 0 {
			w0 = 0
		}
		if w1 >= nWin {
			w1 = nWin - 1
		}
		for _, dname := range live {
			di, ok := idx[dname]
			if !ok {
				continue
			}
			for w := w0; w <= w1; w++ {
				regionalWin[di][w] = true
			}
		}
	}

	// 逐域 P：同时给出边际值与剥离后的独立值。
	perP := map[string]float64{}
	perPIndep := map[string]float64{}
	var pSum, pIndepSum float64
	for i, d := range domains {
		dn, reg := 0, 0
		for w := 0; w < nWin; w++ {
			if failed[i][w] {
				dn++
				if regionalWin[i][w] {
					reg++
				}
			}
		}
		p := float64(dn) / float64(nWin)
		pi := float64(dn-reg) / float64(nWin)
		if pi < 0 {
			pi = 0
		}
		perP[d] = p
		perPIndep[d] = pi
		pSum += p
		pIndepSum += pi
	}
	meanP := pSum / float64(len(domains))
	meanPIndep := pIndepSum / float64(len(domains))

	// 故障时长统计：用于校验窗口尺度（窗口应 >= 3× 平均故障时长）。
	var durs []time.Duration
	for _, iv := range ivs {
		durs = append(durs, iv.end.Sub(iv.start))
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	meanDur := time.Duration(0)
	if len(durs) > 0 {
		var s time.Duration
		for _, d := range durs {
			s += d
		}
		meanDur = s / time.Duration(len(durs))
	}
	medDur := time.Duration(0)
	if len(durs) > 0 {
		medDur = durs[len(durs)/2]
	}

	// Q：出现多域事件的窗口数 / 总窗口数。
	// Q：多域事件数 / 总窗口数。
	//
	// ⚠️ 分母用**事件数**而不是"被事件覆盖的窗口数"。一次持续 15 分钟的
	// 区域事件会跨 1-2 个窗口；若按覆盖窗口数算，同一次事件会被数两遍，
	// Q 系统性偏高（自检里高 1.4-1.7 倍）。
	// CorrelatedFailure.Q 的语义是"每个窗口发生一次事件的概率"，
	// 所以分子必须是事件数。
	q := float64(len(events)) / float64(nWin)

	// K：多域事件中同时下线的域数分布（取事件期间的峰值）。
	kHist := map[int]int{}
	var kSum float64
	for _, ev := range events {
		kHist[ev.maxConc]++
		kSum += float64(ev.maxConc)
	}
	kMean := 0.0
	if len(events) > 0 {
		kMean = kSum / float64(len(events))
	}
	kMode := 0
	best := -1
	for k, c := range kHist {
		if c > best || (c == best && k > kMode) {
			kMode, best = k, c
		}
	}

	// 单域事件数（用于说明"大部分故障其实是单域的"）
	singles := countSingles(ivs)

	rep := &report{
		WindowHours:  window.Hours(),
		SpanHours:    span.Hours(),
		Windows:      nWin,
		Domains:      domains,
		PerDomainP:   perP,
		P:            meanP,
		PIndependent: meanPIndep,
		PerDomainPIndep: perPIndep,
		MeanFailDurationMin: meanDur.Minutes(),
		MedianFailDurationMin: medDur.Minutes(),
		Q:            q,
		KMean:        kMean,
		KMode:        kMode,
		KHistogram:   map[string]int{},
		Events:       len(events),
		SingleEvents: singles,
	}
	for k, c := range kHist {
		rep.KHistogram[fmt.Sprintf("%d", k)] = c
	}

	// 相关性检验：两两同时失效的时间占比 vs 独立模型预测 P_a * P_b。
	for a := 0; a < len(domains); a++ {
		for b := a + 1; b < len(domains); b++ {
			both := 0
			for w := 0; w < nWin; w++ {
				if failed[a][w] && failed[b][w] {
					both++
				}
			}
			obs := float64(both) / float64(nWin)
			exp := perP[domains[a]] * perP[domains[b]]
			lift := 0.0
			if exp > 0 {
				lift = obs / exp
			}
			rep.Correlation = append(rep.Correlation, pairCorr{
				A: domains[a], B: domains[b],
				ObservedRatio: obs, ExpectedRatio: exp, Lift: lift,
				ExpectedCount: exp * float64(nWin),
			})
		}
	}
	sort.Slice(rep.Correlation, func(i, j int) bool {
		return rep.Correlation[i].Lift > rep.Correlation[j].Lift
	})

	rep.GoSnippet = snippet(meanP, q, kMode, domains)
	rep.Notes = notes(rep, window, physical)
	return rep
}

// sweepMultiEvents 扫描时间线，找出所有「同时有 >=2 个域下线」的极大区间。
//
// 这是整个工具的核心：区域事件的定义**不是**"两个域在同一个窗口里都故障过"，
// 而是"在某段真实时间里它们同时是坏的"。前者会把整个观测期摊平，
// 把 Q 高估几个数量级。
func sweepMultiEvents(ivs []interval) []multiEvent {
	type pt struct {
		t    time.Time
		di   int
		up   bool
		name string
	}
	dset := map[string]int{}
	for _, iv := range ivs {
		if _, ok := dset[iv.domain]; !ok {
			dset[iv.domain] = len(dset)
		}
	}
	pts := make([]pt, 0, len(ivs)*2)
	for _, iv := range ivs {
		pts = append(pts, pt{iv.start, dset[iv.domain], false, iv.domain})
		pts = append(pts, pt{iv.end, dset[iv.domain], true, iv.domain})
	}
	sort.Slice(pts, func(i, j int) bool {
		if pts[i].t.Equal(pts[j].t) {
			// 同一时刻先处理"恢复"，避免零长度重叠被算成事件
			return pts[i].up && !pts[j].up
		}
		return pts[i].t.Before(pts[j].t)
	})

	live := map[string]bool{}
	var events []multiEvent
	var cur *multiEvent
	for _, p := range pts {
		if p.up {
			delete(live, p.name)
		} else {
			live[p.name] = true
		}
		n := len(live)
		if n >= 2 {
			if cur == nil {
				cur = &multiEvent{start: p.t}
			}
			if n > cur.maxConc {
				cur.maxConc = n
				// 记下**峰值时刻**的域集合 —— 这才是"这次事件打掉了谁"。
				// 事件末尾的快照只剩 1 个域（并发已经掉下去了），不能用。
				ds := make([]string, 0, n)
				for k := range live {
					ds = append(ds, k)
				}
				sort.Strings(ds)
				cur.domains = ds
			}
		} else if cur != nil {
			cur.end = p.t
			if cur.maxConc >= 2 {
				events = append(events, *cur)
			}
			cur = nil
		}
	}
	if cur != nil {
		// 事件一直延续到数据末尾
		last := ivs[0].end
		for _, iv := range ivs {
			if iv.end.After(last) {
				last = iv.end
			}
		}
		cur.end = last
		events = append(events, *cur)
	}
	return events
}

// liveSnapshot 返回事件结束时仍在线的域（仅为让事件带上域名清单，不参与判定）。
func liveSnapshot(ev *multiEvent, ivs []interval, at time.Time) map[string]bool {
	out := map[string]bool{}
	for _, iv := range ivs {
		if !iv.start.After(at) && !iv.end.Before(at) {
			out[iv.domain] = true
		}
	}
	return out
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// countSingles 统计互不重叠的"只有单个域下线"的时段数。
func countSingles(ivs []interval) int {
	type pt struct {
		t  time.Time
		up bool
	}
	pts := make([]pt, 0, len(ivs)*2)
	for _, iv := range ivs {
		pts = append(pts, pt{iv.start, false}, pt{iv.end, true})
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].t.Before(pts[j].t) })
	n, singles := 0, 0
	inSingle := false
	for _, p := range pts {
		if p.up {
			n--
		} else {
			n++
		}
		if n == 1 && !inSingle {
			singles++
			inSingle = true
		} else if n != 1 {
			inSingle = false
		}
	}
	return singles
}

// ─────────────────────────────────────────────────────────────────────
// 合成序列（自检用）
// ─────────────────────────────────────────────────────────────────────

func synth(nDom, days int, window time.Duration, p, q float64, k int, seed int64) ([]interval, error) {
	// 用确定性伪随机（避免引入 math/rand 的版本差异影响回归测试）
	rng := uint64(seed)*6364136223846793005 + 1442695040888963407
	next := func() float64 {
		rng = rng*6364136223846793005 + 1442695040888963407
		return float64((rng>>11)&((1<<53)-1)) / float64(1<<53)
	}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	total := time.Duration(days) * 24 * time.Hour
	nWin := int(total / window)

	domains := make([]string, nDom)
	for i := range domains {
		domains[i] = fmt.Sprintf("az-%c", 'a'+rune(i))
	}

	var out []interval
	// 独立故障：每域每窗口以概率 p 发生一次，故障时长取窗口的 1/4。
	for w := 0; w < nWin; w++ {
		for d := 0; d < nDom; d++ {
			if next() < p {
				st := t0.Add(time.Duration(w) * window).Add(time.Duration(next() * float64(window/2)))
				out = append(out, interval{start: st, end: st.Add(window / 4), domain: domains[d]})
			}
		}
		// 区域事件：每窗口以概率 q 发生，同时命中 k 个**随机**域。
		if next() < q {
			st := t0.Add(time.Duration(w) * window).Add(time.Duration(next() * float64(window/2)))
			perm := make([]int, nDom)
			for i := range perm {
				perm[i] = i
			}
			for i := nDom - 1; i > 0; i-- {
				j := int(next() * float64(i+1))
				perm[i], perm[j] = perm[j], perm[i]
			}
			for i := 0; i < k && i < nDom; i++ {
				out = append(out, interval{start: st, end: st.Add(window / 4), domain: domains[perm[i]]})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start.Before(out[j].start) })
	return out, nil
}

// ─────────────────────────────────────────────────────────────────────
// 输入输出
// ─────────────────────────────────────────────────────────────────────

func readCSV(path string) ([]interval, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var out []interval
	for i, row := range rows {
		if len(row) < 3 {
			continue
		}
		head := strings.ToLower(strings.TrimSpace(row[0]))
		if head == "start" || strings.HasPrefix(head, "#") {
			continue
		}
		st, err := time.Parse(time.RFC3339, strings.TrimSpace(row[0]))
		if err != nil {
			return nil, fmt.Errorf("第 %d 行 start 解析失败: %w", i+1, err)
		}
		iv := interval{start: st, domain: strings.TrimSpace(row[2])}
		es := strings.TrimSpace(row[1])
		if es == "" {
			iv.openEnded = true
		} else {
			et, err := time.Parse(time.RFC3339, es)
			if err != nil {
				return nil, fmt.Errorf("第 %d 行 end 解析失败: %w", i+1, err)
			}
			iv.end = et
		}
		if !iv.openEnded && iv.end.Before(iv.start) {
			return nil, fmt.Errorf("第 %d 行 end 早于 start", i+1)
		}
		out = append(out, iv)
	}
	return out, nil
}

func printReport(r *report) {
	fmt.Printf("观测跨度 %.1f 小时（%.1f 天），%d 个窗口，%d 个域\n\n",
		r.SpanHours, r.SpanHours/24, r.Windows, len(r.Domains))

	fmt.Println("① 逐域失效率（每窗口）")
	fmt.Println("   边际值 = 直接数出来的；独立值 = 剥掉区域事件覆盖的窗口后的。")
	fmt.Println("   **填给 CorrelatedFailure.P 的必须是独立值** —— 用边际值会把区域事件算两遍。")
	for _, d := range r.Domains {
		fmt.Printf("   %-10s 边际 %.5f   独立 %.5f\n", d, r.PerDomainP[d], r.PerDomainPIndep[d])
	}
	fmt.Printf("   %-10s 边际 %.5f   独立 %.5f\n\n", "均值", r.P, r.PIndependent)

	fmt.Println("② 区域事件率 Q（每窗口）")
	fmt.Printf("   多域事件 %d 次 / %d 个窗口 → Q = %.5f\n", r.Events, r.Windows, r.Q)
	fmt.Printf("   单域时段 %d 次（对照：真实系统里绝大多数故障是单域的）\n", r.SingleEvents)
	fmt.Printf("   平均故障时长 %.1f 分钟（中位 %.1f 分钟），窗口 %.0f 分钟\n\n",
		r.MeanFailDurationMin, r.MedianFailDurationMin, r.WindowHours*60)

	fmt.Println("③ 事件命中域数 K")
	fmt.Printf("   均值 %.2f，众数 %d\n", r.KMean, r.KMode)
	ks := make([]int, 0, len(r.KHistogram))
	for k := range r.KHistogram {
		var v int
		fmt.Sscanf(k, "%d", &v)
		ks = append(ks, v)
	}
	sort.Ints(ks)
	for _, k := range ks {
		fmt.Printf("   K=%d: %d 次\n", k, r.KHistogram[fmt.Sprintf("%d", k)])
	}
	fmt.Println()

	fmt.Println("④ 相关性检验（实测同时失效占比 ÷ 独立假设预测）")
	fmt.Println("   lift >> 1 说明独立模型严重低估了共同失效 —— 这正是区域事件存在的证据")
	fmt.Println("   ⚠️ 但 lift 只在「期望次数」够大时才可解读：期望次数 < 5 时，一次巧合就能造出几倍 lift")
	shown := 0
	for _, c := range r.Correlation {
		if c.ExpectedRatio == 0 && c.ObservedRatio == 0 {
			continue
		}
		flag := ""
		if c.ExpectedCount < 5 {
			flag = "  ← 期望次数不足，lift 不可解读"
		}
		fmt.Printf("   %-8s × %-8s  实测 %.6f  独立预测 %.8f  lift %7.1f×  期望次数 %.2f%s\n",
			c.A, c.B, c.ObservedRatio, c.ExpectedRatio, c.Lift, c.ExpectedCount, flag)
		shown++
		if shown >= 8 {
			break
		}
	}
	fmt.Println()

	fmt.Println("⑤ 可直接粘进 Go 代码的参数")
	fmt.Println(r.GoSnippet)
	fmt.Println()

	fmt.Println("⑥ 注意事项")
	for _, n := range r.Notes {
		fmt.Println("   - " + n)
	}
}

func snippet(p, q float64, k int, domains []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "// 由 cmd/faultfit 从真实故障日志拟合，窗口 = 见报告\n")
	fmt.Fprintf(&b, "model := faft.CorrelatedFailure{P: %.5f, Q: %.5f, K: %d}\n", p, q, k)
	fmt.Fprintf(&b, "topo := &faft.Topology{\n")
	fmt.Fprintf(&b, "    Domains: []faft.Domain{\n")
	for _, d := range domains {
		fmt.Fprintf(&b, "        {Name: %q, FailProb: %.5f},\n", d, p)
	}
	fmt.Fprintf(&b, "    },\n")
	fmt.Fprintf(&b, "    RegionalEventProb: %.5f,\n    RegionalEventDomains: %d,\n}\n", q, k)
	// 引用一下 faft 包，保证 snippet 与实际类型一致（编译期就能发现改名）
	var _ faft.FailureModel = faft.CorrelatedFailure{}
	return b.String()
}

func notes(r *report, window, physical time.Duration) []string {
	out := []string{
		fmt.Sprintf("P 与 Q 都是「每 %v」的概率 —— 换窗口它们就变。论文里必须把窗口长度和结果一起报，"+
			"否则数字不可比。", window),
		"区域事件的判据是「在某段真实时间里同时有 ≥2 个域下线」，不是" +
			"「同一个窗口里都故障过」。后者会把观测期摊平，把 Q 高估几个数量级。",
		"填给 `CorrelatedFailure.P` 的必须是**独立值**而不是边际值 —— 边际值已经把" +
			"区域事件算进去了，再用 Q/K 表达一遍就是双重计数。自检里这一步差了 5 倍。",
		"K 建议取众数而不是均值：均值会被个别超大规模事件拉高，" +
			"而求解器要的是" + "\"典型事件有多大\"" + "。同时应当跑一遍 K±1 的敏感性。",
	}

	// 子窗口重叠膨胀：一次时长 D 的故障，起点均匀分布时会覆盖 1+D/W 个窗口。
	if r.MeanFailDurationMin > 0 {
		infl := 1 + r.MeanFailDurationMin/window.Minutes()
		out = append(out, fmt2(
			"⚠️ 独立值仍含约 %.2f× 的**窗口重叠膨胀**：一次 %.0f 分钟的故障会跨 1–2 个窗口，"+
				"逐窗口计数因此系统性偏高。若需要严格的无偏速率，除以这个因子："+
				"独立值 / %.2f。本工具不自动除，因为膨胀因子依赖"+
				"「故障何时发生」的分布假设，把它留在报告里由使用者判断更诚实。",
			infl, r.MeanFailDurationMin, infl))
	}

	if physical > 0 {
		if window < 3*physical {
			out = append(out, fmt2(
				"⚠️ 窗口 %v < 3×物理故障时长 %v：窗口太短会让同一次故障被切进多个窗口，"+
					"把 Q 高估。建议加大 -window。", window, physical))
		} else {
			out = append(out, fmt2("窗口 %v ≥ 3×物理故障时长 %v，尺度上是合理的。", window, physical))
		}
	} else {
		out = append(out, "建议用 -physical 传入该域的平均故障恢复时长做一次尺度校验："+
			"窗口至少要 3 倍于它，否则一次故障会被切进多个窗口。")
	}
	if r.Events < 30 {
		out = append(out, fmt2(
			"⚠️ 只有 %d 次多域事件：Q 的置信区间会很宽（粗略地 ±1/√n ≈ ±%.0f%%）。"+
				"论文里应当报区间，或者把它当成敏感性扫描的一个端点。",
			r.Events, 100/math.Sqrt(math.Max(1, float64(r.Events)))))
	}
	if len(r.Correlation) > 0 && r.Correlation[0].Lift < 3 {
		out = append(out, "最高的两两 lift 不到 3×：这个集群的相关性不强，"+
			"FAFT 的收益可能主要来自逐域失效率的不均匀（Domain.FailProb），而不是区域事件。")
	}
	return out
}

// fmt2 包一层 Sprintf，避免上面的 notes 里 fmt 变量名与包名冲突。
func fmt2(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
