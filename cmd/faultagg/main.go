// cmd/faultagg —— 把**节点级**故障记录聚合成**域级**故障区间。
//
// ── 为什么需要它 ─────────────────────────────────────────────────────
// 监控系统（node_exporter / kube-state-metrics / DCGM / BMC）导出来的都是
// **节点级**事件："node-1234 NotReady"、"GPU 掉卡"、"整机重启"。
//
// 而 cmd/faultfit 要的是**域级**故障："az-a 整体不可用"。
//
// 中间这一步是整条链路上**唯一带主观判据**的地方，也是整篇论文里
// 最容易被质疑的一处：什么算"一个域挂了"？
//   - "域里有 3 个节点挂了"？—— 那会随集群规模漂移
//   - "域里超过一半节点挂了"？—— 尺度无关，但要选阈值
//   - "域里的训练作业全断了"？—— 最贴近业务，但要有作业层数据
//
// 本工具把判据**显式参数化**（-threshold / -minduration / -mergegap），
// 并要求把取值写进论文。判据错了，后面拟合出来的 P/Q/K 全废。
//
// ── 输入格式 ─────────────────────────────────────────────────────────
// 节点级故障 CSV，带表头，列顺序固定：
//
//	node,domain,start,end,kind
//	node-0001,az-a,2026-01-01T03:12:00Z,2026-01-01T03:19:30Z,unplanned
//	node-0002,az-a,2026-01-01T03:12:30Z,2026-01-01T03:18:00Z,unplanned
//
//   - kind：planned / unplanned。滚动升级、机房割接必须标 planned，
//     否则会混进故障率里把 P 抬高。
//   - end 留空表示延续到数据末尾。
//
// 域内**总节点数**由数据自动推断（该域在整个文件里出现过的不同 node 数）。
// 如果某些节点从没故障过、因而没有出现在输入里，用 -total 显式指定
// 每域总节点数（格式 az-a=64,az-b=64,...）—— 这是个容易踩的坑：
// 用"出现过的节点数"当分母会**系统性高估**故障比例。
//
// ── 输出 ─────────────────────────────────────────────────────────────
// 域级故障区间 CSV，可直接喂给 cmd/faultfit：
//
//	start,end,domain
//
// ── 自检 ─────────────────────────────────────────────────────────────
// -sample 会反向构造：按已知的域级故障区间展开成节点级记录，
// 再跑一遍聚合，检查能不能还原回原来的域级区间。
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type nodeOutage struct {
	node, domain string
	start, end   time.Time
	openEnded    bool
	planned      bool
}

type domInterval struct {
	domain     string
	start, end time.Time
}

func main() {
	in := flag.String("in", "", "节点级故障 CSV（node,domain,start,end,kind）")
	out := flag.String("out", "", "输出的域级故障 CSV（start,end,domain）；留空则打到 stdout")
	threshold := flag.Float64("threshold", 0.5, "域内多少比例的节点同时不可用算作「域级故障」")
	minDur := flag.Duration("minduration", time.Minute, "域级故障的最短持续时间，滤掉瞬时抖动")
	mergeGap := flag.Duration("mergegap", 2*time.Minute, "相邻域级故障间隔小于此值则合并（避免一次故障被切成多段）")
	includePlanned := flag.Bool("include-planned", false, "把 planned 维护也算作故障（默认不算）")
	total := flag.String("total", "", "每域总节点数，如 az-a=64,az-b=64。不填则由数据推断（**可能高估故障比例**）")
	sample := flag.Bool("sample", false, "用合成数据自检：域级 → 展开成节点级 → 再聚合，检查能否还原")
	sampleOut := flag.String("sample-out", "", "自检时把合成的**节点级**记录写成 CSV，用于跑完整链路验证")
	flag.Parse()

	if *sample {
		if err := selfTest(*threshold, *minDur, *mergeGap, *sampleOut); err != nil {
			fmt.Fprintf(os.Stderr, "自检失败: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *in == "" {
		fmt.Fprintln(os.Stderr, "必须给 -in <csv>，或用 -sample 自检")
		os.Exit(2)
	}
	outages, err := readNodeOutages(*in, *includePlanned)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读 %s 失败: %v\n", *in, err)
		os.Exit(1)
	}
	totalMap, err := parseTotal(*total)
	if err != nil {
		fmt.Fprintf(os.Stderr, "-total 解析失败: %v\n", err)
		os.Exit(1)
	}

	tMin, tMax := span(outages)
	fmt.Printf("输入 %d 条节点级故障，%d 个域，观测跨度 %.1f 天\n",
		len(outages), countDomains(outages), tMax.Sub(tMin).Hours()/24)

	ivs, err := aggregate(outages, totalMap, *threshold, *minDur, *mergeGap, tMin, tMax)
	if err != nil {
		fmt.Fprintf(os.Stderr, "聚合失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("聚合出 %d 段域级故障（阈值 %.0f%%，最短 %v，合并间隔 %v）\n",
		len(ivs), *threshold*100, *minDur, *mergeGap)

	if err := writeDomainCSV(*out, ivs); err != nil {
		fmt.Fprintf(os.Stderr, "写出失败: %v\n", err)
		os.Exit(1)
	}
	if *out != "" {
		fmt.Printf("→ %s\n\n下一步：go run ./cmd/faultfit -in %s -window 6h\n", *out, *out)
	}
}

// ─────────────────────────────────────────────────────────────────────
// 聚合
// ─────────────────────────────────────────────────────────────────────

// aggregate 的核心：对每个域做一次时间线扫描，标出"不可用节点占比 ≥ 阈值"
// 的区间，再合并相邻区间、丢掉过短的。
func aggregate(outages []nodeOutage, totalMap map[string]int,
	threshold float64, minDur, mergeGap time.Duration,
	tMin, tMax time.Time) ([]domInterval, error) {

	byDomain := map[string][]nodeOutage{}
	nodesInDomain := map[string]map[string]bool{}
	for i := range outages {
		o := &outages[i]
		if o.openEnded {
			o.end = tMax
		}
		if !o.end.After(o.start) {
			continue // 零长度，忽略
		}
		byDomain[o.domain] = append(byDomain[o.domain], *o)
		if nodesInDomain[o.domain] == nil {
			nodesInDomain[o.domain] = map[string]bool{}
		}
		nodesInDomain[o.domain][o.node] = true
	}

	domains := make([]string, 0, len(byDomain))
	for d := range byDomain {
		domains = append(domains, d)
	}
	sort.Strings(domains)

	var out []domInterval
	for _, d := range domains {
		totalNodes := len(nodesInDomain[d])
		if n, ok := totalMap[d]; ok && n > 0 {
			totalNodes = n
		}
		if totalNodes == 0 {
			continue
		}
		// 需要多少个节点同时挂才算域级故障。至少 1 个，至多 totalNodes。
		need := int(threshold*float64(totalNodes) + 0.999999)
		if need < 1 {
			need = 1
		}
		if need > totalNodes {
			need = totalNodes
		}

		type ev struct {
			t    time.Time
			down bool
			node string
		}
		evs := make([]ev, 0, len(byDomain[d])*2)
		for _, o := range byDomain[d] {
			evs = append(evs, ev{o.start, true, o.node}, ev{o.end, false, o.node})
		}
		sort.Slice(evs, func(i, j int) bool {
			if evs[i].t.Equal(evs[j].t) {
				// 同一时刻先处理恢复，避免零长度重叠被算进去
				return !evs[i].down && evs[j].down
			}
			return evs[i].t.Before(evs[j].t)
		})

		live := map[string]bool{}
		var raw []domInterval
		var curStart time.Time
		inFault := false
		for _, e := range evs {
			if e.down {
				live[e.node] = true
			} else {
				delete(live, e.node)
			}
			now := len(live) >= need
			if now && !inFault {
				curStart = e.t
				inFault = true
			} else if !now && inFault {
				raw = append(raw, domInterval{d, curStart, e.t})
				inFault = false
			}
		}
		if inFault {
			raw = append(raw, domInterval{d, curStart, tMax})
		}

		// 合并 + 过滤
		merged := mergeIntervals(raw, mergeGap)
		for _, iv := range merged {
			if iv.end.Sub(iv.start) >= minDur {
				out = append(out, iv)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].start.Equal(out[j].start) {
			return out[i].domain < out[j].domain
		}
		return out[i].start.Before(out[j].start)
	})
	return out, nil
}

func mergeIntervals(in []domInterval, gap time.Duration) []domInterval {
	if len(in) == 0 {
		return nil
	}
	sort.Slice(in, func(i, j int) bool { return in[i].start.Before(in[j].start) })
	out := []domInterval{in[0]}
	for _, iv := range in[1:] {
		last := &out[len(out)-1]
		if !iv.start.After(last.end.Add(gap)) {
			if iv.end.After(last.end) {
				last.end = iv.end
			}
			continue
		}
		out = append(out, iv)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────
// 自检：域级 → 节点级 → 域级，检查能否还原
// ─────────────────────────────────────────────────────────────────────

func selfTest(threshold float64, minDur, mergeGap time.Duration, sampleOut string) error {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 构造已知的域级故障区间
	want := []domInterval{
		{"az-a", t0.Add(3 * time.Hour), t0.Add(3*time.Hour + 20*time.Minute)},
		{"az-c", t0.Add(5 * time.Hour), t0.Add(5*time.Hour + 40*time.Minute)},
		{"az-b", t0.Add(5*time.Hour + 10*time.Minute), t0.Add(5*time.Hour + 35*time.Minute)}, // 与 az-c 重叠 → 一次区域事件
		{"az-d", t0.Add(30 * time.Hour), t0.Add(30*time.Hour + 15*time.Minute)},
	}
	totalPerDomain := 64
	rng := rand.New(rand.NewSource(7))

	// 展开成节点级：域级故障期间，域内**绝大多数**节点不可用
	//（不是全部 —— 真实世界总有几个节点是好的，这正是阈值判据存在的理由）
	var nodes []nodeOutage
	for _, iv := range want {
		nDown := int(float64(totalPerDomain) * 0.85) // 85% 的节点挂掉
		idx := rng.Perm(totalPerDomain)[:nDown]
		for _, i := range idx {
			// 每个节点的起止略有抖动，模拟真实的不同步
			jitterStart := time.Duration(rng.Intn(60)) * time.Second
			jitterEnd := time.Duration(rng.Intn(120)) * time.Second
			nodes = append(nodes, nodeOutage{
				node:   fmt.Sprintf("node-%04d", i),
				domain: iv.domain,
				start:  iv.start.Add(jitterStart),
				end:    iv.end.Add(-jitterEnd),
			})
		}
	}
	tMax := t0.Add(48 * time.Hour)

	// 把合成的节点级记录落盘，用来跑完整链路：
	//   faultagg -sample -sample-out nodes.csv
	//   faultagg -in nodes.csv -out domains.csv
	//   faultfit -in domains.csv -window 6h
	if sampleOut != "" {
		if err := writeNodeCSV(sampleOut, nodes, tMax); err != nil {
			return err
		}
		fmt.Printf("  已写出合成的节点级记录：%s（%d 条）\n", sampleOut, len(nodes))
	}

	totalMap := map[string]int{}
	for _, d := range []string{"az-a", "az-b", "az-c", "az-d"} {
		totalMap[d] = totalPerDomain
	}
	got, err := aggregate(nodes, totalMap, threshold, minDur, mergeGap, t0, tMax)
	if err != nil {
		return err
	}

	fmt.Printf("自检：注入 %d 段域级故障 → 展开成 %d 条节点级记录 → 再聚合\n", len(want), len(nodes))
	fmt.Println("  期望（注入的真值）                实际（聚合还原）              边界偏差")

	// ⚠️ 这里**不能**要求逐条精确相等，而且偏差是有方向的：
	// 聚合还原出的区间会**比真值短**。原因很直白 —— 节点的恢复时间是错开的，
	// 域级故障在"不足阈值的节点仍在线"时就算结束，而真值里的 end 是
	// 最后一个节点恢复的时刻。本自检里给每个节点加了 0–120 秒的恢复抖动，
	// 于是还原出的 end 会早 1–2 分钟。
	//
	// 这不是缺陷，是判据的固有含义：**聚合出来的是"影响面达标的时段"，
	// 不是"最后一个字节恢复的时刻"**。论文里必须把这个口径写清楚。
	tol := 5 * time.Minute
	ok := len(got) == len(want)
	used := make([]bool, len(got))
	for _, w := range want {
		best := -1
		var bestOverlap time.Duration
		for i, g := range got {
			if used[i] || g.domain != w.domain {
				continue
			}
			ov := overlap(w, g)
			if ov > bestOverlap {
				bestOverlap, best = ov, i
			}
		}
		if best < 0 {
			fmt.Printf("  ✗ %-30s （未匹配到）\n", describe(w))
			ok = false
			continue
		}
		used[best] = true
		g := got[best]
		ds := g.start.Sub(w.start)
		de := g.end.Sub(w.end)
		good := absDur(ds) <= tol && absDur(de) <= tol
		mark := "✓ "
		if !good {
			mark = "✗ "
			ok = false
		}
		fmt.Printf("  %s%-30s %-28s 起 %+5.1f 分 / 止 %+5.1f 分\n",
			mark, describe(w), describe(g), ds.Minutes(), de.Minutes())
	}
	if !ok {
		return fmt.Errorf("聚合结果与注入值偏差超过容差 %v", tol)
	}
	fmt.Printf("  自检通过：往返还原成功，边界偏差均在 %v 以内"+
		"（且方向固定为「还原出的区间更短」，见代码注释）\n", tol)
	return nil
}

// overlap 返回两个区间的时间重叠长度。
func overlap(a, b domInterval) time.Duration {
	start := a.start
	if b.start.After(start) {
		start = b.start
	}
	end := a.end
	if b.end.Before(end) {
		end = b.end
	}
	if end.Before(start) {
		return 0
	}
	return end.Sub(start)
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func describe(iv domInterval) string {
	return fmt.Sprintf("%s %s–%s", iv.domain,
		iv.start.Format("01-02 15:04"), iv.end.Format("15:04"))
}

// ─────────────────────────────────────────────────────────────────────
// IO
// ─────────────────────────────────────────────────────────────────────

func readNodeOutages(path string, includePlanned bool) ([]nodeOutage, error) {
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
	var out []nodeOutage
	for i, row := range rows {
		if len(row) < 5 {
			continue
		}
		head := strings.ToLower(strings.TrimSpace(row[0]))
		if head == "node" || strings.HasPrefix(head, "#") {
			continue
		}
		st, err := time.Parse(time.RFC3339, strings.TrimSpace(row[2]))
		if err != nil {
			return nil, fmt.Errorf("第 %d 行 start 解析失败: %w", i+1, err)
		}
		o := nodeOutage{
			node:   strings.TrimSpace(row[0]),
			domain: strings.TrimSpace(row[1]),
			start:  st,
		}
		es := strings.TrimSpace(row[3])
		if es == "" {
			o.openEnded = true
		} else {
			et, err := time.Parse(time.RFC3339, es)
			if err != nil {
				return nil, fmt.Errorf("第 %d 行 end 解析失败: %w", i+1, err)
			}
			o.end = et
		}
		kind := strings.ToLower(strings.TrimSpace(row[4]))
		o.planned = kind == "planned" || kind == "maintenance" || kind == "planned_maintenance"
		if o.planned && !includePlanned {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

// writeNodeCSV 写出合成的节点级记录，格式与 -in 的输入格式一致，
// 这样"生成 → 聚合 → 拟合"能走完整条真实的数据路径。
func writeNodeCSV(path string, nodes []nodeOutage, tMax time.Time) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"node", "domain", "start", "end", "kind"}); err != nil {
		return err
	}
	for _, o := range nodes {
		end := o.end
		if o.openEnded {
			end = tMax
		}
		if err := w.Write([]string{
			o.node, o.domain,
			o.start.UTC().Format(time.RFC3339),
			end.UTC().Format(time.RFC3339),
			"unplanned",
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeDomainCSV(path string, ivs []domInterval) error {	var w *csv.Writer
	var f *os.File
	if path == "" {
		w = csv.NewWriter(os.Stdout)
	} else {
		var err error
		f, err = os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		w = csv.NewWriter(f)
	}
	if err := w.Write([]string{"start", "end", "domain"}); err != nil {
		return err
	}
	for _, iv := range ivs {
		if err := w.Write([]string{
			iv.start.UTC().Format(time.RFC3339),
			iv.end.UTC().Format(time.RFC3339),
			iv.domain,
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func parseTotal(s string) (map[string]int, error) {
	out := map[string]int{}
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("期望 domain=count，得到 %q", part)
		}
		n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("%q 的计数不是整数", part)
		}
		out[strings.TrimSpace(kv[0])] = n
	}
	return out, nil
}

func span(o []nodeOutage) (time.Time, time.Time) {
	if len(o) == 0 {
		now := time.Now()
		return now, now
	}
	mn, mx := o[0].start, o[0].end
	for _, x := range o {
		if x.start.Before(mn) {
			mn = x.start
		}
		e := x.end
		if x.openEnded {
			e = x.start
		}
		if e.After(mx) {
			mx = e
		}
	}
	return mn, mx
}

func countDomains(o []nodeOutage) int {
	s := map[string]bool{}
	for _, x := range o {
		s[x.domain] = true
	}
	return len(s)
}
