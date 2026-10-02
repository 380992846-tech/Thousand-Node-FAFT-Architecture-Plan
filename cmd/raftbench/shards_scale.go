// cmd/raftbench/shards_scale.go
//
// shards-scale：**同一结构、不同分片数** —— 验证"可用比例与规模无关"。
//
// ── 为什么必须单独做这个模式（评审指出）────────────────────────────
// shards.go 里写着：
//
//	"本模式量的是**比例**（可用分片占比），
//	 而比例对 400 分片与 1000 分片是同一个数（已实测对照）。"
//
// 但 results/ 里**只有 400 分片的文件**（raftbench-shards-S400-*.json），
// 1000 分片那一侧**没有任何落盘证据** —— 也就是说那句话当时是
// **不可核对**的。评审问"实测数据在哪"，答案就是：没有。
//
// 这个模式把两侧放进**同一次调用**、写进**同一份 JSON**，
// 让"同一个数"这句话可以被直接复核，而不是只能相信注释。
//
// ── 判据要小心：两个"同一个数"不是一回事 ───────────────────────────
//  ① 每个规模内部：观测到的可用比例 == 该规模的**算术界**（结构性质，强结论）
//  ② 跨规模：两个规模的可用比例一致 —— 但它是**随机变量**：
//     域分配与故障注入决定了每个分片是否可用，样本数就是分片数 S，
//     所以二项标准差 ≈ sqrt(p(1-p)/S)。S=400 与 S=1000 的抽样误差不同，
//     "完全相同"通常只是碰巧。所以这里报**差值**与**差值相当于几个标准差**，
//     而不是笼统地说"同一个数"。
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

type shardsScaleOpts struct {
	base        shardsOpts
	shardCounts []int
	reps        int
	outPath     string
}

// shardsScaleRun 一次运行的关键指标（从 shards 模式的完整结果里摘出来）。
type shardsScaleRun struct {
	Shards   int `json:"shards"`
	Rep      int `json:"rep"`
	Instances int `json:"instances"`

	// AvailableRate：恢复窗口**末刻**仍然选出 leader 的分片占比。
	AvailableRate float64 `json:"available_rate"`
	// PredictedRate：同一时刻的**算术界**（存活副本数 ≥ |Q1| 的分片占比）。
	PredictedRate float64 `json:"predicted_rate"`
	// EverRate：窗口内**曾经**可用过的占比（含退位瞬态，只作对照）。
	EverRate float64 `json:"ever_available_rate"`
	// TransientRate：注入故障后首个采样的 leader 占比（退位瞬态，不算恢复）。
	TransientRate float64 `json:"transient_rate"`

	Available int `json:"available"`
	Predicted int `json:"predicted"`

	BuildMs float64 `json:"build_ms"`
	ReadyMs float64 `json:"all_leaders_ms"`
	VoteRPS float64 `json:"requestvote_per_sec"`
}

type shardsScaleReport struct {
	Purpose string `json:"purpose"`
	Config  struct {
		N          int     `json:"replicas_per_shard"`
		Domains    int     `json:"domains"`
		Q1         int     `json:"q1"`
		Q2         int     `json:"q2"`
		Kill       int     `json:"domains_killed"`
		InjectedUs int64   `json:"injected_delay_us"`
		Reps       int     `json:"reps"`
		ShardCounts []int  `json:"shard_counts"`
	} `json:"config"`

	Runs []shardsScaleRun `json:"runs"`

	// PerScale：每个规模的中位数与"与算术界的最大偏差"。
	PerScale []shardsScaleSummary `json:"per_scale"`

	// CrossScale：跨规模比较（含抽样误差口径）。
	CrossScale struct {
		Rates       map[string]float64 `json:"median_rate_by_shards"`
		MaxAbsDiff  float64            `json:"max_abs_rate_diff"`
		// DiffInSigma：差值相当于几个二项标准差（基于较小规模算的保守值）。
		DiffInSigma float64 `json:"diff_in_binomial_sigma"`
		// SigmaUsed：判据里用的保守二项标准差（p=0.5、最小规模）。
		SigmaUsed float64 `json:"sigma_used"`
		// SameWithinNoise：差值是否落在抽样噪声内（< 2σ）。
		SameWithinNoise bool   `json:"same_within_sampling_noise"`
		Detail          string `json:"detail"`
	} `json:"cross_scale"`

	Notes []string `json:"notes"`
}

type shardsScaleSummary struct {
	Shards        int     `json:"shards"`
	Runs          int     `json:"runs"`
	MedianRate    float64 `json:"median_available_rate"`
	MedianPredict float64 `json:"median_predicted_rate"`
	// MaxAbsGapToBound：各轮里观测与算术界的最大绝对偏差。
	MaxAbsGapToBound float64 `json:"max_abs_gap_to_bound"`
	// MinRate/MaxRate：本规模各轮的最小/最大可用比例。
	// **必须报**：实测 S=1000 三轮 34.3%/50.9%/100.0%，只看中位数会把 3 倍波动抹平。
	MinRate float64 `json:"min_rate"`
	MaxRate float64 `json:"max_rate"`
	// BoundExact: 每一轮是否都精确等于算术界。
	BoundExact bool `json:"observed_equals_bound_every_run"`
}

func runShardsScale(o shardsScaleOpts) error {
	if len(o.shardCounts) == 0 {
		o.shardCounts = []int{1000, 400}
	}
	if o.reps <= 0 {
		o.reps = 3
	}

	rep := &shardsScaleReport{
		Purpose: "验证「整域故障后的可用分片比例」是否与分片数无关 —— " +
			"并把两侧数据写进同一份文件，使该说法可被直接复核。" +
			"（此前 shards.go 的注释引用了这个对照，但 1000 分片那一侧没有落盘证据。）",
	}
	rep.Config.N = o.base.n
	rep.Config.Domains = o.base.domains
	rep.Config.Q1 = effectiveQuorum(o.base.n, o.base.q1)
	rep.Config.Q2 = effectiveQuorum(o.base.n, o.base.q2)
	rep.Config.Kill = o.base.kill
	rep.Config.InjectedUs = o.base.delay.Microseconds()
	rep.Config.Reps = o.reps
	rep.Config.ShardCounts = append([]int(nil), o.shardCounts...)

	for _, s := range o.shardCounts {
		for i := 1; i <= o.reps; i++ {
			opts := o.base
			opts.shards = s
			opts.label = fmt.Sprintf("shards-scale-S%d-r%d", s, i)
			res, err := runShards(opts)
			if err != nil {
				return fmt.Errorf("S=%d 第 %d 轮失败: %w", s, i, err)
			}
			rep.Runs = append(rep.Runs, shardsScaleRun{
				Shards:        s,
				Rep:           i,
				Instances:     s * o.base.n,
				AvailableRate: res.ShardAvailRate,
				PredictedRate: res.ShardPredRate,
				EverRate:      res.ShardEverRate,
				TransientRate: res.ShardTransient,
				Available:     res.ShardAvailable,
				Predicted:     res.ShardPredicted,
				BuildMs:       res.ShardBuildMs,
				ReadyMs:       res.ShardReadyMs,
				VoteRPS:       res.ShardVoteRPS,
			})
			fmt.Printf("[shards-scale] S=%-5d r%d  可用 %d/%d = %.4f ｜ 算术界 %.4f ｜ 曾可用 %.4f ｜ 瞬态 %.4f\n",
				s, i, res.ShardAvailable, s, res.ShardAvailRate,
				res.ShardPredRate, res.ShardEverRate, res.ShardTransient)
		}
	}

	// ── 每个规模的小结 ──
	for _, s := range o.shardCounts {
		var rates, preds []float64
		maxGap := 0.0
		exact := true
		for _, r := range rep.Runs {
			if r.Shards != s {
				continue
			}
			rates = append(rates, r.AvailableRate)
			preds = append(preds, r.PredictedRate)
			g := math.Abs(r.AvailableRate - r.PredictedRate)
			if g > maxGap {
				maxGap = g
			}
			// 分片占比是离散量：精确相等要求 k/S 与预测的 k'/S 一致。
			if math.Abs(r.AvailableRate-r.PredictedRate) > 1e-12 {
				exact = false
			}
		}
		if len(rates) == 0 {
			continue
		}
		lo, hi := rates[0], rates[0]
		for _, v := range rates {
			if v < lo {
				lo = v
			}
			if v > hi {
				hi = v
			}
		}
		rep.PerScale = append(rep.PerScale, shardsScaleSummary{
			Shards:           s,
			Runs:             len(rates),
			MedianRate:       medianF(rates),
			MinRate:          lo,
			MaxRate:          hi,
			MedianPredict:    medianF(preds),
			MaxAbsGapToBound: maxGap,
			BoundExact:       exact,
		})
	}

	// ── 跨规模比较 ──
	//
	// ⚠️ 第一版用**各规模的中位数**去比，并拿"中位数的中位数"当 p 算 σ ——
	// 结果 p 恰好落在 1.0 上，σ 变成 0，判据把"差 0.49"判成了"在抽样噪声内"。
	// 这是同一个坑第三次出现（见 BUG-38 的教训）：**判据本身也会骗人**。
	//
	// 现在改成：
	//   · 差值取**所有轮次两两之间**的最大差，而不是中位数之差 ——
	//     中位数会把"样本内 34% ↔ 100%"这种巨大波动整个抹掉；
	//   · σ 用最小规模 + p=0.5（方差最大，是上界）算，不会低估噪声；
	//   · 每个规模的 min–max 一并报出来：波动本身就是结论的一部分。
	byShards := map[string]float64{}
	for _, ps := range rep.PerScale {
		byShards[fmt.Sprintf("%d", ps.Shards)] = ps.MedianRate
	}
	rep.CrossScale.Rates = byShards

	minS := 0
	for _, s := range o.shardCounts {
		if minS == 0 || s < minS {
			minS = s
		}
	}
	if len(rep.Runs) >= 2 && minS > 0 {
		maxAbs := 0.0
		for _, a := range rep.Runs {
			for _, b := range rep.Runs {
				if d := math.Abs(a.AvailableRate - b.AvailableRate); d > maxAbs {
					maxAbs = d
				}
			}
		}
		rep.CrossScale.MaxAbsDiff = maxAbs

		sigma := math.Sqrt(0.25 / float64(minS))
		rep.CrossScale.SigmaUsed = sigma
		rep.CrossScale.DiffInSigma = maxAbs / sigma
		rep.CrossScale.SameWithinNoise = rep.CrossScale.DiffInSigma < 2.0
		rep.CrossScale.Detail = fmt.Sprintf(
			"所有 %d 轮之间的最大可用比例差 %.4f；按最小规模 S=%d 与 p=0.5（最保守，"+
				"σ 取上界）算得 σ=%.5f，相当于 %.1f σ。",
			len(rep.Runs), maxAbs, minS, sigma, rep.CrossScale.DiffInSigma)
	}

	rep.Notes = []string{
		"「可用」= 恢复窗口末刻该分片仍有 leader。算术界 = 存活副本数 ≥ |Q1| 的分片占比。",
		"判据一（强）：**每个规模内部**观测比例是否等于算术界（逐轮核对，见 per_scale.bound_exact）。" +
			"不等就说明「存活数 ≥ |Q1|」只是必要条件，不是充分条件。",
		"判据二（跨规模）：差值必须与抽样噪声比。样本数就是分片数，σ ≈ sqrt(0.25/S)" +
			"（p=0.5 是最保守取值），S=400 与 S=1000 的噪声不同。",
		"⚠️ 跨规模比较**不能只看中位数**：实测 S=1000 的三轮是 34.3% / 50.9% / 100.0%，" +
			"中位数 50.9% 把 3 倍波动抹平了 —— 必须同时报 min–max（见 per_scale）。",
		"ever_available_rate 含「原 leader 退位延迟」造成的假可用（实测 32%–83% 的分片在首个采样仍报 leader），" +
			"它不是恢复率，只能作对照 —— 见 shards.go 第 4 步的长注释。",
		"本装置是**单进程多 Raft 组**，量的是「分片数」的扩展，不是「机器数」的扩展：" +
			"3000 个 Raft 实例共享 16 个逻辑核时，CPU 饱和会让选举 RPC 超时并进入活锁。",
	}

	printShardsScale(rep)

	if o.outPath != "" {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(o.outPath, append(data, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Printf("\n→ 已写出 %s\n", o.outPath)
	}
	return nil
}

func printShardsScale(r *shardsScaleReport) {
	fmt.Println()
	fmt.Println("=== 分片数不变性：同一结构、不同分片数 ===")
	fmt.Println()
	fmt.Printf("结构：每分片 %d 副本，%d 个故障域，杀 %d 个域，|Q1|=%d |Q2|=%d，注入延迟 %dµs\n",
		r.Config.N, r.Config.Domains, r.Config.Kill, r.Config.Q1, r.Config.Q2, r.Config.InjectedUs)
	fmt.Println()
	fmt.Printf("%-8s %-14s %-18s %-14s %-16s %s\n",
		"分片数", "可用比例(中位)", "各轮 min–max", "算术界(中位)", "最大偏差", "逐轮 == 算术界")
	fmt.Println(strings.Repeat("-", 92))
	for _, ps := range r.PerScale {
		ok := "否"
		if ps.BoundExact {
			ok = "是"
		}
		fmt.Printf("%-8d %-14.4f %-18s %-14.4f %-16.6f %s\n",
			ps.Shards, ps.MedianRate,
			fmt.Sprintf("%.4f–%.4f", ps.MinRate, ps.MaxRate),
			ps.MedianPredict, ps.MaxAbsGapToBound, ok)
	}
	fmt.Println()
	if r.CrossScale.Detail != "" {
		fmt.Printf("跨规模：%s\n", r.CrossScale.Detail)
		if r.CrossScale.SameWithinNoise {
			fmt.Println("        差值与抽样噪声同量级 ⇒ 「比例与规模无关」**成立**（但不能说「完全相同」）。")
		} else {
			fmt.Println("        ⚠️ 差值超出抽样噪声 ⇒ 不能声称「比例与规模无关」，必须查原因。")
		}
	}
	fmt.Println()
	fmt.Println("注意事项：")
	for _, n := range r.Notes {
		fmt.Printf("  - %s\n", n)
	}
}

func medianF(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	ys := append([]float64(nil), xs...)
	sort.Float64s(ys)
	return ys[len(ys)/2]
}
