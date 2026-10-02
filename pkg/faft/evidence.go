package faft

import "fmt"

// Evidence 证据等级。
//
// 为什么需要显式分级：本项目同时存在三类数字，它们的可主张强度差别极大，
// 混用会让论文站不住。写作与复现时都必须能一眼看出某个数字属于哪一级。
//
// 本项目在文档中曾把解析计算结果表述为"本项目实测"——那是错的。
// 这个枚举的存在就是为了防止再犯：任何引用 faft 数字的地方，
// 都必须同时说明它是 Analysis 级。
type Evidence int

const (
	// EvidenceMeasured 真实系统实测。可以作绝对性能声明。
	// 产出位置：cmd/kvbench -> results/*.json
	EvidenceMeasured Evidence = iota + 1

	// EvidenceAnalysis 解析计算（组合概率、成本模型、约束求解）。
	// 只能作**成本界与结构性质**的声明，不能作性能声明。
	// 产出位置：pkg/faft、pkg/model
	EvidenceAnalysis

	// EvidenceSimulated 离散事件模拟，且经实测校准。
	// 只能作**趋势**声明；若校准未通过，还须标注偏离规模点。
	// 产出位置：pkg/sim
	EvidenceSimulated

	// EvidenceDerived 本项目自行推导的模型（非引用文献，也非实测）。
	// 必须在论文中同样标注为推导。
	EvidenceDerived
)

// String 实现 fmt.Stringer。
func (e Evidence) String() string {
	switch e {
	case EvidenceMeasured:
		return "MEASURED(实测)"
	case EvidenceAnalysis:
		return "ANALYSIS(解析)"
	case EvidenceSimulated:
		return "SIMULATED(模拟)"
	case EvidenceDerived:
		return "DERIVED(推导)"
	}
	return "UNKNOWN"
}

// Claim 一条可主张的结论，附带其证据等级与产出位置。
//
// 设计意图：把"这句话有多硬"变成数据结构而不是散文。
// 论文写作时逐条核对，就不会把解析结果写成实测。
type Claim struct {
	// Statement 结论本身。
	Statement string `json:"statement"`
	// Evidence 证据等级。
	Evidence Evidence `json:"evidence"`
	// Where 产出位置（文件路径或命令）。
	Where string `json:"where"`
	// CanClaim 该等级下**可以**主张什么。
	CanClaim string `json:"can_claim"`
	// CannotClaim 该等级下**不可以**主张什么。
	CannotClaim string `json:"cannot_claim"`
}

// EvidenceManifest 返回本项目主要结论及其证据等级的清单。
//
// 用法：论文写作与审稿自查时逐条过一遍。
func EvidenceManifest() []Claim {
	return []Claim{
		{
			Statement: "readIndex 语义把读 p50 从 39.13ms 降到 4.09ms；put p50 从 99.24ms 降到 17.72ms",
			Evidence:  EvidenceMeasured,
			Where:     "cmd/kvbench -> results/readheavy-after.json, results/writeheavy.json",
			CanClaim:  "在这台机器、这个配置下的绝对性能改善",
			CannotClaim: "在其他硬件/网络/副本数下的绝对性能 —— 本机 fsync 波动大（同配置不同轮次 4.6~8.1ms/op），" +
				"绝对数字不可跨轮次比较",
		},
		{
			Statement: "弹性 quorum 把 n=1000 的稳态写消息从 1002 压到 4（167 倍），" +
				"代价是选举需 999/1000 副本在线、控制路径可用性从 ~1.0 降到 0.9953",
			Evidence: EvidenceAnalysis,
			Where:    "pkg/faft/analysis.go: ComputeControlCost; cmd/faftbench cost",
			CanClaim: "在既定故障模型与闭式下界下的**结构性质**：消息数、门槛与可用性的定量关系",
			CannotClaim: "任何性能声明。这是组合概率与消息计数，不是测出来的吞吐或延迟",
		},
		{
			Statement: "域感知的成员选择在同样 quorum 大小下把可用性从 0.9999998989 提升到 0.999999999997",
			Evidence: EvidenceAnalysis,
			Where:    "pkg/faft/planner.go: UniformFaftPlanner 消融; pkg/faft/planner_test.go",
			CanClaim: "在给定故障模型下，成员选择准则单独贡献了多少可用性",
			CannotClaim: "真实系统中的可用性 —— 依赖故障模型的正确性，而模型参数是假设值",
		},
		{
			Statement: "相关故障下加大 quorum 会让系统更脆弱：5 域、事件打掉 3 域时，" +
				"3 副本/3 域可用性 = 1-0.7q，4 副本/4 域 = 1-q",
			Evidence: EvidenceAnalysis,
			Where:    "pkg/faft/solve_failure_test.go: TestMemberAvailabilityRegionalEventCapsAvailability",
			CanClaim: "在'事件等概率命中固定数量域'这一模型下的精确结论",
			CannotClaim: "真实区域事件的命中分布就是这样 —— 模型是简化，参数需实测标定",
		},
		{
			Statement: "1000 分片下单分片可用性 99.8%，全集群同时可用仅 13.5%",
			Evidence:  EvidenceAnalysis,
			Where:     "pkg/sim/sim.go: Result.ClusterAvailability（= 单分片可用性 ^ 分片数）",
			CanClaim:  "在分片间**独立**假设下，全集群可用性随分片数指数衰减这一结构性质",
			CannotClaim: "绝对可用性 —— 分片间独立是强假设。宿主共享会让多个分片的副本一起挂，" +
				"实际值会更低。注意此数字是**解析计算**，不是离散事件模拟的结果，" +
				"尽管它产出于名为 sim 的包",
		},
		{
			Statement: "模拟吞吐上限与实测的比值：实测平均为上限的 7.26%（范围 4.44%–10.07%）",
			Evidence:  EvidenceAnalysis,
			Where:     "pkg/sim/calib.go: FitOverheadFactor；输入为 results/*.json 的实测样本",
			CanClaim:  "从 2 个实测样本拟合出的「模型外开销」量级",
			CannotClaim: "把它当作实测值本身 —— 它是派生量。更不能迁移到其他硬件：" +
				"2 个样本远不足以支撑这样的外推",
		},
		{
			Statement: "leader 出口带宽 ∝ 1/(n-1)，因此大规模下绑定约束必然转向带宽",
			Evidence: EvidenceDerived,
			Where:    "pkg/model; pkg/sim",
			CanClaim: "作为引用（Mencius OSDI'08 §7 已给出该律），不能作为本项目的发现",
			CannotClaim: "这是本项目的贡献 —— 已发表文献里明确写着",
		},
		{
			Statement: "在 hashicorp/raft 的真实提交路径上，|Q2| 从 3 降到 1 使提交吞吐从 " +
				"41.2k 升到 166.7k ops/s（4.05 倍，注入单程延迟 5ms）；极值比随 RTT " +
				"单调上升：0ms 1.03 → 1ms 1.25 → 2ms 2.02 → 5ms 4.05，而注入延迟为 0 时三组区间重叠",
			Evidence: EvidenceMeasured,
			Where:    "cmd/raftbench -> results/raftbench-sweep-n5.json; scripts/run-raftbench.ps1",
			CanClaim: "在**本机、进程内 in-memory 介质、注入延迟**这套设定下，" +
				"quorum 几何对共识层提交吞吐/延迟的因果效应；以及「收益随 RTT 单调上升」这一形状",
			CannotClaim: "端到端系统吞吐（路由/编解码/存储/fsync 全被剥掉）；" +
				"真实网络的绝对性能（注入的是延迟模型，不是真实网络）；" +
				"不能报单点倍数 —— 本机同一配置 5 轮内波动可达 72k–184k，" +
				"必须报区间与趋势方向",
		},
		{
			Statement: "故障切换后没有已确认的写丢失或被回滚：三层判定全部通过 —— " +
				"① 每条已确认的写至少存在于一个节点、② 切换前已确认的写都在新 leader 上、" +
				"③ 被杀 leader 的日志含它确认过的全部写（n=5，2ms 注入延迟，三层各 0 缺失）",
			Evidence: EvidenceMeasured,
			Where:    "cmd/raftbench -mode fail -> results/raftbench-fail-q1*.json（MEASUREMENT.md §5.7）",
			CanClaim: "在 |Q1|+|Q2|>N 的几组配置下，硬停 leader 不会让已确认的写消失；" +
				"以及 |Q2|=1 时「已确认但只存在于一个副本上」的量级（实测 33.7 万条，存活节点各缺 275 条）",
			CannotClaim: "**这不是安全性证明**。它做的是证伪：只要三层里任何一层出问题方案就是错的。" +
				"安全性靠 FPaxos 的理论论证，实测只是把「理论说不会出事」变成" +
				"「在这些条件下确实没出事」。样本量也远不足以覆盖所有故障交错",
		},

		{
			Statement: "|Q2| 变小的可行性条件是可调且零成本的：把 MaxAppendEntries 从上游默认 64 " +
				"提到 256，落后量从单调增长（1.1 万→4.3 万条）变成稳定有界（约 2.5k–4k 条），" +
				"而吞吐不变（150k–174k ops/s，落在噪声内）",
			Evidence: EvidenceMeasured,
			Where:    "cmd/raftbench -mae -> results/raftbench-mae-*.json（MEASUREMENT.md §5.6）",
			CanClaim: "「小 |Q2| 的收益有前提」这条限制是**可消除**的：" +
				"控制变量是 MaxAppendEntries，实测转折点在 128 与 256 之间，" +
				"提高它不牺牲吞吐。因此 FAFT 求解器应把它作为一条约束" +
				"（选了小的 |Q2| 就必须同时选够 MaxAppendEntries）",
			CannotClaim: "精确的容量模型 —— 简单估计 MaxAppendEntries/延迟 是保守下界" +
				"（它假设每 RTT 只有一条 RPC 在途）；实测有效在途深度约 3–4，" +
				"而这个数依赖 inmemPipeline 的实现细节，不能外推到其他实现",
		},
		{
			Statement: "|Q2| 就是「Apply 返回成功那一刻日志所在副本数」的严格下界：" +
				"|Q2|=3/2/1 时实测下界分别为 3/2/1（1500 个串行样本，注入延迟 1ms）",
			Evidence: EvidenceMeasured,
			Where:    "cmd/raftbench -mode lag -> results/raftbench-lag-q1*.json",
			CanClaim: "|Q2| 的语义在真实实现上确实就是提交所需的副本数；" +
				"|Q2|=1 时客户端收到成功时数据只在一个副本上（实测 100%）",
			CannotClaim: "真实磁盘故障下的持久性 —— in-memory 日志；" +
				"也不能说这违反安全性：|Q1|+|Q2|>N 保证已提交日志不回滚，" +
				"被牺牲的是介质丢失容忍度与选主可用性",
		},
		{
			Statement: "|Q1| 变大对选主可用性的影响是**阶跃**的：存活副本数 < |Q1| 时 0/7 成功，" +
				"≥ |Q1| 时 7/7 成功（n=5，杀掉原 leader + k-1 个节点）",
			Evidence: EvidenceMeasured,
			Where:    "cmd/raftbench -mode avail -> results/raftbench-avail-q1*.json",
			CanClaim: "在 in-memory 介质下，选主成功率由「存活数 ≥ |Q1|」这个算术关系决定，" +
				"不是渐变；|Q1|=5 时任何单点故障都让集群失去选主能力",
			CannotClaim: "真实分区场景下的可用性 —— 没有真实网络分区，" +
				"且选举本身在内存介质下快到不构成瓶颈，所以这里量的主要是门槛而不是超时",
		},
	}
}

// FormatEvidenceManifest 渲染证据清单。
func FormatEvidenceManifest() string {
	out := "本项目结论的证据等级清单\n"
	out += "============================\n\n"
	for i, c := range EvidenceManifest() {
		out += fmt.Sprintf("%d. [%s] %s\n", i+1, c.Evidence, c.Statement)
		out += fmt.Sprintf("   产出位置: %s\n", c.Where)
		out += fmt.Sprintf("   可主张  : %s\n", c.CanClaim)
		out += fmt.Sprintf("   不可主张: %s\n\n", c.CannotClaim)
	}
	out += "证据等级说明：\n"
	out += "  MEASURED(实测)   真实系统运行产出，可作绝对性能声明\n"
	out += "  ANALYSIS(解析)   组合概率/成本模型/约束求解，只作成本界与结构性质\n"
	out += "  SIMULATED(模拟)  离散事件模拟，只作趋势；校准未过还须标注偏离点\n"
	out += "  DERIVED(推导)    本项目自行推导，论文中须同样标注为推导\n"
	return out
}
