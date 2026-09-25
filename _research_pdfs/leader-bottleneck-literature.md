# The Leader Bottleneck / Scaling Laws of Leader-Based Consensus Replication

Verified literature review, 2010–2026 plus key earlier work. Every entry was checked against a
primary source (author PDF, USENIX page, arXiv abs/HTML, DROPS, repository record, or Crossref
metadata API). Numbers in quotes were read out of the actual paper text, not search snippets.

---

## 1. Papers that MODEL or MEASURE the single-leader throughput ceiling

- **Songtao Mao, Flavio P. Junqueira, Keith Marzullo** | *Mencius: Building Efficient Replicated State
  Machines for WANs* | 8th USENIX Symposium on OSDI, 2008, pp. 369–384 |
  https://www.usenix.org/legacy/events/osdi08/tech/full_papers/mao/mao.pdf |
  **This is the paper that actually states the `1/(n−1)` leader-bandwidth law in plain text.**
  "Since Paxos is limited by the leader's total outgoing bandwidth, its throughput is in proportion to
  1/(n−1). Mencius, on the other hand, can use the extra bandwidth provided by the new sites, and so the
  throughput is in proportion to n/(n−1)." Measured (network-bound, ρ = 4000, wide-area clique): Mencius
  430 → 360 → 340 ops at n = 3, 5, 7, versus Paxos 150 → 75 → 50 ops; the paper itself writes Paxos's drop
  as "50% = (1/4)/(1/2)" and "33% = (1/6)/(1/2)", i.e. exactly the 1/(n−1) prediction. CPU-bound the numbers
  invert: "Paxos ... presents a throughput of 6,000 ops, with 100% CPU utilization at the leader and 50% at
  the other servers. Mencius's throughput under the same condition was 9,000 ops." Also: "The number of
  messages a leader needs to process for every request grows linearly with the number of servers n, but it
  remains constant for other replicas. This seriously impacts the scalability of Paxos for larger n." And:
  "Under high load, the outgoing bandwidth of the leader is a bottleneck, whereas the channels between the
  non-leaders idle." On the pure-bandwidth experiment: "Paxos had a throughput of about 540 ops, or one
  third of Mencius's throughput: Paxos is limited by the leader's outgoing bandwidth."

- **Martin Biely, Zarko Milosevic, Nuno Santos, André Schiper** | *S-Paxos: Offloading the Leader for High
  Throughput State Machine Replication* | 2012 IEEE 31st International Symposium on Reliable Distributed
  Systems (SRDS), pp. 111–120 | DOI [10.1109/SRDS.2012.66](https://doi.org/10.1109/SRDS.2012.66) |
  https://ieeexplore.ieee.org/abstract/document/6424845 |
  Exact title, venue, year, author order and page range verified via Crossref. **The IEEE full text is
  paywalled and I could not fetch it**, so the finding below is quoted from the same author's EPFL doctoral
  thesis, which contains the S-Paxos chapter (Nuno Filipe de Sousa Santos, *State Machine Replication: from
  Analytical Evaluation to High-Performance Paxos*, EPFL THÈSE NO 5410, 21 Sept 2012,
  https://infoscience.epfl.ch/server/api/core/bitstreams/7de242ea-09f1-4985-b319-90d41610c802/content):
  "most implementations of state machine replication have an unbalanced division of work among threads, with
  one replica, the leader, having a significantly higher workload than the other replicas. Naturally, the
  leader becomes the bottleneck of the system, while other replicas are only lightly loaded. We propose and
  evaluate S-Paxos, which evenly balances the workload among all replicas, and thus overcomes the leader
  bottleneck." The thesis also states S-Paxos "breaks the traditional trade-off between performance and
  fault tolerance, because adding more replicas to S-Paxos improves its performance (up to reasonable number
  of replicas)."

- **Heidi Howard, Malte Schwarzkopf, Anil Madhavapeddy, Jon Crowcroft** | *Raft Refloated: Do We Have
  Consensus?* | ACM SIGOPS Operating Systems Review **49**(1), pp. 12–21, 2015 |
  DOI [10.1145/2723872.2723876](https://doi.org/10.1145/2723872.2723876) |
  https://www.repository.cam.ac.uk/items/078703f7-8c64-4b6e-b2b2-091e6fb91733 |
  **CORRECTION TO THE PREMISE: this paper does not report throughput as a function of cluster size at
  n = 3, 5, 7, 9 — or at any n.** I read the accepted-version full text. It re-implements Raft in OCaml and
  reproduces *only* the leader-election experiment from the original Raft paper, explicitly: "Figure 7
  reprints the results from Ongaro and Ousterhout's evaluation of the duration of a Raft leader election...
  In the following, we will focus on the latter experiment [leader election]." Setup: "five idle machines
  connected via a 1Gb/s Ethernet switch, with an average broadcast time of 15ms." Its quantitative results
  are *packet counts to elect a leader* (Table 1, mean packets): original version 10.8 / 14.6 / 108.2 and
  combined optimizations 11.7 / 14.1 / 48.6 for follower timeouts 150–300 / 150–200 / 150–155 ms. It also
  reports that in the highly contested 150–155 ms case "95% of the time leaders are established within 281ms
  ... compared to 1330ms without the optimization." No throughput-vs-n curve exists in this paper.

- **Diego Ongaro, John Ousterhout** | *In Search of an Understandable Consensus Algorithm* | 2014 USENIX
  Annual Technical Conference (ATC '14), pp. 305–319 |
  https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf |
  **The original Raft paper also does not publish throughput vs cluster size.** Its §8.3 says: "We used our
  Raft implementation to measure the performance of Raft's leader election algorithm and answer two
  questions." Cluster sizes appear only as: "cluster of five servers with a broadcast time of roughly 15ms.
  Results for a cluster of nine servers are similar." The paper's performance statement is qualitative:
  "Raft achieves this using the minimal number of messages (a single round-trip from the leader to half the
  cluster)."

- **Diego Ongaro** | *Consensus: Bridging Theory and Practice* (PhD thesis, Stanford University, 2014) |
  https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf |
  This is where the Raft-lineage statement of the leader bottleneck lives. §5.4 (Log Compaction): "the
  leader's outbound network bandwidth is usually Raft's most precious (bottleneck) resource." §11.7.1 is
  titled "Reducing leader bottleneck": "Many optimizations focus on reducing the leader as a performance
  bottleneck. As a single server, the leader has limited resources and may be located inconveniently in
  wide-area deployments... Unfortunately, most of these optimizations are in conflict with Raft's strong
  leader approach. Raft leverages its strong leader for understandability and reducing mechanism, and this
  key design choice is at odds with reducing the leader's involvement in normal operations."

- **Parisa Jalili Marandi, Marco Primi, Nicolas Schiper, Fernando Pedone** | *Ring Paxos: A high-throughput
  atomic broadcast protocol* | 2010 IEEE/IFIP International Conference on Dependable Systems and Networks
  (DSN), pp. 527–536 | DOI [10.1109/DSN.2010.5544272](https://doi.org/10.1109/DSN.2010.5544272) |
  extended version (JPDC) arXiv:1401.6015, https://ar5iv.labs.arxiv.org/html/1401.6015 |
  Quantifies the leader/coordinator fan-in cost with an explicit n factor: "to make one decision the
  coordinator consumes n times more incoming bandwidth and CPU in the many-to-one communication pattern
  than in pipelined communication pattern, where n is the number of acceptors other than the coordinator."
  It defines efficiency as "the rate between its maximum achieved throughput per receiver, and the nominal
  transmission capacity of the system per receiver", and claims "efficiency above 90%, which in some cases
  does not depend on the number of receivers." M-Ring Paxos "places f+1 nodes in a logical ring"; U-Ring
  Paxos uses unicast only. "M-Ring Paxos has low delivery latency, below 5 msec, which remains approximately
  constant with an increasing number of receivers (up to 25 receivers in our experiments)."

- **Parisa Jalili Marandi, Marco Primi, Fernando Pedone** | *Multi-Ring Paxos* | 42nd Annual IEEE/IFIP
  International Conference on Dependable Systems and Networks (DSN 2012) |
  DOI [10.1109/DSN.2012.6263916](https://doi.org/10.1109/DSN.2012.6263916) |
  https://dl.acm.org/doi/10.5555/2354410.2355144 |
  Metadata (title, authors, venue, year) verified via Crossref. **The full text is paywalled on IEEE/ACM;
  I did not fetch it, so its specific throughput numbers are UNVERIFIED here.** Its continuation is what I
  did read:

- **Samuel Benz, Leandro Pacheco de Sousa, Fernando Pedone** | *Stretching Multi-Ring Paxos* | arXiv:1504.04942
  (v1, 20 April 2015) | https://arxiv.org/abs/1504.04942 |
  States the design intent of the ring family explicitly: "Multi-Ring Paxos was designed to scale throughput
  with the addition of resources", and positions it against protocols where "bottlenecks (e.g., as typically
  happens with the coordinator ...)" cap scaling. Evaluated in a 10 Gbps network with 1 to 32 rings,
  reporting aggregate and per-ring throughput for 32-Kbyte and 200-byte values, and reporting that during
  recovery "the average throughput during recovery is 78% of the throughput under normal operation."

- **Robbert van Renesse, Fred B. Schneider** | *Chain Replication for Supporting High Throughput and
  Availability* | 6th USENIX Symposium on OSDI, 2004 | https://www.cs.cornell.edu/fbs/publications/ChainReplicOSDI.html |
  Chain replication's throughput argument is that per-update work is genuinely O(chain length) and that
  shortening the chain measurably raises update throughput: "Update throughput decreases to 0 at the time of
  the server failure and then, once the master deletes the failed server from all chains, throughput is
  actually better than it was initially. This throughput improvement occurs because the server failure
  causes some chains to be length 2 (rather than 3), reducing the amount of work involved in performing an
  update." It also makes the head/tail split explicit — "the primary's role in sequencing requests is shared
  by two replicas. The head sequences update requests; the tail extends that sequence by interleaving query
  requests" — and notes that for updates, "computation done at t − 1 of the t servers does not contribute to
  producing the reply and, arguably, is redundant." Caveat: results are from a *simulated* network with
  chain length t = 3 over N = 24 servers, not a physical testbed.

## 2. Consensus/leader-bottleneck papers at SOSP/OSDI/NSDI/EuroSys/SIGMOD

- **Iulian Moraru, David G. Andersen, Michael Kaminsky** | *There Is More Consensus in Egalitarian
  Parliaments* | 24th ACM Symposium on Operating Systems Principles (SOSP 2013), pp. 358–372 |
  DOI [10.1145/2517349.2517350](https://doi.org/10.1145/2517349.2517350) | PDF:
  https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf |
  The clearest complexity statement of the leader bottleneck in the EPaxos line: "For each command, the
  leader handles Θ(N) messages, and non-leader replicas handle only O(1). Thus, the leader can become a
  bottleneck, as practical implementations of Paxos have observed [4]." (Reference [4] is Chubby.) Measured:
  "EPaxos outperforms Multi-Paxos because the Multi-Paxos leader becomes bottlenecked by its CPU", and
  "Batching increases the maximum throughput of Multi-Paxos by 5x and of EPaxos by 9x."

- **Dan R. K. Ports, Jialin Li, Vincent Liu, Naveen Kr. Sharma, Arvind Krishnamurthy** | *Designing
  Distributed Systems Using Approximate Synchrony in Data Center Networks* (Speculative Paxos) | 12th USENIX
  Symposium on NSDI, 2015 | https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-ports.pdf |
  Message-count asymmetry and its measured throughput consequence: "In Speculative Paxos, each replica
  processes only two messages (plus periodic synchronizations), whereas all 2n messages are processed by the
  leader in Paxos." Measured on a twelve-switch testbed: "Speculative Paxos is able to sustain a higher
  throughput level (∼100,000 req/s) than either Paxos or Fast Paxos (∼38,000 req/s), because fewer messages
  are handled by the leader, which otherwise becomes a bottleneck." Headline: "40% lower latency and 2.6×
  higher throughput than leader-based Paxos." Note it also confirms batching as an alternative escape:
  "batching also increases the throughput of Paxos substantially by eliminating the leader bottleneck: the
  two achieve equivalent peak throughput levels. However, batching also increases latency: at a throughput
  level of 90,000 req/s, its latency is 3.5 times higher than that of Speculative Paxos."

- **Jialin Li, Ellis Michael, Naveen Kr. Sharma, Adriana Szekeres, Dan R. K. Ports** | *Just say NO to Paxos
  Overhead: Replacing Consensus with Network Ordering* (NOPaxos) | 12th USENIX Symposium on OSDI, 2016 |
  https://www.usenix.org/system/files/conference/osdi16/osdi16-li.pdf |
  The single most explicit measured statement that leader-based throughput *degrades with n*: "Paxos and
  Fast Paxos suffer throughput degradation proportional to the number of replicas because the leaders in
  those protocols have to process more messages from the additional replicas. Replicas in NOPaxos and
  Speculative Paxos, however, process a constant number of messages, so those protocols maintain their
  throughput when more replicas are added." The mechanism is stated earlier: "the leaders' message
  processing quickly becomes the bottleneck of these systems." All experiments used five replicas. Reported
  gains: "by 54% in latency and 4.7× in throughput" over classic leader-based Paxos.

- **Aleksey Charapko, Ailidani Ailijiang, Murat Demirbas** | *PigPaxos: Devouring the Communication
  Bottlenecks in Distributed Consensus* | Proceedings of the 2021 International Conference on Management of
  Data (SIGMOD '21), pp. 235–247 | DOI [10.1145/3448016.3452834](https://doi.org/10.1145/3448016.3452834) |
  arXiv:2003.07760, https://arxiv.org/abs/2003.07760 |
  Frames the bottleneck as a fan-out/fan-in asymmetry and shows it survives mitigation: "In strongly
  consistent replication, all the communication drains through a single node – the primary (a.k.a. the
  leader), which constitutes a throughput bottleneck", and "the leader handles up to 4 times more messages
  than a follower." Key negative result, stated for the single-relay configuration: "With R = 1 and an
  arbitrary number of follower nodes, the leader remains a bottleneck. The leader's message load M_l is a
  linear [function of the number of relay groups]" and "the leader remains a bottleneck regardless of the
  cluster size and the number of relay groups." Measured on 5 AWS nodes: "EPaxos throughput gets saturated
  at 3000 requests per second, Paxos throughput reaches its limit of around 2000 req/sec"; PigPaxos shows
  "three times more throughput than Paxos at all payload sizes"; on a 9-node cluster with R = 2 it
  "improves the throughput over classical Multi-Paxos by as much as 57%".

- **Jinkun Geng, Anirudh Sivaraman, Balaji Prabhakar, Mendel Rosenblum** | *Nezha: Deployable and
  High-Performance Consensus Using Synchronized Clocks* | Proceedings of the VLDB Endowment **16**(4), 2022
  | DOI [10.14778/3574245.3574250](https://doi.org/10.14778/3574245.3574250) | technical report:
  arXiv:2206.03285, https://arxiv.org/abs/2206.03285 |
  Table 1 tabulates "Load on Leader" — defined as "the total number of messages the leader needs to send or
  receive in order to commit one client request" — across protocols: Multi-Paxos/Raft **2(2f+1)**; Fast Paxos
  **2f+2**; Speculative Paxos **2**; NOPaxos **2**; Mencius and EPaxos **2(2f+1)/l** where l is the number of
  leaders; Nezha without proxy **2 + 2f/m**. It also measured client-side saturation effects on scalable
  designs: "the throughput of Nezha-Non-Proxy distinctly degrades from 187.8K requests/sec to 148.7K
  requests/sec, as the number of replicas grows."

- **Kexin Hu, Kaiwen Guo, Qiang Tang, Zhenfeng Zhang, Hao Cheng, Zhiyang Zhao** | *Leopard: Towards High
  Throughput-Preserving BFT for Large-scale Systems* | arXiv:2106.08114 (v1 2021, v4 2022) |
  https://arxiv.org/abs/2106.08114 | https://arxiv.org/html/2106.08114v4 |
  Defines the "scaling factor" metric specifically to capture the leader ceiling and reports measured
  HotStuff leader-bandwidth saturation with growing n (Fig. 2, 128-byte payload): "the throughput increase
  via scaling-up in state-of-the-art protocols approaches 0 when the scale (number of replicas) gets
  larger." Leopard reports throughput "remains at a high level of 10^5" at 600 replicas and "5× throughput
  over HotStuff when the scale is 300". (Also see the formal derivation in §3 below.)

- **Marios Kogias, Edouard Bugnion** | *HovercRaft: Achieving Scalability and Fault-tolerance for
  microsecond-scale Datacenter Services* | 15th European Conference on Computer Systems (EuroSys 2020), pp.
  1–17 | DOI [10.1145/3342195.3387545](https://doi.org/10.1145/3342195.3387545) |
  Metadata verified via Crossref. This is the RDMA-based Raft line: it augments the Raft leader with
  in-network (RDMA) semantics so the leader no longer serially serves follower traffic. **I downloaded the
  PDF but did not extract its throughput numbers; the specific figures are UNVERIFIED in this report.**

- **Aleksandar Dragojević, Dushyanth Narayanan, Edmund B. Nightingale, Matthew Renzelmann, Alex Shamis,
  et al.** | *No compromises: distributed transactions with consistency, availability, and performance*
  (DrTM) | 25th ACM Symposium on Operating Systems Principles (SOSP 2015), pp. 54–70 |
  DOI [10.1145/2815400.2815425](https://doi.org/10.1145/2815400.2815425) |
  Bibliographic record (title, venue, year, DOI, author list prefix) verified via Crossref. **Content
  claims — including any throughput numbers — are UNVERIFIED here; I did not read the paper.**

- **Patrick Hunt, Mahadev Konar, Flavio P. Junqueira, Benjamin Reed** | *ZooKeeper: Wait-free coordination
  for Internet-scale systems* | 2010 USENIX Annual Technical Conference (ATC '10) |
  https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf |
  Relevant because Zab/ZooKeeper is the canonical production leader-based design and it publishes the
  write-throughput penalty of adding voters: "We use more servers to tolerate more faults. We increase write
  throughput by partitioning the ZooKeeper data into multiple ZooKeeper ensembles. This performance trade
  off between replication and partitioning has been previously observed by Gray et al. [12]." Exact numbers
  in §4 below.

- **Haochen Pan, Jesse Tuglu, Neo Zhou, Tiantian Wang, et al.** | *Rabia: Simplifying State-Machine
  Replication Through Randomization* | ACM SIGOPS 28th Symposium on Operating Systems Principles (SOSP '21)
  | DOI [10.1145/3477132.3483582](https://doi.org/10.1145/3477132.3483582) | arXiv:2109.12616 |
  Leaderless/randomized alternative to the EPaxos line. Abstract-level claims read from the paper:
  "It does not need any fail-over protocol and supports trivial auxiliary protocols like log compaction",
  reports "higher throughput than the closest competitor (i.e., EPaxos)", with the paper stating "improvement
  in throughput is up to 1.5x using a close-loop test", and citing that NOPaxos "outperforms Multi-Paxos by
  4.7x in throughput."

## 3. Formal lower bounds and analytical throughput-vs-n results

**Answer to the core question: the "leader egress bandwidth-limited at C/(n−1)" argument is NOT folklore.
It appears explicitly and with the exact algebra in at least three real, citable papers.**

- **Songtao Mao, Flavio P. Junqueira, Keith Marzullo** | *Mencius* | OSDI 2008 |
  https://www.usenix.org/legacy/events/osdi08/tech/full_papers/mao/mao.pdf |
  Quoted verbatim: "Since Paxos is limited by the leader's total outgoing bandwidth, its throughput is in
  proportion to 1/(n−1)." This is the earliest clean statement of the law I could verify, and it is
  accompanied by measurements that track the prediction (150 → 75 → 50 ops for n = 3, 5, 7).

- **Kexin Hu et al.** | *Leopard* | arXiv:2106.08114 | https://arxiv.org/html/2106.08114v4 |
  The full derivation, with the constant named C and the reciprocal stated: "Recall that the leader serves
  as disseminating every pending request via consensus proposals to all the other n − 1 replicas... Let Λ be
  throughput (the number of requests to be confirmed per second)... When processing Λ requests, the leader's
  communication cost at the propose step during request dissemination is **Λ × payload size × (n − 1)**"
  [their Eq. 1]. "Since the maximal achievable throughput is subject to the constraint of the number of bits
  that can be transmitted per second at each replica, denoted as C, throughput shall certainly drop when n
  grows." They then define SF = max{c_i} and state: "a protocol's expected throughput is limited by C/SF."
  And the reciprocal law: "it can be deduced from (1) that, the γ is at most 1/(n−1) in these leader-based
  BFT protocols. And it quickly approaches 0 as n increases. This means that scaling up becomes ineffective
  for these BFT systems to improve throughput in a large-scale environment." Their scaling-factor value for
  a leader-based BFT protocol is "SF = max{c_L, c_R} = O(n)", so the ceiling decays as C/n.

- **Andrew Lewis-Pye, Patrick O'Grady** | *The Carnot Bound: Limits and Possibilities for Bandwidth-Efficient
  Consensus* | arXiv:2603.11797 (v1 12 Mar 2026, v3 28 Jul 2026) | https://arxiv.org/abs/2603.11797 |
  https://arxiv.org/html/2603.11797v3 |
  A genuine impossibility/lower-bound result on the leader's egress, framed around erasure coding. Verbatim:
  "In leader-based State Machine Replication (SMR), the leader's outgoing bandwidth is a natural throughput
  bottleneck." Their throughput law: "For a network in which each processor has bandwidth S, i.e., processors
  ... the maximum throughput, measured in payload bits finalised per unit time, is therefore approximately
  **S/d**", where d is the data expansion rate (the ratio of total data sent to payload size). Main theorem:
  "We prove that protocols with 2-round finality (one voting round) cannot achieve a data expansion rate below
  approximately 2.5, matching existing protocols. Protocols with 3-round finality (two voting rounds) can do
  significantly better." The bound is tight: "This bound is exactly tight: E-Minimmit and Kudzu use erasure
  codes with reconstruction parameter k = 2f + 1, achieving data expansion rates of (5f+1)/(2f+1) ≈ 2.5."
  Their two 3-round protocols reach safe rates of "approximately 1.33 and 1.5, respectively, both well below
  the 2.5 lower bound for 2-round finality." Useful structural results they give for the leader's fan-out:
  \(n/(n-2f) = (3f+1)/(f+1) \to 3\) and \(n/(n-f) = (4f+1)/(3f+1) \to 1.33\). They explicitly credit the
  throughput model to Lewis-Pye, Nayak, Shrestha, *The Pipes Model for Latency Analysis*, Cryptology ePrint
  Archive (2025) — that is the modelling framework, if you want the primary bandwidth model.

- **Tuanir França Rezende, Pierre Sutra** | *Leaderless State-Machine Replication: Specification, Properties,
  Limits (Extended Version)* | arXiv:2008.02512 (6 Aug 2020) | https://arxiv.org/abs/2008.02512 |
  The complementary bound on the *leaderless* escape route. "We show that protocols matching all of the ROLL
  properties are subject to a trade-off between performance and reliability. We also establish a lower bound
  on the message delay to execute a command in protocols optimal for the ROLL properties. This lower bound
  explains the persistent chaining effect observed in experimental results." It also states the leader-based
  baseline limitation directly: "Due to their reliance on a leader replica, classical SMR protocols offer
  limited scalability and availability in this setting... Second, as the leader becomes a bottleneck or its
  network gets slower, system performance decreases."

## 4. Measured throughput of real systems vs cluster size

- **Patrick Hunt, Mahadev Konar, Flavio P. Junqueira, Benjamin Reed** | *ZooKeeper: Wait-free coordination
  for Internet-scale systems* | USENIX ATC 2010 |
  https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf |
  **This — not Raft Refloated — is where the n = 3/5/7/9 saturation numbers live.** Table 1, "throughput
  performance of the extremes of a saturated system", 1 KB read or write requests, 35 client machines
  simulating 250 clients, Java server logging to a dedicated disk:

  | servers | 100% reads | 0% reads (pure writes) |
  |---|---|---|
  | 3 | 87k ops/s | 21k ops/s |
  | 5 | 165k ops/s | 18k ops/s |
  | 7 | 257k ops/s | 14k ops/s |
  | 9 | 296k ops/s | 12k ops/s |
  | 13 | 460k ops/s | 8k ops/s |

  Read throughput rises steeply with ensemble size (local reads parallelize); **write throughput falls
  monotonically, 21k → 8k ops/s from 3 to 13 servers.** The paper's own reading: "the number of servers in
  the system does not only impact the number of failures that the service can handle, but also the workload
  the service can handle." It also reports average request latency "1.2ms for three servers and 1.4ms for 9
  servers." (ZooKeeper replication is Zab, a leader-based atomic broadcast.)

- **Venkata Swaroop Matte, Aleksey Charapko, Abutalib Aghayev** | *Scalable but Wasteful: Current State of
  Replication in the Cloud* | 13th ACM Workshop on Hot Topics in Storage and File Systems (HotStorage '21) |
  DOI [10.1145/3465332.3470882](https://doi.org/10.1145/3465332.3470882) | author slides:
  https://www.hotstorage.org/2021/2021-slides/Scalable-but-Wasteful-Current-State-of-Replication-in-the-Cloud.pdf |
  A direct cloud-VM comparison of the bottleneck trade-off. Setup: "5 AWS EC2 m5a.large nodes, each 2 vCPU,
  8GB RAM, 50% write workload." Result: Multi-Paxos **14 kops/s at 200% aggregate CPU utilization** versus
  EPaxos **18 kops/s at 500% utilization**; "EPaxos achieves 20% higher throughput compared to Multi-Paxos"
  (the stated 20% and the 18/14 ratio are not exactly consistent in the source — quoted as reported) and
  "Multi-Paxos shows better resource efficiency compared to EPaxos." The paper's framing: "Many protocols
  shift work from the bottleneck to the under-utilized node. Examples: EPaxos, SDPaxos and PigPaxos."

- **Mencius (OSDI 2008)** and **PigPaxos (SIGMOD 2021)** and **Nezha (PVLDB 2022)** also supply measured
  throughput-vs-n curves; their numbers are given in §1 and §2 above. The strongest single scaling-out
  measurement across the literature reviewed here is Mencius's Paxos line: 150 → 75 → 50 ops/s at n = 3, 5, 7.

---

## UNVERIFIED / COULD NOT CONFIRM

1. **"Raft Refloated measured throughput scaling vs cluster size at n = 3, 5, 7, 9."** **False / could not
   confirm — and positively contradicted.** I read the full accepted-version text of the paper. It has no
   throughput-vs-n experiment; its evaluation reproduces only the leader-election experiment. The n = 3/5/7/9
   saturation numbers that circulate in this context belong to **ZooKeeper's ATC 2010 Table 1** (21k → 12k
   write ops/s over 3/5/7/9 servers). The original **Raft ATC 2014** paper likewise reports only election
   timing. If a source asserts Raft Refloated has these numbers, that assertion is wrong.

2. **BlindWrites.** I could not verify this paper exists under any form of the title given. A Crossref
   bibliographic query for "BlindWrites" returned zero items, and a web search returned only unrelated
   documents. **UNVERIFIED — do not cite it without an independent bibliographic record.** (If it is real, it
   is not indexed in Crossref under that title, which is unusual for EuroSys/ACM.)

3. **DARE venue.** The task described DARE as HPDC 2019. Crossref returns **DARE — Poke & Hoefler — 24th
   International Symposium on High-Performance Parallel and Distributed Computing (HPDC 2015), DOI
   10.1145/2749246.2749267**. The 2019 date is **wrong**; the 2015 date is verified. Content claims
   (throughput numbers) are UNVERIFIED — I could not retrieve a readable PDF.

4. **S-Paxos (SRDS 2012) exact numeric results.** Title, authors, venue, year, pages and DOI are verified via
   Crossref, and the leader-bottleneck thesis is verified from the author's EPFL thesis. The paper's own
   measured throughput figures are **UNVERIFIED** — IEEE Xplore is paywalled and I could not fetch the PDF.

5. **Multi-Ring Paxos (DSN 2012) throughput numbers.** Bibliographic record verified via Crossref; the
   IEEE/ACM full text is paywalled and I did not read it. Only the successor paper (*Stretching Multi-Ring
   Paxos*, arXiv:1504.04942) was read in full. **UNVERIFIED.**

6. **HovercRaft, DrTM ("No compromises"), Microsecond Consensus, Nezha's full evaluation.** Bibliographic
   records verified (Crossref for the first three; HovercRaft DOI 10.1145/3342195.3387545, DrTM DOI
   10.1145/2815400.2815425, Microsecond Consensus arXiv:2010.06288). Their **throughput numbers are
   UNVERIFIED in this report** — I did not extract them. Nezha's Table 1 and one scalability number *are*
   verified and quoted above.

7. **etcd, TiKV, CockroachDB, Kafka, Spanner throughput-vs-cluster-size.** I could **not** verify peer-reviewed
   measurements of these systems' write throughput as a function of voter count. etcd's official performance
   page (https://etcd.io/docs/v3.5/op-guide/performance/) is a tuning guide, not an n-sweep; its demo
   benchmarks describe a 3-node cluster. **No verified n = 3/5/7 measurement exists in this report for etcd,
   TiKV, CockroachDB, Kafka or Spanner.** Treat any such numbers you have seen as unverified until a primary
   source is produced.

8. **A single canonical formula covering batch size, fsync latency, link bandwidth and follower count
   simultaneously.** I found no paper that states the full four-variable closed form. What exists is (a) the
   bandwidth-only law S/d and the C/(n−1) leader-egress law (Carnot Bound, Leopard, Mencius), and (b) analytic
   batching/pipelining tuning models in the Paxos-implementation line (Santos's EPFL thesis contains an
   "analytical model of the performance of Paxos" used "to derive good values for the bounds on the batch size
   and number of parallel instances"). I did not verify a combined closed form — **UNVERIFIED** as a single
   published formula.

9. **A "C/(n−1)" formula phrased exactly that way.** No paper I read writes the literal string `C/(n−1)`.
   Leopard writes the equivalent as throughput bounded by `C/SF` with `SF = O(n)` and γ "at most 1/(n−1)";
   Mencius writes "in proportion to 1/(n−1)"; Carnot writes `S/d`. The *content* is a real published result;
   the exact notation is not a quotation from any source.
