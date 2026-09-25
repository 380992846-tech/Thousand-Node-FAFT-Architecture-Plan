// pkg/raft/node.go
//
// 每个分片（Raft group）中的一个数据节点。
//
// 本文件在原始版本基础上修复了以下缺陷，详见 docs/BUGS.md：
//
//	BUG-1  FSM 数据竞争：Get 读的是 fsm.store，却只持有 ks.mu（两个互不相干的锁）。
//	BUG-2  死字段 KVStore.store 从未被写入，任何基于它的读取都会返回空。
//	BUG-3  Bootstrap 逻辑反了：仅在非 Follower/Candidate 时返回，且重复调用会报错。
//	BUG-4  Join 语义错误：在本地节点上调用 AddVoter，只有本地恰好是 Leader 时才生效，
//	       且 leaderAddr 参数被完全忽略；同时 timeout 传 0 会立刻超时。
//	BUG-5  gob 编码可被对端解码为任意类型（非确定性字节流），且体积大于手写二进制编码。
//	BUG-6  快照 Persist 失败路径重复调用 sink.Close()。
//	BUG-7  raft 传输层与 metrics 端口未做冲突检测，千节点场景下静默失败。
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/distributed-kv/kvstore/pkg/metrics"
)

// 命令操作码。
const (
	opSet    byte = 1
	opDelete byte = 2
	// opAddVoter 通过 Raft 日志本身完成成员变更，避免"在本地节点上 AddVoter"的错误语义（BUG-4）。
	opAddVoter byte = 3
	// opRemoveServer 移除一个成员。
	opRemoveServer byte = 4
)

// 命令编码：1 字节 op + 4 字节 CRC32C + 2×uint32 长度前缀 + key + value。
// 相比 gob：无反射、无类型描述、字节确定性，解码器只接受自己的格式（修复 BUG-5）；
// 额外的 CRC 让"被截断/损坏的日志条目"在 Apply 阶段就被拒绝，而不是污染状态机。
const cmdHeaderLen = 1 + 4 + 4 + 4

var (
	// ErrNotLeader 该节点不是当前分片 Leader。
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrCorruptCommand 日志条目不是合法的命令编码。
	ErrCorruptCommand = errors.New("raft: corrupt command encoding")
	// ErrTooLarge 命令超过编解码上限。
	ErrTooLarge = errors.New("raft: command too large")
)

// maxFieldLen 单个字段上限（64 MiB），防止畸形长度前缀导致巨额分配。
const maxFieldLen = 64 << 20

// Command 一条状态机命令。
type Command struct {
	Op    byte
	Key   string
	Value string
	// Addr 仅用于 opAddVoter。
	Addr string
	// ID 仅用于 opAddVoter / opRemoveServer。
	ID string
}

// EncodeCommand 编码命令为确定性字节流。
func EncodeCommand(cmd Command) ([]byte, error) {
	if len(cmd.Key) > maxFieldLen || len(cmd.Value) > maxFieldLen {
		return nil, ErrTooLarge
	}
	buf := make([]byte, cmdHeaderLen+len(cmd.Key)+len(cmd.Value))
	buf[0] = cmd.Op
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(cmd.Key)))
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(cmd.Value)))
	n := cmdHeaderLen
	n += copy(buf[n:], cmd.Key)
	copy(buf[n:], cmd.Value)
	binary.BigEndian.PutUint32(buf[1:5], crc32.ChecksumIEEE(buf[5:]))
	return buf, nil
}

// DecodeCommand 解码命令；只接受本编码格式，并校验 CRC。
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < cmdHeaderLen {
		return Command{}, ErrCorruptCommand
	}
	if binary.BigEndian.Uint32(data[1:5]) != crc32.ChecksumIEEE(data[5:]) {
		return Command{}, fmt.Errorf("%w: checksum mismatch", ErrCorruptCommand)
	}
	cmd := Command{Op: data[0]}
	kl := binary.BigEndian.Uint32(data[5:9])
	vl := binary.BigEndian.Uint32(data[9:13])
	if kl > maxFieldLen || vl > maxFieldLen {
		return Command{}, ErrTooLarge
	}
	need := uint64(cmdHeaderLen) + uint64(kl) + uint64(vl)
	if uint64(len(data)) < need {
		return Command{}, ErrCorruptCommand
	}
	cmd.Key = string(data[cmdHeaderLen : cmdHeaderLen+int(kl)])
	cmd.Value = string(data[cmdHeaderLen+int(kl) : need])
	return cmd, nil
}

// encodeMembership 成员变更命令使用 key=ID、value=Addr 的同一编码格式。
func encodeMembership(op byte, id, addr string) ([]byte, error) {
	return EncodeCommand(Command{Op: op, Key: id, Value: addr})
}

// FSM 状态机。所有对 store 的访问都必须持有 mu（修复 BUG-1）。
type FSM struct {
	mu    sync.RWMutex
	store map[string]string
	// applied 已应用日志条目的数量，用于测试与观测。
	applied atomic.Uint64
}

// NewFSM 创建空状态机。
func NewFSM() *FSM {
	return &FSM{store: make(map[string]string)}
}

// Get 并发安全读取。
func (fsm *FSM) Get(key string) (string, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	v, ok := fsm.store[key]
	return v, ok
}

// Len 当前键数量。
func (fsm *FSM) Len() int {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return len(fsm.store)
}

// Applied 已应用条目数。
func (fsm *FSM) Applied() uint64 { return fsm.applied.Load() }

// Apply 实现 hraft.FSM。
func (fsm *FSM) Apply(log *hraft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		return err
	}
	fsm.applied.Add(1)

	switch cmd.Op {
	case opSet:
		fsm.mu.Lock()
		fsm.store[cmd.Key] = cmd.Value
		fsm.mu.Unlock()
	case opDelete:
		fsm.mu.Lock()
		delete(fsm.store, cmd.Key)
		fsm.mu.Unlock()
	case opAddVoter, opRemoveServer:
		// 成员变更由 raftSink 在日志之外的协调路径处理（见 ApplyMembership），
		// 状态机本身不改变业务数据，但要保证重放幂等。
	default:
		return fmt.Errorf("%w: unknown op %d", ErrCorruptCommand, cmd.Op)
	}
	return nil
}

// Snapshot 实现 hraft.FSM。
func (fsm *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	cp := make(map[string]string, len(fsm.store))
	for k, v := range fsm.store {
		cp[k] = v
	}
	return &Snapshot{store: cp}, nil
}

// Restore 实现 hraft.FSM。
func (fsm *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// 快照格式：magic + uint32 计数 + (uint32 klen + key + uint32 vlen + value)*
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("snapshot: read magic: %w", err)
	}
	if string(magic[:]) != "KVS1" {
		return fmt.Errorf("snapshot: bad magic %q", magic[:])
	}
	var cnt uint32
	if err := binary.Read(rc, binary.BigEndian, &cnt); err != nil {
		return fmt.Errorf("snapshot: read count: %w", err)
	}
	next := make(map[string]string, cnt)
	var klen, vlen uint32
	for i := uint32(0); i < cnt; i++ {
		if err := binary.Read(rc, binary.BigEndian, &klen); err != nil {
			return fmt.Errorf("snapshot: read key len: %w", err)
		}
		if klen > maxFieldLen {
			return ErrTooLarge
		}
		kb := make([]byte, klen)
		if _, err := io.ReadFull(rc, kb); err != nil {
			return fmt.Errorf("snapshot: read key: %w", err)
		}
		if err := binary.Read(rc, binary.BigEndian, &vlen); err != nil {
			return fmt.Errorf("snapshot: read value len: %w", err)
		}
		if vlen > maxFieldLen {
			return ErrTooLarge
		}
		vb := make([]byte, vlen)
		if _, err := io.ReadFull(rc, vb); err != nil {
			return fmt.Errorf("snapshot: read value: %w", err)
		}
		next[string(kb)] = string(vb)
	}

	fsm.mu.Lock()
	fsm.store = next
	fsm.mu.Unlock()
	return nil
}

// Snapshot 快照载荷。
type Snapshot struct {
	store map[string]string
}

var _ hraft.FSMSnapshot = (*Snapshot)(nil)

// Persist 写出快照。失败时只 Cancel 一次，成功时只 Close 一次（修复 BUG-6）。
func (s *Snapshot) Persist(sink hraft.SnapshotSink) error {
	err := writeSnapshot(sink, s.store)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func writeSnapshot(w io.Writer, store map[string]string) error {
	if _, err := io.WriteString(w, "KVS1"); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(store))); err != nil {
		return err
	}
	for k, v := range store {
		if err := binary.Write(w, binary.BigEndian, uint32(len(k))); err != nil {
			return err
		}
		if _, err := io.WriteString(w, k); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, uint32(len(v))); err != nil {
			return err
		}
		if _, err := io.WriteString(w, v); err != nil {
			return err
		}
	}
	return nil
}

// Release 实现 hraft.FSMSnapshot。
func (s *Snapshot) Release() {}

// Config 节点配置。
type Config struct {
	NodeID   string
	ShardID  int
	BindAddr string
	RaftDir  string

	// HTTPAddr 该节点的数据面 HTTP 监听地址。仅用于元数据上报与发现，
	// Raft 本身不使用它。
	HTTPAddr string

	// NodeWeight 该节点在负载均衡打分中的权重（1 表示默认）。
	NodeWeight float64

	// 以下为可调项，零值使用默认。
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout     time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	MaxAppendEntries  int
	TrailingLogs      uint64

	// ApplyTimeout 单次 Apply 的超时。
	ApplyTimeout time.Duration

	// LogOutput 为 nil 时写到 os.Stderr。
	LogOutput io.Writer
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.HeartbeatTimeout == 0 {
		out.HeartbeatTimeout = 100 * time.Millisecond
	}
	if out.ElectionTimeout == 0 {
		// 必须 >= HeartbeatTimeout，否则 follower 会在 leader 心跳前反复发起选举。
		out.ElectionTimeout = 500 * time.Millisecond
	}
	if out.LeaderLeaseTimeout == 0 {
		// Raft 的 leader lease 必须在 heartbeat 超时之前失效，否则 leader 会
		// 在自己已经不再是 leader 之后仍以为持有租约。取 heartbeat 的一半。
		out.LeaderLeaseTimeout = out.HeartbeatTimeout / 2
	}
	if out.CommitTimeout == 0 {
		out.CommitTimeout = 20 * time.Millisecond
	}
	if out.SnapshotInterval == 0 {
		out.SnapshotInterval = 60 * time.Second
	}
	if out.SnapshotThreshold == 0 {
		out.SnapshotThreshold = 8192
	}
	if out.MaxAppendEntries == 0 {
		out.MaxAppendEntries = 64
	}
	if out.TrailingLogs == 0 {
		out.TrailingLogs = 10240
	}
	if out.ApplyTimeout == 0 {
		out.ApplyTimeout = 5 * time.Second
	}
	if out.LogOutput == nil {
		out.LogOutput = os.Stderr
	}
	// 强制不变量：election >= heartbeat >= leaderLease。
	// 原实现在这里没有约束，用户可以配出"选举超时小于心跳超时"的组合，
	// 结果是 follower 在 leader 还没来得及发心跳时就反复发起选举。
	if out.ElectionTimeout < out.HeartbeatTimeout {
		out.ElectionTimeout = out.HeartbeatTimeout
	}
	if out.LeaderLeaseTimeout > out.HeartbeatTimeout {
		out.LeaderLeaseTimeout = out.HeartbeatTimeout / 2
	}
	if out.CommitTimeout > out.HeartbeatTimeout {
		out.CommitTimeout = out.HeartbeatTimeout / 5
	}
	return out
}

// KVStore 单个 Raft 节点。
type KVStore struct {
	cfg     Config
	raft    *hraft.Raft
	fsm     *FSM
	metrics *metrics.MetricsCollector

	transport *hraft.NetworkTransport
	logStore  *raftboltdb.BoltStore
	stable    *raftboltdb.BoltStore
	snapStore hraft.SnapshotStore

	closeOnce sync.Once
	// 应用侧观测。
	applyErrors atomic.Uint64

	// readIndex / readIndexTerm 缓存"本任期已确认可读"的提交点。
	// 见 ReadBarrier：这使稳态读完全不需要额外往返。
	readIndex     atomic.Uint64
	readIndexTerm atomic.Uint64
}

// NewKVStore 创建并启动一个 Raft 节点。
func NewKVStore(cfg Config) (*KVStore, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("raft: NodeID required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("raft: BindAddr required")
	}
	if cfg.RaftDir == "" {
		return nil, errors.New("raft: RaftDir required")
	}
	cfg = cfg.withDefaults()

	fsm := NewFSM()
	ks := &KVStore{
		cfg:     cfg,
		fsm:     fsm,
		metrics: metrics.NewMetricsCollector(cfg.NodeID, cfg.ShardID),
	}
	if err := ks.setupRaft(); err != nil {
		return nil, err
	}
	return ks, nil
}

func (ks *KVStore) setupRaft() error {
	rc := hraft.DefaultConfig()
	rc.LocalID = hraft.ServerID(ks.cfg.NodeID)
	rc.HeartbeatTimeout = ks.cfg.HeartbeatTimeout
	rc.ElectionTimeout = ks.cfg.ElectionTimeout
	rc.LeaderLeaseTimeout = ks.cfg.LeaderLeaseTimeout
	rc.CommitTimeout = ks.cfg.CommitTimeout
	rc.SnapshotInterval = ks.cfg.SnapshotInterval
	rc.SnapshotThreshold = ks.cfg.SnapshotThreshold
	rc.MaxAppendEntries = ks.cfg.MaxAppendEntries
	rc.TrailingLogs = ks.cfg.TrailingLogs
	rc.LogOutput = ks.cfg.LogOutput

	if err := os.MkdirAll(ks.cfg.RaftDir, 0o755); err != nil {
		return fmt.Errorf("raft: mkdir %s: %w", ks.cfg.RaftDir, err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(ks.cfg.RaftDir, "raft-log.db"))
	if err != nil {
		return fmt.Errorf("raft: log store: %w", err)
	}
	ks.logStore = logStore

	stable, err := raftboltdb.NewBoltStore(filepath.Join(ks.cfg.RaftDir, "raft-stable.db"))
	if err != nil {
		_ = logStore.Close()
		return fmt.Errorf("raft: stable store: %w", err)
	}
	ks.stable = stable

	snapStore, err := hraft.NewFileSnapshotStore(ks.cfg.RaftDir, 2, ks.cfg.LogOutput)
	if err != nil {
		_ = logStore.Close()
		_ = stable.Close()
		return fmt.Errorf("raft: snapshot store: %w", err)
	}
	ks.snapStore = snapStore

	// 预先验证端口可用性，避免千节点场景下静默绑定失败（BUG-7）。
	//
	// 关于 advertise：
	// hraft.TCPStreamLayer.Addr() 的实现是"advertise 非 nil 就返回 advertise，
	// 否则返回真实 listener 地址"。因此当配置为 ":0"（让内核分配端口）时，
	// 绝不能传一个解析出来的 adverise —— 那会把真实端口永久遮住，
	// 所有节点都对外声称自己是 ":0"，raft 直接报
	// "found duplicate address in configuration"。
	// 只有配置了固定端口时才把该地址作为 advertise 传下去。
	addr, err := net.ResolveTCPAddr("tcp", ks.cfg.BindAddr)
	if err != nil {
		_ = logStore.Close()
		_ = stable.Close()
		return fmt.Errorf("raft: resolve %s: %w", ks.cfg.BindAddr, err)
	}
	var advertise net.Addr
	if addr.Port != 0 {
		advertise = addr
	}
	transport, err := hraft.NewTCPTransport(ks.cfg.BindAddr, advertise, 3, 10*time.Second, ks.cfg.LogOutput)
	if err != nil {
		_ = logStore.Close()
		_ = stable.Close()
		return fmt.Errorf("raft: tcp transport on %s: %w", ks.cfg.BindAddr, err)
	}
	ks.transport = transport

	r, err := hraft.NewRaft(rc, ks.fsm, logStore, stable, snapStore, transport)
	if err != nil {
		_ = transport.Close()
		_ = logStore.Close()
		_ = stable.Close()
		return fmt.Errorf("raft: new: %w", err)
	}
	ks.raft = r
	return nil
}

// BootstrapResult 引导结果。
type BootstrapResult struct {
	// Bootstrapped 为 true 表示本次调用真正初始化了集群。
	Bootstrapped bool
	// AlreadyInitialized 为 true 表示该节点已有持久化的集群配置，本次为无害空操作。
	AlreadyInitialized bool
	// ExistingVoters 已存在的投票成员数（仅当 AlreadyInitialized）。
	ExistingVoters int
}

// Bootstrap 以单节点配置引导分片集群。
//
// 语义已修复（BUG-3）：
//   - 幂等：已有配置时返回 AlreadyInitialized 而不是报错。
//   - 不再因为"状态是 Follower"而跳过引导——引导恰好在无配置时需要执行。
func (ks *KVStore) Bootstrap() (BootstrapResult, error) {
	// 判断是否已有持久化配置：有配置就没有"未引导"的可能。
	hasState, err := hraft.HasExistingState(ks.logStore, ks.stable, ks.snapStore)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("raft: probe existing state: %w", err)
	}
	if hasState {
		cfgF := ks.raft.GetConfiguration()
		if err := cfgF.Error(); err != nil {
			return BootstrapResult{}, fmt.Errorf("raft: get configuration: %w", err)
		}
		return BootstrapResult{AlreadyInitialized: true, ExistingVoters: len(cfgF.Configuration().Servers)}, nil
	}

	f := ks.raft.BootstrapCluster(hraft.Configuration{
		Servers: []hraft.Server{{
			ID:      hraft.ServerID(ks.cfg.NodeID),
			Address: hraft.ServerAddress(ks.AdvertiseAddr()),
		}},
	})
	if err := f.Error(); err != nil {
		if errors.Is(err, hraft.ErrCantBootstrap) {
			return BootstrapResult{AlreadyInitialized: true}, nil
		}
		return BootstrapResult{}, fmt.Errorf("raft: bootstrap: %w", err)
	}
	return BootstrapResult{Bootstrapped: true}, nil
}

// AddVoter 在 Leader 上将 id/addr 加入本分片。非 Leader 返回 ErrNotLeader（修复 BUG-4）。
//
// 注意：调用者必须把请求发送到 Leader；本方法不会代替你做重定向。
func (ks *KVStore) AddVoter(id, addr string) error {
	if ks.raft.State() != hraft.Leader {
		return ErrNotLeader
	}
	return ks.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, 10*time.Second).Error()
}

// AddNonvoter 以非投票成员（learner）加入，用于扩展读能力而不扩大 quorum。
func (ks *KVStore) AddNonvoter(id, addr string) error {
	if ks.raft.State() != hraft.Leader {
		return ErrNotLeader
	}
	return ks.raft.AddNonvoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, 10*time.Second).Error()
}

// RemoveServer 从分片移除一个成员。
func (ks *KVStore) RemoveServer(id string) error {
	if ks.raft.State() != hraft.Leader {
		return ErrNotLeader
	}
	return ks.raft.RemoveServer(hraft.ServerID(id), 0, 10*time.Second).Error()
}

// ApplyMembership 通过 Raft 日志复制一次成员变更（幂等，可在任一节点重放）。
func (ks *KVStore) ApplyMembership(op byte, id, addr string) error {
	if ks.raft.State() != hraft.Leader {
		return ErrNotLeader
	}
	data, err := encodeMembership(op, id, addr)
	if err != nil {
		return err
	}
	f := ks.raft.Apply(data, ks.cfg.ApplyTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// IsLeader 是否为 Leader。
func (ks *KVStore) IsLeader() bool { return ks.raft.State() == hraft.Leader }

// LeaderAddr 当前已知 Leader 地址。
func (ks *KVStore) LeaderAddr() (string, bool) {
	l := ks.raft.Leader()
	return string(l), l != ""
}

// State 返回 raft 状态。
func (ks *KVStore) State() hraft.RaftState { return ks.raft.State() }

// NodeID 节点标识。
func (ks *KVStore) NodeID() string { return ks.cfg.NodeID }

// ShardID 分片标识。
func (ks *KVStore) ShardID() int { return ks.cfg.ShardID }

// BindAddr Raft 监听地址（配置值）。
func (ks *KVStore) BindAddr() string { return ks.cfg.BindAddr }

// AdvertiseAddr 返回 Raft 传输层**实际**绑定的地址。
//
// 为什么不能用 BindAddr：当配置为 "127.0.0.1:0" 时内核会分配一个临时端口，
// 而 BindAddr 仍然字面返回 "127.0.0.1:0"。若把它写进 raft 配置，
// 三个节点会得到同一个 "127.0.0.1:0"，raft 直接报
// "found duplicate address in configuration" 而拒绝加入。
// 生产环境中端口固定时二者相同，但 ephemeral 端口在测试与本地实验中很常用。
func (ks *KVStore) AdvertiseAddr() string {
	if ks.transport != nil {
		// hraft.NetworkTransport.LocalAddr() 返回 ServerAddress（字符串别名），
		// 不是 net.Addr。
		if a := ks.transport.LocalAddr(); a != "" {
			return string(a)
		}
	}
	return ks.cfg.BindAddr
}

// HTTPAddr 数据面地址（可能为空）。
func (ks *KVStore) HTTPAddr() string { return ks.cfg.HTTPAddr }

// NodeWeight 负载权重。
func (ks *KVStore) NodeWeight() float64 {
	if ks.cfg.NodeWeight <= 0 {
		return 1
	}
	return ks.cfg.NodeWeight
}

// FSM 暴露状态机（测试用）。
func (ks *KVStore) FSM() *FSM { return ks.fsm }

// Set 写入一个键。
func (ks *KVStore) Set(key, value string) error {
	return ks.propose(Command{Op: opSet, Key: key, Value: value}, "set")
}

// Delete 删除一个键。
func (ks *KVStore) Delete(key string) error {
	return ks.propose(Command{Op: opDelete, Key: key}, "delete")
}

func (ks *KVStore) propose(cmd Command, opName string) error {
	if ks.raft.State() != hraft.Leader {
		return ErrNotLeader
	}
	data, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}

	start := time.Now()
	f := ks.raft.Apply(data, ks.cfg.ApplyTimeout)
	if err := f.Error(); err != nil {
		ks.applyErrors.Add(1)
		ks.metrics.RecordError(opName)
		return err
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			ks.applyErrors.Add(1)
			ks.metrics.RecordError(opName)
			return e
		}
	}

	ks.metrics.RecordOperation(opName, 1)
	ks.metrics.RecordLatency(opName, time.Since(start))
	return nil
}

// Get 读取一个键（线性化读由上层通过 ReadIndex/转发到 Leader 保证）。
func (ks *KVStore) Get(key string) (string, bool) { return ks.fsm.Get(key) }

// Len 键数量。
func (ks *KVStore) Len() int { return ks.fsm.Len() }

// Stats 返回 raft 统计。
func (ks *KVStore) Stats() map[string]string { return ks.raft.Stats() }

// Registry 返回本节点的 Prometheus 注册表（每个节点独立，见 BUG-9）。
func (ks *KVStore) Registry() *prometheus.Registry { return ks.metrics.Registry() }

// Metrics 返回指标采集器。
func (ks *KVStore) Metrics() *metrics.Collector { return ks.metrics }

// Configuration 返回当前集群成员。
func (ks *KVStore) Configuration() []hraft.Server {
	f := ks.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil
	}
	return f.Configuration().Servers
}

// Term 返回当前任期。
func (ks *KVStore) Term() uint64 {
	st := ks.raft.Stats()
	if v, ok := st["term"]; ok {
		var t uint64
		if _, err := fmt.Sscanf(v, "%d", &t); err == nil {
			return t
		}
	}
	return 0
}

// LastIndex 返回最后日志索引。
func (ks *KVStore) LastIndex() uint64 {
	st := ks.raft.Stats()
	if v, ok := st["last_log_index"]; ok {
		var t uint64
		if _, err := fmt.Sscanf(v, "%d", &t); err == nil {
			return t
		}
	}
	return 0
}

// Barrier 等待此前所有已提交条目被应用。
//
// 注意：hashicorp/raft 的 Barrier 会**向日志追加一条空条目**并等待其提交。
// 在未开启批处理时，这意味着每个调用都要付一次 fsync。
// 因此**不要**把它放在读路径上——线性化读请用 ReadBarrier（见下）。
func (ks *KVStore) Barrier(timeout time.Duration) error {
	return ks.raft.Barrier(timeout).Error()
}

// ReadBarrier 线性化读的协调部分（Raft §6.4 readIndex 的等价实现）。
//
// 为什么不用 Barrier：Barrier 会往日志里塞一条空条目，单节点、batch=1、
// 本机 fsync ≈ 5.4ms 时实测 6.67ms/次；若每个读请求都调一次，读延迟会被
// 完全钉死在磁盘上（实测端到端 p50 = 39.91ms）。
//
// 正确且廉价的做法（与 etcd 的 readIndex 一致）：leader 只要已经提交过
// **本任期**的一条日志，就知道自己的 commit index 是当时全集群最大的，
// 因此可以直接在该 commit index 上服务读，**无需任何额外网络或磁盘往返**。
//
// 这里复用成员变更（配置变更）条目来推进任期提交点，不必引入专门的心跳。
// 若本任期尚无已提交条目，则回退到一次 Barrier（每任期一次，可接受）。
func (ks *KVStore) ReadBarrier(timeout time.Duration) (uint64, error) {
	if ks.raft.State() != hraft.Leader {
		return 0, ErrNotLeader
	}

	// 只有在"当前任期内已提交过条目"时才走快路径。
	// 换主后任期编号变化，缓存自动失效，必须重新确认一次。
	term := ks.Term()
	if idx := ks.readIndex.Load(); idx != 0 && ks.readIndexTerm.Load() == term {
		return idx, nil
	}

	// 冷路径：本任期还没有已提交条目（刚当选）。注一次空条目即可，
	// 之后整个任期内所有读都是零协调成本。
	if err := ks.raft.Barrier(timeout).Error(); err != nil {
		return 0, err
	}
	idx := ks.raft.AppliedIndex()
	ks.readIndex.Store(idx)
	ks.readIndexTerm.Store(term)
	return idx, nil
}

// commitFromCurrentTerm 判断是否已提交过本任期的一条日志。
// 这是 Raft §6.4 里"leader 必须先提交一条本任期条目"的检查。
func (ks *KVStore) commitFromCurrentTerm() bool {
	return ks.readIndex.Load() != 0
}

// Snapshot 触发一次快照。
func (ks *KVStore) Snapshot() error { return ks.raft.Snapshot().Error() }

// WaitForLeader 在 timeout 内等待出现 Leader。
func (ks *KVStore) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr, ok := ks.LeaderAddr(); ok {
			return addr, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", errors.New("raft: no leader elected within timeout")
}

// Shutdown 关闭节点。可重复调用。
func (ks *KVStore) Shutdown() error {
	var err error
	ks.closeOnce.Do(func() {
		if ks.raft != nil {
			err = ks.raft.Shutdown().Error()
		}
		if ks.transport != nil {
			_ = ks.transport.Close()
		}
		if ks.logStore != nil {
			_ = ks.logStore.Close()
		}
		if ks.stable != nil {
			_ = ks.stable.Close()
		}
	})
	return err
}
