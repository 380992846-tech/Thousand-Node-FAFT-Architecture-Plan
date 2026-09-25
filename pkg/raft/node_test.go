package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"

	hraft "github.com/hashicorp/raft"
)

// TestCommandRoundTrip 验证命令编解码往返一致。
func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: opSet, Key: "k", Value: "v"},
		{Op: opDelete, Key: "k"},
		{Op: opSet, Key: "", Value: ""},
		{Op: opSet, Key: "键", Value: "值"}, // 多字节 UTF-8
		{Op: opSet, Key: "a/b?c#d%e", Value: "x"},
	}
	for _, want := range cases {
		data, err := EncodeCommand(want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := DecodeCommand(data)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got.Op != want.Op || got.Key != want.Key || got.Value != want.Value {
			t.Errorf("round trip mismatch: got %+v want %+v", got, want)
		}
	}
}

// TestDecodeRejectsGarbage 验证解码器不被畸形输入欺骗。
//
// 这是 BUG-5 的回归测试：原实现用 gob，任何合法的 gob 流（包括别的类型）
// 都能被解码进来。
func TestDecodeRejectsGarbage(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{1},
		{1, 2, 3},
		bytes.Repeat([]byte{0xff}, 16),
		// 合法头但长度前缀超出实际数据
		func() []byte {
			b := make([]byte, cmdHeaderLen)
			b[0] = opSet
			binary.BigEndian.PutUint32(b[5:9], 1<<30)
			binary.BigEndian.PutUint32(b[9:13], 1<<30)
			return b
		}(),
	}
	for i, c := range cases {
		if _, err := DecodeCommand(c); err == nil {
			t.Errorf("case %d: 期望解码失败，却成功了", i)
		}
	}
}

// TestDecodeRejectsCorruptChecksum 验证 CRC 校验真的生效。
func TestDecodeRejectsCorruptChecksum(t *testing.T) {
	data, err := EncodeCommand(Command{Op: opSet, Key: "key", Value: "value"})
	if err != nil {
		t.Fatal(err)
	}
	// 翻转一个数据字节，CRC 应当不匹配。
	data[len(data)-1] ^= 0xff
	if _, err := DecodeCommand(data); err == nil {
		t.Fatal("篡改数据后仍然解码成功，CRC 校验未生效")
	} else if !errors.Is(err, ErrCorruptCommand) {
		t.Fatalf("期望 ErrCorruptCommand，得到 %v", err)
	}
}

// TestEncodeDeterministic 验证编码是确定性的（同输入 => 同字节）。
//
// gob 不保证这一点（含 map 迭代序、类型描述），这是 BUG-5 的核心动机之一。
func TestEncodeDeterministic(t *testing.T) {
	cmd := Command{Op: opSet, Key: "hello", Value: "world"}
	first, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		again, err := EncodeCommand(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("第 %d 次编码结果不同", i)
		}
	}
}

// TestFSMConcurrentReadWrite 是 BUG-1 的回归测试。
//
// 原实现的 KVStore.Get 读的是 fsm.store，却只持有 ks.mu —— 两把不相干的锁。
// 在 -race 下必然报数据竞争；不加 -race 时表现为偶发读到撕裂状态或直接 panic
// （并发读写 Go map 是未定义行为）。
func TestFSMConcurrentReadWrite(t *testing.T) {
	fsm := NewFSM()

	// 先放一些数据，保证读路径有内容可读。
	const writers, readers, iters = 4, 8, 500

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				data, err := EncodeCommand(Command{Op: opSet, Key: keyOf(id, i), Value: "v"})
				if err != nil {
					t.Error(err)
					return
				}
				_ = fsm.Apply(&hraft.Log{Data: data})
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				fsm.Get(keyOf(id%writers, i))
				_ = fsm.Len()
			}
		}(r)
	}
	wg.Wait()

	if got := fsm.Len(); got != writers*iters {
		t.Fatalf("键数量 = %d，期望 %d", got, writers*iters)
	}
	if got := fsm.Applied(); got != uint64(writers*iters) {
		t.Fatalf("Applied = %d，期望 %d", got, writers*iters)
	}
}

func keyOf(a, b int) string {
	return string(rune('a'+a%26)) + "-" + itoa(b)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// TestFSMApplyRejectsCorrupt 验证状态机拒绝损坏日志而不是污染数据。
func TestFSMApplyRejectsCorrupt(t *testing.T) {
	fsm := NewFSM()
	resp := fsm.Apply(&hraft.Log{Data: []byte{0xde, 0xad, 0xbe, 0xef}})
	if _, ok := resp.(error); !ok {
		t.Fatalf("期望 Apply 返回 error，得到 %T (%v)", resp, resp)
	}
	if fsm.Len() != 0 {
		t.Fatalf("损坏日志不应改变状态机，当前键数 = %d", fsm.Len())
	}
}

// TestFSMSnapshotRestore 验证快照往返，包括空值与多字节键。
func TestFSMSnapshotRestore(t *testing.T) {
	fsm := NewFSM()
	want := map[string]string{
		"a":    "1",
		"":     "empty-key",
		"键":    "值",
		"k/../x": "traversal-ish",
	}
	for k, v := range want {
		data, _ := EncodeCommand(Command{Op: opSet, Key: k, Value: v})
		fsm.Apply(&hraft.Log{Data: data})
	}

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeSnapshot(&buf, snap.(*Snapshot).store); err != nil {
		t.Fatal(err)
	}

	restored := NewFSM()
	if err := restored.Restore(&readCloser{&buf}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if restored.Len() != len(want) {
		t.Fatalf("恢复后键数 = %d，期望 %d", restored.Len(), len(want))
	}
	for k, v := range want {
		got, ok := restored.Get(k)
		if !ok || got != v {
			t.Errorf("key %q: got (%q,%v) want (%q,true)", k, got, ok, v)
		}
	}
}

// TestFSMRestoreRejectsBadMagic 验证快照格式校验。
func TestFSMRestoreRejectsBadMagic(t *testing.T) {
	fsm := NewFSM()
	bad := bytes.NewReader([]byte("XXXX\x00\x00\x00\x00"))
	if err := fsm.Restore(&readCloser{bad}); err == nil {
		t.Fatal("期望 bad magic 被拒绝")
	}
}

// TestFSMRestoreTruncated 验证截断快照不会导致 panic。
func TestFSMRestoreTruncated(t *testing.T) {
	// 声明 5 个条目但只给 1 个。
	var buf bytes.Buffer
	buf.WriteString("KVS1")
	_ = binary.Write(&buf, binary.BigEndian, uint32(5))
	_ = binary.Write(&buf, binary.BigEndian, uint32(1))
	buf.WriteString("k")

	fsm := NewFSM()
	if err := fsm.Restore(&readCloser{&buf}); err == nil {
		t.Fatal("期望截断快照被拒绝")
	}
}

// TestConfigDefaultsElectionGEHeartbeat 验证选举超时不低于心跳超时。
//
// BUG-7 相关：原来 HeartbeatTimeout=100ms 而 ElectionTimeout=1s 尚可，
// 但如果用户把 heartbeat 调大而不动 election，follower 会在 leader 心跳前
// 反复发起选举，导致集群持续抖动。
func TestConfigDefaultsElectionGEHeartbeat(t *testing.T) {
	base := Config{NodeID: "n", BindAddr: "127.0.0.1:1", RaftDir: t.TempDir()}
	c := base.withDefaults()
	if c.ElectionTimeout < c.HeartbeatTimeout {
		t.Fatalf("ElectionTimeout(%s) < HeartbeatTimeout(%s)", c.ElectionTimeout, c.HeartbeatTimeout)
	}
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		t.Fatalf("LeaderLeaseTimeout(%s) > HeartbeatTimeout(%s)：lease 不可能长于心跳",
			c.LeaderLeaseTimeout, c.HeartbeatTimeout)
	}
}

// TestNewKVStoreValidatesRequired 验证必填参数被拒绝。
func TestNewKVStoreValidatesRequired(t *testing.T) {
	cases := []Config{
		{BindAddr: "127.0.0.1:1", RaftDir: "x"},              // 缺 NodeID
		{NodeID: "n", RaftDir: "x"},                           // 缺 BindAddr
		{NodeID: "n", BindAddr: "127.0.0.1:1"},                // 缺 RaftDir
	}
	for i, c := range cases {
		if _, err := NewKVStore(c); err == nil {
			t.Errorf("case %d (%+v): 期望报错", i, c)
		}
	}
}

// readCloser 把任意 Reader 包装成 io.ReadCloser。
type readCloser struct{ r io.Reader }

func (rc *readCloser) Read(p []byte) (int, error) { return rc.r.Read(p) }
func (rc *readCloser) Close() error               { return nil }
