package statelet

// Unit tests for the durable change-feed consumer (CDC Phase 5c, #824).
//
// These drive Client.SubscribeCommitted and FileCheckpointStore against a fake
// gRPC stub (no live server), mirroring the canonical Python tests (#823). They
// assert:
//   - FileCheckpointStore round-trips and writes atomically (a crash between
//     write and rename leaves the prior valid offset intact).
//   - start offset resolves to checkpoint+1 when a checkpoint exists, else
//     FromOffset.
//   - change items are delivered and (auto-commit) committed after the handler.
//   - heartbeat advances + commits the high-watermark without delivering.
//   - a compacted notice triggers a bootstrap Scan emitting synthetic snapshot
//     put changes, commits snapshot_offset, then resumes at snapshot_offset+1.
//   - an old-server snapshot_offset==0 resumes at earliest_offset.
//   - a stream error mid-stream reconnects from last_offset+1.

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
	"google.golang.org/grpc"
)

// ── fakes ────────────────────────────────────────────────────────────────────

func changeItem(offset uint64, key []byte, op string, value []byte) *pb.CommittedFeedItem {
	if op == "" {
		op = "put"
	}
	return &pb.CommittedFeedItem{Item: &pb.CommittedFeedItem_Change{Change: &pb.CommittedChangeProto{
		Offset: offset, Term: 1, SeqInEntry: 0, Cf: 0, Key: key, Op: op, Value: value,
	}}}
}

func heartbeatItem(hw uint64) *pb.CommittedFeedItem {
	return &pb.CommittedFeedItem{Item: &pb.CommittedFeedItem_Heartbeat{Heartbeat: hw}}
}

func compactedItem(earliest, snapshot uint64) *pb.CommittedFeedItem {
	return &pb.CommittedFeedItem{Item: &pb.CommittedFeedItem_Compacted{Compacted: &pb.CompactedNotice{
		EarliestOffset: earliest, SnapshotOffset: snapshot,
	}}}
}

// scriptItem is either a feed item or an error to raise while iterating.
type scriptItem struct {
	item *pb.CommittedFeedItem
	err  error
}

type scanPage struct {
	entries    [][2][]byte // (key,value) pairs
	nextCursor []byte
}

// fakeStub replays scripted streams and scan pages, recording requests.
type fakeStub struct {
	pb.StateletClient // embedded; only Scan + SubscribeCommitted are overridden
	streams          [][]scriptItem
	requests         []*pb.SubscribeCommittedRequest
	scanPages        []scanPage
	scanCalls        []*pb.ScanRequest
}

func (f *fakeStub) SubscribeCommitted(ctx context.Context, in *pb.SubscribeCommittedRequest, opts ...grpc.CallOption) (pb.Statelet_SubscribeCommittedClient, error) {
	f.requests = append(f.requests, in)
	var script []scriptItem
	if len(f.streams) > 0 {
		script = f.streams[0]
		f.streams = f.streams[1:]
	} else {
		// Nothing more scripted: a consumer that keeps looping shouldn't spin
		// forever in a test (tests stop via the handler/ctx first).
		script = []scriptItem{{err: errors.New("no more scripted streams")}}
	}
	return &fakeStream{ctx: ctx, script: script}, nil
}

func (f *fakeStub) Scan(ctx context.Context, in *pb.ScanRequest, opts ...grpc.CallOption) (*pb.ScanResponse, error) {
	f.scanCalls = append(f.scanCalls, in)
	var page scanPage
	if len(f.scanPages) > 0 {
		page = f.scanPages[0]
		f.scanPages = f.scanPages[1:]
	}
	entries := make([]*pb.ScanEntry, len(page.entries))
	for i, kv := range page.entries {
		entries[i] = &pb.ScanEntry{Key: kv[0], Value: kv[1]}
	}
	return &pb.ScanResponse{Entries: entries, NextCursor: page.nextCursor}, nil
}

// fakeStream implements pb.Statelet_SubscribeCommittedClient over a script.
type fakeStream struct {
	grpc.ClientStream
	ctx    context.Context
	script []scriptItem
	pos    int
}

func (s *fakeStream) Recv() (*pb.CommittedFeedItem, error) {
	if s.pos >= len(s.script) {
		return nil, io.EOF
	}
	it := s.script[s.pos]
	s.pos++
	if it.err != nil {
		return nil, it.err
	}
	return it.item, nil
}

func newTestClient(stub pb.StateletClient) *Client {
	return &Client{stub: stub, DefaultCF: 0}
}

// collectN runs SubscribeCommitted on a goroutine-free single-thread basis by
// stopping after n changes are delivered (the handler returns a sentinel error).
var errStop = errors.New("stop")

func collectN(t *testing.T, c *Client, opts SubscribeCommittedOptions, n int) []CommittedChange {
	t.Helper()
	var out []CommittedChange
	err := c.SubscribeCommitted(context.Background(), opts, func(ch CommittedChange) error {
		out = append(out, ch)
		if len(out) >= n {
			return errStop
		}
		return nil
	})
	if n > 0 && !errors.Is(err, errStop) {
		t.Fatalf("expected stop after %d changes, got err=%v (collected %d)", n, err, len(out))
	}
	return out
}

// ── FileCheckpointStore ──────────────────────────────────────────────────────

func TestFileCheckpointRoundtrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ck.json")
	store, err := NewFileCheckpointStore(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := store.Load("sub-a"); ok {
		t.Fatal("expected no checkpoint for sub-a")
	}
	if err := store.Commit("sub-a", 7); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("sub-b", 11); err != nil {
		t.Fatal(err)
	}
	if off, ok, _ := store.Load("sub-a"); !ok || off != 7 {
		t.Fatalf("sub-a = (%d,%v), want (7,true)", off, ok)
	}
	// Re-open from disk: state survives.
	store2, err := NewFileCheckpointStore(p)
	if err != nil {
		t.Fatal(err)
	}
	if off, ok, _ := store2.Load("sub-b"); !ok || off != 11 {
		t.Fatalf("sub-b after reopen = (%d,%v), want (11,true)", off, ok)
	}
}

func TestFileCheckpointAtomicNoTmpLeftover(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "ck.json")
	store, _ := NewFileCheckpointStore(p)
	_ = store.Commit("s", 3)
	_ = store.Commit("s", 4)
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if len(e.Name()) >= 6 && e.Name()[:6] == ".ckpt-" {
			t.Fatalf("leftover temp file: %s", e.Name())
		}
	}
	reopened, _ := NewFileCheckpointStore(p)
	if off, _, _ := reopened.Load("s"); off != 4 {
		t.Fatalf("s = %d, want 4", off)
	}
}

func TestFileCheckpointCorruptFileIsEmpty(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ck.json")
	if err := os.WriteFile(p, []byte("{ this is not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileCheckpointStore(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := store.Load("s"); ok {
		t.Fatal("corrupt file should be treated as empty")
	}
}

// ── start-offset resolution ──────────────────────────────────────────────────

func TestStartFromCheckpointPlusOne(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	_ = ck.Commit("sub", 41)
	stub := &fakeStub{streams: [][]scriptItem{{{item: changeItem(42, []byte("k"), "put", nil)}}}}
	c := newTestClient(stub)
	collectN(t, c, SubscribeCommittedOptions{SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true}, 1)
	if stub.requests[0].FromOffset != 42 {
		t.Fatalf("from_offset = %d, want 42 (checkpoint 41 + 1)", stub.requests[0].FromOffset)
	}
}

func TestStartFromOffsetWhenNoCheckpoint(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{streams: [][]scriptItem{{{item: changeItem(100, []byte("k"), "put", nil)}}}}
	c := newTestClient(stub)
	collectN(t, c, SubscribeCommittedOptions{FromOffset: 100, SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true}, 1)
	if stub.requests[0].FromOffset != 100 {
		t.Fatalf("from_offset = %d, want 100", stub.requests[0].FromOffset)
	}
}

// ── change delivery + auto-commit ────────────────────────────────────────────

func TestChangeDeliveredAndCommittedAfterHandler(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{streams: [][]scriptItem{{
		{item: changeItem(5, []byte("a"), "put", []byte("v"))},
		{item: changeItem(6, []byte("b"), "put", nil)},
	}}}
	c := newTestClient(stub)
	got := collectN(t, c, SubscribeCommittedOptions{SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true}, 2)
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2", len(got))
	}
	g := got[0]
	if g.Offset != 5 || string(g.Key) != "a" || g.Op != "put" || string(g.Value) != "v" || g.IsSnapshot {
		t.Fatalf("first change = %+v", g)
	}
	// After delivering offset 5 (and before the handler stopped on 6), offset 5
	// was committed.
	if off, ok, _ := ck.Load("sub"); !ok || off != 5 {
		t.Fatalf("committed offset = (%d,%v), want (5,true)", off, ok)
	}
}

func TestNoCommitWhenAutoCommitFalse(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{streams: [][]scriptItem{{
		{item: changeItem(5, []byte("a"), "put", nil)},
		{item: changeItem(6, []byte("b"), "put", nil)},
	}}}
	c := newTestClient(stub)
	collectN(t, c, SubscribeCommittedOptions{SubscriptionID: "sub", Checkpoint: ck, AutoCommit: false}, 2)
	if _, ok, _ := ck.Load("sub"); ok {
		t.Fatal("auto_commit=false must not commit")
	}
}

// ── heartbeat ────────────────────────────────────────────────────────────────

func TestHeartbeatAdvancesAndCommitsWithoutDelivery(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{streams: [][]scriptItem{{
		{item: heartbeatItem(50)},
		{item: changeItem(51, []byte("k"), "put", nil)},
	}}}
	c := newTestClient(stub)
	got := collectN(t, c, SubscribeCommittedOptions{SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true}, 1)
	if got[0].Offset != 51 {
		t.Fatalf("first delivered offset = %d, want 51 (heartbeat not delivered)", got[0].Offset)
	}
	// Heartbeat committed its watermark before the change was delivered.
	if off, ok, _ := ck.Load("sub"); !ok || off != 50 {
		t.Fatalf("committed watermark = (%d,%v), want (50,true)", off, ok)
	}
}

// ── compaction bootstrap ─────────────────────────────────────────────────────

func TestCompactedTriggersBootstrapScan(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{
		streams: [][]scriptItem{
			{{item: compactedItem(10, 20)}},
			{{item: changeItem(21, []byte("live"), "put", nil)}},
		},
		scanPages: []scanPage{
			{entries: [][2][]byte{{[]byte("k1"), []byte("v1")}}, nextCursor: []byte("cursor1")},
			{entries: [][2][]byte{{[]byte("k2"), []byte("v2")}}, nextCursor: nil},
		},
	}
	c := newTestClient(stub)
	cf := uint32(0)
	got := collectN(t, c, SubscribeCommittedOptions{
		SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true, KeyPrefix: []byte("k"), CF: &cf,
	}, 3)
	// Two synthetic snapshot puts then the live tail.
	if !got[0].IsSnapshot || got[0].Op != "put" || got[0].Offset != 20 || string(got[0].Key) != "k1" || string(got[0].Value) != "v1" {
		t.Fatalf("snap1 = %+v", got[0])
	}
	if !got[1].IsSnapshot || got[1].Offset != 20 || string(got[1].Key) != "k2" {
		t.Fatalf("snap2 = %+v", got[1])
	}
	if got[2].Offset != 21 || got[2].IsSnapshot {
		t.Fatalf("live = %+v, want offset 21 non-snapshot", got[2])
	}
	if off, ok, _ := ck.Load("sub"); !ok || off != 20 {
		t.Fatalf("committed offset = (%d,%v), want (20,true) snapshot_offset", off, ok)
	}
	if stub.requests[1].FromOffset != 21 {
		t.Fatalf("resume from_offset = %d, want 21 (snapshot_offset+1)", stub.requests[1].FromOffset)
	}
}

func TestCompactedOldServerResumesAtEarliest(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{streams: [][]scriptItem{
		{{item: compactedItem(99, 0)}},
		{{item: changeItem(99, []byte("k"), "put", nil)}},
	}}
	c := newTestClient(stub)
	got := collectN(t, c, SubscribeCommittedOptions{SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true}, 1)
	if got[0].Offset != 99 {
		t.Fatalf("offset = %d, want 99", got[0].Offset)
	}
	if len(stub.scanCalls) != 0 {
		t.Fatalf("old-server path must not bootstrap scan, got %d scan calls", len(stub.scanCalls))
	}
	if stub.requests[1].FromOffset != 99 {
		t.Fatalf("resume from_offset = %d, want 99 (earliest_offset)", stub.requests[1].FromOffset)
	}
}

// ── reconnect ────────────────────────────────────────────────────────────────

func TestStreamErrorReconnectsFromLastPlusOne(t *testing.T) {
	ck, _ := NewFileCheckpointStore(filepath.Join(t.TempDir(), "ck.json"))
	stub := &fakeStub{streams: [][]scriptItem{
		{{item: changeItem(5, []byte("a"), "put", nil)}, {err: errors.New("disconnect")}},
		{{item: changeItem(6, []byte("b"), "put", nil)}},
	}}
	c := newTestClient(stub)
	got := collectN(t, c, SubscribeCommittedOptions{SubscriptionID: "sub", Checkpoint: ck, AutoCommit: true}, 2)
	if got[0].Offset != 5 || got[1].Offset != 6 {
		t.Fatalf("offsets = %d,%d, want 5,6", got[0].Offset, got[1].Offset)
	}
	if stub.requests[1].FromOffset != 6 {
		t.Fatalf("reconnect from_offset = %d, want 6 (last+1)", stub.requests[1].FromOffset)
	}
}
