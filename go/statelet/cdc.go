package statelet

// Durable change-feed (CDC) — issue #824 (CDC epic #692, Phase 5c).
//
// This mirrors the canonical Python consumer (#823, Phase 5b) with identical
// semantics: Kafka-style client-managed committed offsets, a pluggable
// CheckpointStore with a file-backed atomic default, bootstrap-on-compacted
// (paged Scan baseline then resume from snapshot_offset+1),
// heartbeat-advances-checkpoint, reconnect-on-disconnect from last_offset+1,
// and at-least-once delivery with idempotency required on the boundary.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
)

// CheckpointStore is a pluggable store for a consumer's last fully-processed
// CDC offset.
//
// It implements Kafka-style client-managed offsets: the consumer commits the
// offset *after* a change has been processed (at-least-once). Apps may supply a
// custom store that persists the offset transactionally with their own sink to
// achieve exactly-once-sink semantics.
type CheckpointStore interface {
	// Load returns the last fully-processed offset for subscriptionID and
	// whether a checkpoint exists. ok is false when nothing has been committed.
	Load(subscriptionID string) (offset uint64, ok bool, err error)
	// Commit durably records offset as fully processed for subscriptionID.
	Commit(subscriptionID string, offset uint64) error
}

// FileCheckpointStore is the default CheckpointStore backed by an
// atomically-written JSON file.
//
// The file maps subscriptionID -> offset. Each commit rewrites the file via a
// temp file + os.Rename, which is atomic on POSIX, so a crash between the write
// and the rename leaves the previous valid offset intact (never a torn/partial
// file).
type FileCheckpointStore struct {
	path  string
	mu    sync.Mutex
	state map[string]uint64
}

// NewFileCheckpointStore opens (or initializes) a file-backed checkpoint store
// at path. A missing or corrupt file is treated as "no checkpoint"; a prior
// atomic commit could never produce a corrupt file.
func NewFileCheckpointStore(path string) (*FileCheckpointStore, error) {
	s := &FileCheckpointStore{path: path, state: map[string]uint64{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, nil
		}
		// Unreadable file: start empty rather than fail to open.
		return s, nil
	}
	var loaded map[string]uint64
	if err := json.Unmarshal(data, &loaded); err == nil && loaded != nil {
		s.state = loaded
	}
	// Corrupt JSON: keep the empty map (treat as "no checkpoint").
	return s, nil
}

// Load returns the committed offset for subscriptionID, if any.
func (s *FileCheckpointStore) Load(subscriptionID string) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off, ok := s.state[subscriptionID]
	return off, ok, nil
}

// Commit atomically records offset for subscriptionID.
func (s *FileCheckpointStore) Commit(subscriptionID string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[subscriptionID] = offset

	dir := filepath.Dir(s.path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".ckpt-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup of the temp file on any failure path.
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// CommittedChange is a single change delivered by SubscribeCommitted.
//
// Offset is the stable Raft log index (the resume key). IsSnapshot is true for
// synthetic put changes produced by a bootstrap Scan after the requested offset
// was compacted away; those all carry the same SnapshotOffset and
// Term=0/SeqInEntry=0.
type CommittedChange struct {
	Offset     uint64
	Term       uint64
	SeqInEntry uint32
	CF         uint32
	Key        []byte
	Op         string // "put" | "delete" | "merge"
	Value      []byte
	IsSnapshot bool
}

// SubscribeCommittedOptions configures a durable change-feed consumer.
//
// CF uses *uint32 so that nil means "the client's default CF" while an explicit
// 0 selects CF 0 (matching the Python keyword default of cf=None).
type SubscribeCommittedOptions struct {
	FromOffset     uint64 // first Raft offset when no checkpoint applies (0 = live-only)
	CF             *uint32
	KeyPrefix      []byte
	IncludeValues  bool
	SubscriptionID string          // checkpoint key
	Checkpoint     CheckpointStore // resume/commit store
	AutoCommit     bool            // commit each offset after the item is handled
	ShardID        uint64
}

// Bounded exponential backoff for reconnect.
const (
	cdcBackoffBase = 500 * time.Millisecond
	cdcBackoffMax  = 30 * time.Second
)

// SubscribeCommitted consumes the durable, ordered, resumable committed
// change-feed (CDC), invoking handler for each CommittedChange in stable
// Raft-offset order.
//
// Offsets are client-managed (Kafka-style): supply opts.SubscriptionID and
// opts.Checkpoint to resume across restarts. With opts.AutoCommit=true each
// change's offset is committed *after* handler returns nil — giving
// at-least-once delivery. With AutoCommit=false the caller commits offsets via
// opts.Checkpoint.Commit(subID, change.Offset).
//
// Start offset resolution: if both SubscriptionID and Checkpoint are given and a
// checkpoint exists, the stream starts at checkpoint+1; otherwise it starts at
// opts.FromOffset.
//
// Resilience:
//   - On disconnect / stream error, the consumer reconnects from last_offset+1
//     (the server replays from the durable log) with bounded exponential
//     backoff. SubscribeCommitted runs until ctx is cancelled or handler returns
//     a non-nil error.
//   - On a compaction notice (the requested offset fell below the compaction
//     floor), the feed bootstraps: it pages a full Scan over KeyPrefix (within
//     CF), invoking handler for each (key,value) as a synthetic put
//     CommittedChange with IsSnapshot=true and Offset=snapshot_offset, commits
//     snapshot_offset, then resumes live-tail from snapshot_offset+1. Against an
//     old server that reports snapshot_offset==0 it falls back to resuming at
//     earliest_offset.
//
// handler returning a non-nil error stops the consumer and that error is
// returned. A nil return from handler signals successful processing (and, under
// AutoCommit, triggers the commit of that offset).
func (c *Client) SubscribeCommitted(
	ctx context.Context,
	opts SubscribeCommittedOptions,
	handler func(CommittedChange) error,
) error {
	effectiveCF := c.DefaultCF
	if opts.CF != nil {
		effectiveCF = *opts.CF
	}
	doCommit := opts.Checkpoint != nil && opts.SubscriptionID != "" && opts.AutoCommit

	commit := func(offset uint64) error {
		if doCommit {
			return opts.Checkpoint.Commit(opts.SubscriptionID, offset)
		}
		return nil
	}

	// Resolve the resume start: checkpoint+1 wins over FromOffset.
	nextOffset := opts.FromOffset
	if opts.Checkpoint != nil && opts.SubscriptionID != "" {
		if saved, ok, err := opts.Checkpoint.Load(opts.SubscriptionID); err != nil {
			return err
		} else if ok {
			nextOffset = saved + 1
		}
	}

	// Track the highest offset observed so reconnect can resume.
	var lastOffset uint64
	if nextOffset > 0 {
		lastOffset = nextOffset - 1
	}
	backoff := cdcBackoffBase

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		req := &pb.SubscribeCommittedRequest{
			ShardId:       opts.ShardID,
			FromOffset:    nextOffset,
			Cf:            effectiveCF,
			KeyPrefix:     opts.KeyPrefix,
			IncludeValues: opts.IncludeValues,
		}
		stream, err := c.stub.SubscribeCommitted(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			nextOffset = lastOffset + 1
			if sleepErr := cdcSleep(ctx, backoff); sleepErr != nil {
				return sleepErr
			}
			backoff = nextBackoff(backoff)
			continue
		}

		streamErr := false
		compacted := false
		for {
			item, recvErr := stream.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					// Server closed the stream cleanly. Reconnect to continue
					// tailing from the next offset.
					nextOffset = lastOffset + 1
				} else {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					streamErr = true
				}
				break
			}

			switch payload := item.GetItem().(type) {
			case *pb.CommittedFeedItem_Change:
				ch := payload.Change
				change := CommittedChange{
					Offset:     ch.GetOffset(),
					Term:       ch.GetTerm(),
					SeqInEntry: ch.GetSeqInEntry(),
					CF:         ch.GetCf(),
					Key:        ch.GetKey(),
					Op:         ch.GetOp(),
					Value:      ch.GetValue(),
					IsSnapshot: false,
				}
				if hErr := handler(change); hErr != nil {
					return hErr
				}
				if ch.GetOffset() > lastOffset {
					lastOffset = ch.GetOffset()
				}
				nextOffset = lastOffset + 1
				if cErr := commit(ch.GetOffset()); cErr != nil {
					return cErr
				}
			case *pb.CommittedFeedItem_Heartbeat:
				// Filtered consumer matched nothing up to this watermark;
				// advance + commit so a later resume skips the gap.
				hw := payload.Heartbeat
				if hw > lastOffset {
					lastOffset = hw
					nextOffset = hw + 1
					if cErr := commit(hw); cErr != nil {
						return cErr
					}
				}
			case *pb.CommittedFeedItem_Compacted:
				snapshotOffset := payload.Compacted.GetSnapshotOffset()
				earliestOffset := payload.Compacted.GetEarliestOffset()
				if snapshotOffset > 0 {
					// Rebuild baseline from a full prefix scan, then resume
					// strictly after the snapshot watermark.
					if bErr := c.bootstrapScan(
						ctx, opts.KeyPrefix, effectiveCF, snapshotOffset, handler,
					); bErr != nil {
						return bErr
					}
					if snapshotOffset > lastOffset {
						lastOffset = snapshotOffset
					}
					if cErr := commit(snapshotOffset); cErr != nil {
						return cErr
					}
					nextOffset = snapshotOffset + 1
				} else {
					// Old server: no state-machine watermark available; resume
					// at the compaction floor (pre-Phase-5a behavior).
					nextOffset = earliestOffset
					if earliestOffset > 0 && earliestOffset-1 > lastOffset {
						lastOffset = earliestOffset - 1
					}
				}
				compacted = true
			}
			if compacted {
				break // reconnect from the resolved nextOffset
			}
		}

		if streamErr {
			// Disconnect: resume from the next unprocessed offset after a
			// bounded exponential backoff. at-least-once on reconnect.
			nextOffset = lastOffset + 1
			if sleepErr := cdcSleep(ctx, backoff); sleepErr != nil {
				return sleepErr
			}
			backoff = nextBackoff(backoff)
			continue
		}
		// A clean stream end / compaction reconnect is not a failure; reset
		// backoff so transient cleans don't accumulate delay.
		backoff = cdcBackoffBase
	}
}

// bootstrapScan pages the whole prefix and invokes handler with synthetic
// snapshot put changes.
func (c *Client) bootstrapScan(
	ctx context.Context,
	keyPrefix []byte,
	cf uint32,
	snapshotOffset uint64,
	handler func(CommittedChange) error,
) error {
	var cursor []byte
	for {
		entries, next, err := c.Scan(ctx, keyPrefix, cursor, 500, &cf)
		if err != nil {
			return err
		}
		for _, e := range entries {
			change := CommittedChange{
				Offset:     snapshotOffset,
				Term:       0,
				SeqInEntry: 0,
				CF:         cf,
				Key:        e.Key,
				Op:         "put",
				Value:      e.Value,
				IsSnapshot: true,
			}
			if hErr := handler(change); hErr != nil {
				return hErr
			}
		}
		if next == nil {
			return nil
		}
		cursor = next
	}
}

func nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > cdcBackoffMax {
		return cdcBackoffMax
	}
	if next <= 0 || next == time.Duration(math.MaxInt64) {
		return cdcBackoffMax
	}
	return next
}

// cdcSleep waits for d or until ctx is cancelled, returning ctx.Err() if
// cancelled.
func cdcSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
