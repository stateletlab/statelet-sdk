// Copyright 2024 Statelet Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statelet

// Agent state: the causal graph, branches, reactive state, coordination
// leases, temporal edges and the prefix watch stream.
//
// These wrap AgentStateService (the KV surface in client.go wraps Statelet);
// both are served by the gateway on the same port, so one Client speaks both.
// The surface mirrors the canonical Python client (sdk/python/statelet's
// high_level.Client) method for method.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
)

// ── Enumerations carried on the wire as strings ─────────────────────────────

// StepType is the kind of a causal step. The server parses these names
// exactly; any other value is rejected with InvalidArgument.
type StepType string

const (
	StepObserve StepType = "Observe"
	StepThink   StepType = "Think"
	StepAct     StepType = "Act"
	StepTool    StepType = "Tool"
	StepResult  StepType = "Result"
)

// Valid reports whether s is one of the five server-recognized step types.
func (s StepType) Valid() bool {
	switch s {
	case StepObserve, StepThink, StepAct, StepTool, StepResult:
		return true
	}
	return false
}

// EdgeType is the kind of a causal edge.
type EdgeType string

const (
	// EdgeTriggers: A directly causes B.
	EdgeTriggers EdgeType = "Triggers"
	// EdgeInforms: A provides context to B.
	EdgeInforms EdgeType = "Informs"
	// EdgeBranches: fork point, A branches into B.
	EdgeBranches EdgeType = "Branches"
	// EdgeMerges: join point, A merges back from B.
	EdgeMerges EdgeType = "Merges"
	// EdgeSupersedes: fact A retired fact B (B's ValidTo is closed).
	EdgeSupersedes EdgeType = "Supersedes"
	// EdgeDerivedFrom: provenance, fact A was derived from episode/step B.
	EdgeDerivedFrom EdgeType = "DerivedFrom"
	// EdgeContradicts: asserted conflict, both kept; the bitemporal as-of
	// filter decides which is valid at query time.
	EdgeContradicts EdgeType = "Contradicts"
)

// Valid reports whether e is one of the seven server-recognized edge types.
func (e EdgeType) Valid() bool {
	switch e {
	case EdgeTriggers, EdgeInforms, EdgeBranches, EdgeMerges,
		EdgeSupersedes, EdgeDerivedFrom, EdgeContradicts:
		return true
	}
	return false
}

// Direction selects which way an edge query or traversal walks.
type Direction string

const (
	// Forward walks src -> dst.
	Forward Direction = "forward"
	// Backward walks dst -> src.
	Backward Direction = "backward"
	// Both walks either way. Accepted by Traverse only — GetEdges rejects it.
	Both Direction = "both"
)

// MemoryScope is a step's query-time access boundary (issue #697).
type MemoryScope string

const (
	// ScopeWorld is visible to every authenticated agent in the tenant. This
	// is the default an empty scope decodes to.
	ScopeWorld MemoryScope = "world"
	// ScopeTeam is visible to agents holding the owning team's grant;
	// ScopeOwner is the team id.
	ScopeTeam MemoryScope = "team"
	// ScopePrivate is visible only to the owning agent; ScopeOwner is the
	// agent id.
	ScopePrivate MemoryScope = "private"
)

// ── Result types ────────────────────────────────────────────────────────────

// FieldRule is one field-level ACL entry on a step's metadata JSON.
type FieldRule struct {
	// JSONPointer is an RFC-6901 pointer into the step's metadata JSON.
	JSONPointer string
	// MinScope is the minimum scope a caller must satisfy to see the field;
	// otherwise it is stripped from the returned metadata.
	MinScope MemoryScope
}

// Step is a node in the causal DAG, decoded from the server's JSON encoding.
//
// Raw carries the exact bytes the server sent, so a field added server-side is
// still reachable before this struct grows to match.
type Step struct {
	ID      uint64
	AgentID string
	Type    StepType
	// Timestamp is ms since the Unix epoch.
	Timestamp uint64
	// BranchID is 0 on the main timeline.
	BranchID uint64
	// EmbeddingID is nil when the step carries no embedding.
	EmbeddingID *uint64
	// Metadata is arbitrary caller bytes (JSON or msgpack).
	Metadata []byte
	Scope    MemoryScope
	// ScopeOwner is the team id for ScopeTeam, the agent id for ScopePrivate,
	// empty for ScopeWorld.
	ScopeOwner string
	FieldACL   []FieldRule
	Raw        []byte
}

// stepJSON mirrors the serde encoding of the engine's CausalStep. `metadata`
// is a Vec<u8>, which serde_json writes as an array of numbers — decoding it
// into a Go []byte would fail (encoding/json expects base64 there), hence the
// []int hop.
type stepJSON struct {
	ID          uint64  `json:"id"`
	AgentID     string  `json:"agent_id"`
	StepType    string  `json:"step_type"`
	Timestamp   uint64  `json:"timestamp"`
	BranchID    uint64  `json:"branch_id"`
	EmbeddingID *uint64 `json:"embedding_id"`
	Metadata    []int   `json:"metadata"`
	Scope       struct {
		Scope    string `json:"scope"`
		Owner    string `json:"owner"`
		FieldACL []struct {
			JSONPointer string `json:"json_pointer"`
			MinScope    string `json:"min_scope"`
		} `json:"field_acl"`
	} `json:"scope"`
}

func decodeStep(raw []byte) (Step, error) {
	if len(raw) == 0 {
		return Step{}, nil
	}
	var s stepJSON
	if err := json.Unmarshal(raw, &s); err != nil {
		return Step{Raw: raw}, fmt.Errorf("statelet: decode step json: %w", err)
	}
	step := Step{
		ID:          s.ID,
		AgentID:     s.AgentID,
		Type:        StepType(s.StepType),
		Timestamp:   s.Timestamp,
		BranchID:    s.BranchID,
		EmbeddingID: s.EmbeddingID,
		Scope:       MemoryScope(s.Scope.Scope),
		ScopeOwner:  s.Scope.Owner,
		Raw:         raw,
	}
	if s.Scope.Scope == "" {
		// A step written before scope tagging decodes as world-readable.
		step.Scope = ScopeWorld
	}
	if len(s.Metadata) > 0 {
		step.Metadata = make([]byte, len(s.Metadata))
		for i, b := range s.Metadata {
			step.Metadata[i] = byte(b)
		}
	}
	for _, r := range s.Scope.FieldACL {
		step.FieldACL = append(step.FieldACL, FieldRule{
			JSONPointer: r.JSONPointer,
			MinScope:    MemoryScope(r.MinScope),
		})
	}
	return step, nil
}

func decodeSteps(raws [][]byte) ([]Step, error) {
	steps := make([]Step, 0, len(raws))
	for _, raw := range raws {
		step, err := decodeStep(raw)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, nil
}

// Edge is one causal edge as seen from a queried step.
type Edge struct {
	// PeerStepID is the step at the other end (dst for a forward query, src
	// for a backward one).
	PeerStepID uint64
	Type       EdgeType
	Props      []byte
	// ValidFrom / ValidTo bound the edge's valid time in ms; ValidTo 0 means
	// the edge is still open.
	ValidFrom uint64
	ValidTo   uint64
}

func decodeEdges(in []*pb.AgentEdgeProto) []Edge {
	edges := make([]Edge, 0, len(in))
	for _, e := range in {
		edges = append(edges, Edge{
			PeerStepID: e.PeerStepId,
			Type:       EdgeType(e.EdgeType),
			Props:      e.Props,
			ValidFrom:  e.ValidFrom,
			ValidTo:    e.ValidTo,
		})
	}
	return edges
}

// TraverseResult is the sub-graph a traversal reached.
type TraverseResult struct {
	Steps []Step
	Edges []Edge
}

// CausalChain is one chain returned by FindSimilarChains: the anchor step the
// query embedding matched, plus the sub-graph walked from it.
type CausalChain struct {
	Anchor   Step
	Distance float32
	Steps    []Step
	Edges    []Edge
}

// BranchMeta describes one fork branch.
type BranchMeta struct {
	ID                uint64
	ParentID          uint64
	ParentSnapshotSeq uint64
	CreatedAt         uint64
	// Status is "Active", "Merged" or "Discarded".
	Status string
	Label  string
}

// CasPutResult reports the outcome of a compare-and-swap put.
type CasPutResult struct {
	Success bool
	// NewSeq is the new logical version, set on success.
	NewSeq uint64
	// ActualSeq is the current logical version, set on conflict.
	ActualSeq uint64
}

// LeaseResult is the outcome of Claim / Lease / Renew.
//
// Acquired is true iff the caller now holds the key; Fence is then the fencing
// token to carry on subsequent fenced writes. On failure Holder is the current
// holder and Fence is *their* token.
type LeaseResult struct {
	Acquired bool
	Holder   string
	Fence    uint64
}

// TemporalEdge is one revision of an edge in its bitemporal history.
type TemporalEdge struct {
	Src       uint64
	Dst       uint64
	Type      EdgeType
	ValidFrom uint64
	ValidTo   uint64
	Props     []byte
	// TxFrom / TxTo are transaction time (when the revision was believed and
	// when it was superseded); 0 means "always known" / "still believed".
	TxFrom uint64
	TxTo   uint64
	// AuthorAgentID is empty when the author is unknown.
	AuthorAgentID string
}

// WatchEvent is a single write observed by WatchPrefix.
type WatchEvent struct {
	// EventType is "put" or "delete".
	EventType string
	CF        uint32
	Key       []byte
	Value     []byte
	Seq       uint64
}

// ── Causal graph ────────────────────────────────────────────────────────────

// AddStepOptions carries the optional arguments of AddStep.
type AddStepOptions struct {
	Content   []byte
	Metadata  []byte
	Embedding []float32
	// BranchID is 0 for the main timeline.
	BranchID uint64
	// Scope defaults to ScopeWorld when empty.
	Scope MemoryScope
	// ScopeOwner is the team id for ScopeTeam, the agent id for ScopePrivate.
	ScopeOwner string
	// FieldACLJSON is an optional JSON [{json_pointer, min_scope}] array.
	FieldACLJSON []byte
}

// AddStep appends a causal step and returns its step id. opts may be nil.
func (c *Client) AddStep(ctx context.Context, agentID string, stepType StepType, opts *AddStepOptions) (uint64, error) {
	req := &pb.AgentAddStepRequest{AgentId: agentID, StepType: string(stepType)}
	if opts != nil {
		req.Content = opts.Content
		req.Metadata = opts.Metadata
		req.Embedding = opts.Embedding
		req.BranchId = opts.BranchID
		req.Scope = string(opts.Scope)
		req.ScopeOwner = opts.ScopeOwner
		req.FieldAclJson = opts.FieldACLJSON
	}
	resp, err := c.agent.AddStep(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.StepId, nil
}

// AddEdgeOptions carries the optional arguments of AddEdge.
type AddEdgeOptions struct {
	Props []byte
	// ValidFrom 0 means "now"; ValidTo 0 means the edge never expires.
	ValidFrom uint64
	ValidTo   uint64
	// AuthorAgentID records which agent's write created this belief; set it to
	// make the edge visible to per-agent belief queries.
	AuthorAgentID string
}

// AddEdge links two steps. opts may be nil.
func (c *Client) AddEdge(ctx context.Context, srcStepID, dstStepID uint64, edgeType EdgeType, opts *AddEdgeOptions) error {
	req := &pb.AgentAddEdgeRequest{
		SrcStepId: srcStepID,
		DstStepId: dstStepID,
		EdgeType:  string(edgeType),
	}
	if opts != nil {
		req.Props = opts.Props
		req.ValidFrom = opts.ValidFrom
		req.ValidTo = opts.ValidTo
		req.AuthorAgentId = opts.AuthorAgentID
	}
	_, err := c.agent.AddEdge(ctx, req)
	return err
}

// GetStep reads one step. Returns (nil, nil) when the step does not exist or
// the caller's scope does not admit it.
func (c *Client) GetStep(ctx context.Context, stepID uint64) (*Step, error) {
	resp, err := c.agent.GetStep(ctx, &pb.AgentGetStepRequest{StepId: stepID})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	step, err := decodeStep(resp.StepJson)
	if err != nil {
		return nil, err
	}
	return &step, nil
}

// GetContent reads a step's content blob. Returns (nil, nil) if not found.
func (c *Client) GetContent(ctx context.Context, stepID uint64) ([]byte, error) {
	resp, err := c.agent.GetContent(ctx, &pb.AgentGetContentRequest{StepId: stepID})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return resp.Content, nil
}

// GetEdgesOptions narrows an edge query.
type GetEdgesOptions struct {
	// Direction defaults to Forward. The server accepts Forward or Backward
	// here — Both is rejected with InvalidArgument.
	Direction Direction
	// Type filters by edge type; empty returns every type.
	Type EdgeType
	// AtTimestamp returns the edges valid at that instant (0 = no filter).
	AtTimestamp uint64
	// WindowStart / WindowEnd (both > 0, with AtTimestamp 0) return the edges
	// overlapping the window. A window query requires Direction Forward.
	WindowStart uint64
	WindowEnd   uint64
}

// GetEdges lists the edges incident to a step. opts may be nil (forward, all
// types, no temporal filter).
func (c *Client) GetEdges(ctx context.Context, stepID uint64, opts *GetEdgesOptions) ([]Edge, error) {
	req := &pb.AgentGetEdgesRequest{StepId: stepID, Direction: string(Forward)}
	if opts != nil {
		if opts.Direction != "" {
			req.Direction = string(opts.Direction)
		}
		req.EdgeType = string(opts.Type)
		req.AtTimestamp = opts.AtTimestamp
		req.WindowStart = opts.WindowStart
		req.WindowEnd = opts.WindowEnd
	}
	resp, err := c.agent.GetEdges(ctx, req)
	if err != nil {
		return nil, err
	}
	return decodeEdges(resp.Edges), nil
}

// Traverse walks the causal graph breadth-first from a step, up to maxDepth
// hops, and returns every step and edge it reached. An empty direction means
// Forward.
func (c *Client) Traverse(ctx context.Context, startStepID uint64, direction Direction, maxDepth uint32) (*TraverseResult, error) {
	resp, err := c.agent.Traverse(ctx, &pb.AgentTraverseRequest{
		StartStepId: startStepID,
		Direction:   string(direction),
		MaxDepth:    maxDepth,
	})
	if err != nil {
		return nil, err
	}
	steps, err := decodeSteps(resp.StepJsons)
	if err != nil {
		return nil, err
	}
	return &TraverseResult{Steps: steps, Edges: decodeEdges(resp.Edges)}, nil
}

// FindSimilarChains returns the k causal chains whose anchor step is nearest
// the query embedding, each walked chainDepth hops. ef is the vector-search
// ef (0 = index default).
func (c *Client) FindSimilarChains(ctx context.Context, queryEmbedding []float32, k, chainDepth, ef uint32) ([]CausalChain, error) {
	resp, err := c.agent.FindSimilarChains(ctx, &pb.AgentFindSimilarChainsRequest{
		QueryEmbedding: queryEmbedding,
		K:              k,
		ChainDepth:     chainDepth,
		Ef:             ef,
	})
	if err != nil {
		return nil, err
	}
	chains := make([]CausalChain, 0, len(resp.Chains))
	for _, ch := range resp.Chains {
		anchor, err := decodeStep(ch.AnchorStepJson)
		if err != nil {
			return nil, err
		}
		steps, err := decodeSteps(ch.StepJsons)
		if err != nil {
			return nil, err
		}
		chains = append(chains, CausalChain{
			Anchor:   anchor,
			Distance: ch.Distance,
			Steps:    steps,
			Edges:    decodeEdges(ch.Edges),
		})
	}
	return chains, nil
}

// ── Branches (fork) ─────────────────────────────────────────────────────────

// Fork opens a new branch off parentBranchID (0 = the main timeline) and
// returns the new branch id.
func (c *Client) Fork(ctx context.Context, label string, parentBranchID uint64) (uint64, error) {
	resp, err := c.agent.Fork(ctx, &pb.AgentForkRequest{Label: label, ParentBranchId: parentBranchID})
	if err != nil {
		return 0, err
	}
	return resp.BranchId, nil
}

// MergeBranch merges a branch back into its parent.
func (c *Client) MergeBranch(ctx context.Context, branchID uint64) error {
	_, err := c.agent.MergeBranch(ctx, &pb.AgentMergeBranchRequest{BranchId: branchID})
	return err
}

// DiscardBranch throws a branch away without merging it.
func (c *Client) DiscardBranch(ctx context.Context, branchID uint64) error {
	_, err := c.agent.DiscardBranch(ctx, &pb.AgentDiscardBranchRequest{BranchId: branchID})
	return err
}

// ListBranches returns every known branch and its status.
func (c *Client) ListBranches(ctx context.Context) ([]BranchMeta, error) {
	resp, err := c.agent.ListBranches(ctx, &pb.AgentListBranchesRequest{})
	if err != nil {
		return nil, err
	}
	branches := make([]BranchMeta, 0, len(resp.Branches))
	for _, b := range resp.Branches {
		branches = append(branches, BranchMeta{
			ID:                b.Id,
			ParentID:          b.ParentId,
			ParentSnapshotSeq: b.ParentSnapshotSeq,
			CreatedAt:         b.CreatedAt,
			Status:            b.Status,
			Label:             b.Label,
		})
	}
	return branches, nil
}

// BranchPut writes a key inside a branch's overlay.
func (c *Client) BranchPut(ctx context.Context, branchID uint64, cf uint32, key, value []byte) error {
	_, err := c.agent.BranchPut(ctx, &pb.AgentBranchPutRequest{
		BranchId: branchID, Cf: cf, Key: key, Value: value,
	})
	return err
}

// BranchGet reads a key as of a branch, falling back to the parent snapshot.
// Returns (nil, nil) if not found.
func (c *Client) BranchGet(ctx context.Context, branchID uint64, cf uint32, key []byte) ([]byte, error) {
	resp, err := c.agent.BranchGet(ctx, &pb.AgentBranchGetRequest{
		BranchId: branchID, Cf: cf, Key: key,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return resp.Value, nil
}

// ── Reactive state ──────────────────────────────────────────────────────────

// CasPut writes newValue only if the key is still at expectedSeq. A failed CAS
// is not an error: it comes back with Success false and the current
// ActualSeq.
func (c *Client) CasPut(ctx context.Context, cf uint32, key []byte, expectedSeq uint64, newValue []byte) (CasPutResult, error) {
	resp, err := c.agent.CasPut(ctx, &pb.AgentCasPutRequest{
		Cf: cf, Key: key, ExpectedSeq: expectedSeq, NewValue: newValue,
	})
	if err != nil {
		return CasPutResult{}, err
	}
	return CasPutResult{
		Success:   resp.Success,
		NewSeq:    resp.NewSeq,
		ActualSeq: resp.ActualSeq,
	}, nil
}

// IngestAction is how MemoryIngest resolved an incoming fact.
type IngestAction string

const (
	// ActionAdded: the fact was new and was created.
	ActionAdded IngestAction = "added"
	// ActionDeduplicated: a near-duplicate already existed and was reused.
	ActionDeduplicated IngestAction = "deduplicated"
	// ActionSuperseded: the fact replaced one or more existing facts, whose
	// valid time this ingest closed.
	ActionSuperseded IngestAction = "superseded"
	// ActionConflict: the optimistic transaction could not commit and nothing
	// was written. See MemoryIngestResult.FenceLost for which of the two
	// causes it was.
	ActionConflict IngestAction = "conflict"
)

var ingestActions = map[uint32]IngestAction{
	0: ActionAdded,
	1: ActionDeduplicated,
	2: ActionSuperseded,
	3: ActionConflict,
}

// IngestCandidate is one existing fact the caller already retrieved (ANN over
// the scope index, scope-filtered) and its cosine similarity to the incoming
// content.
type IngestCandidate struct {
	FactID uint64
	Sim    float32
}

// MemoryIngestOptions carries the optional arguments of MemoryIngest.
type MemoryIngestOptions struct {
	Candidates      []IngestCandidate
	ProvenanceSteps []uint64
	// EmbeddingID is nil when the fact carries no embedding.
	EmbeddingID *uint64
	// DedupThreshold defaults to 0.97 when left at 0: a candidate at or above
	// it is treated as a duplicate.
	DedupThreshold float32
	// SupersedeThreshold defaults to 0.80 when left at 0: a candidate between
	// it and DedupThreshold is superseded by the incoming fact.
	SupersedeThreshold float32
	AuthorAgentID      string
	Confidence         float32
	RunID              string
	// Fence gates the write on a lease (#784). When non-zero, LeaseKey must be
	// the exact bytes passed to Lease/Claim: if the lease moved past Fence the
	// commit aborts with ActionConflict and FenceLost true, and nothing is
	// written.
	Fence    uint64
	LeaseKey []byte
}

// MemoryIngestResult reports how a fact was resolved.
type MemoryIngestResult struct {
	Action IngestAction
	// FactID is the resulting fact: newly created, or the deduped existing one.
	FactID uint64
	// Superseded lists the facts whose valid time this ingest closed.
	Superseded []uint64
	// Committed is false exactly when Action is ActionConflict.
	Committed bool
	// ConflictFactID is the candidate whose version moved, on a data conflict;
	// 0 on a lost fence.
	ConflictFactID uint64
	// FenceLost separates the two conflict causes. False means a concurrent
	// writer kept moving a read candidate within the retry budget — back off
	// and retry. True means the lease was re-acquired past the Fence passed
	// (#784), which is terminal: retrying cannot reacquire it, so take a fresh
	// lease for a new fence.
	FenceLost bool
}

// MemoryIngest transactionally ingests a fact (#780), optionally fence-gated
// (#784).
//
// Dedup, create-with-provenance and supersede all commit as ONE atomic,
// snapshot-isolated batch on the owning data node. A conflict is not an error:
// it comes back as ActionConflict with Committed false. opts may be nil.
func (c *Client) MemoryIngest(ctx context.Context, scope, content string, opts *MemoryIngestOptions) (MemoryIngestResult, error) {
	req := &pb.AgentMemoryIngestRequest{
		Scope:              scope,
		Content:            content,
		DedupThreshold:     0.97,
		SupersedeThreshold: 0.80,
	}
	if opts != nil {
		for _, cand := range opts.Candidates {
			req.Candidates = append(req.Candidates, &pb.AgentIngestCandidate{
				FactId: cand.FactID, Sim: cand.Sim,
			})
		}
		req.ProvenanceSteps = opts.ProvenanceSteps
		req.EmbeddingId = opts.EmbeddingID
		if opts.DedupThreshold != 0 {
			req.DedupThreshold = opts.DedupThreshold
		}
		if opts.SupersedeThreshold != 0 {
			req.SupersedeThreshold = opts.SupersedeThreshold
		}
		req.AuthorAgentId = opts.AuthorAgentID
		req.Confidence = opts.Confidence
		req.RunId = opts.RunID
		req.Fence = opts.Fence
		req.LeaseKey = opts.LeaseKey
	}
	resp, err := c.agent.MemoryIngest(ctx, req)
	if err != nil {
		return MemoryIngestResult{}, err
	}
	action, ok := ingestActions[resp.Action]
	if !ok {
		return MemoryIngestResult{}, fmt.Errorf("statelet: unknown ingest action %d", resp.Action)
	}
	return MemoryIngestResult{
		Action:         action,
		FactID:         resp.FactId,
		Superseded:     resp.Superseded,
		Committed:      resp.Committed,
		ConflictFactID: resp.ConflictFactId,
		FenceLost:      resp.FenceLost,
	}, nil
}

// ── Coordination: claims, leases, fences ────────────────────────────────────

// Claim takes key for agentID iff it is unheld, with no expiry.
func (c *Client) Claim(ctx context.Context, key []byte, agentID string) (LeaseResult, error) {
	resp, err := c.agent.Claim(ctx, &pb.AgentClaimRequest{Key: key, AgentId: agentID})
	if err != nil {
		return LeaseResult{}, err
	}
	return LeaseResult{Acquired: resp.Acquired, Holder: resp.Holder, Fence: resp.Fence}, nil
}

// Lease takes key for agentID with a TTL; an un-renewed lease auto-expires.
// ttlMs 0 makes it a plain claim.
func (c *Client) Lease(ctx context.Context, key []byte, agentID string, ttlMs uint64) (LeaseResult, error) {
	resp, err := c.agent.Lease(ctx, &pb.AgentLeaseRequest{Key: key, AgentId: agentID, TtlMs: ttlMs})
	if err != nil {
		return LeaseResult{}, err
	}
	return LeaseResult{Acquired: resp.Acquired, Holder: resp.Holder, Fence: resp.Fence}, nil
}

// Renew extends the lease on key iff fence still matches the holder's token.
//
// A successful renew ADVANCES the fence: adopt the returned Fence for
// subsequent fenced writes or they abort (#784).
func (c *Client) Renew(ctx context.Context, key []byte, agentID string, fence, ttlMs uint64) (LeaseResult, error) {
	resp, err := c.agent.Renew(ctx, &pb.AgentRenewRequest{
		Key: key, AgentId: agentID, Fence: fence, TtlMs: ttlMs,
	})
	if err != nil {
		return LeaseResult{}, err
	}
	return LeaseResult{Acquired: resp.Acquired, Holder: resp.Holder, Fence: resp.Fence}, nil
}

// Release drops the lease on key iff fence matches. Reports whether the key
// was actually dropped.
func (c *Client) Release(ctx context.Context, key []byte, fence uint64) (bool, error) {
	resp, err := c.agent.Release(ctx, &pb.AgentReleaseRequest{Key: key, Fence: fence})
	if err != nil {
		return false, err
	}
	return resp.Released, nil
}

// ── Temporal edges ──────────────────────────────────────────────────────────

// ExpireEdge closes an edge's valid time at expireAt, leaving the earlier
// revision readable through EdgeHistory and as-of queries.
func (c *Client) ExpireEdge(ctx context.Context, srcStepID, dstStepID uint64, edgeType EdgeType, expireAt uint64) error {
	_, err := c.agent.ExpireEdge(ctx, &pb.AgentExpireEdgeRequest{
		SrcStepId: srcStepID, DstStepId: dstStepID,
		EdgeType: string(edgeType), ExpireAt: expireAt,
	})
	return err
}

// EdgeHistory returns every bitemporal revision of one edge.
func (c *Client) EdgeHistory(ctx context.Context, srcStepID, dstStepID uint64, edgeType EdgeType) ([]TemporalEdge, error) {
	resp, err := c.agent.EdgeHistory(ctx, &pb.AgentEdgeHistoryRequest{
		SrcStepId: srcStepID, DstStepId: dstStepID, EdgeType: string(edgeType),
	})
	if err != nil {
		return nil, err
	}
	edges := make([]TemporalEdge, 0, len(resp.Edges))
	for _, e := range resp.Edges {
		edges = append(edges, TemporalEdge{
			Src:           e.Src,
			Dst:           e.Dst,
			Type:          EdgeType(e.EdgeType),
			ValidFrom:     e.ValidFrom,
			ValidTo:       e.ValidTo,
			Props:         e.Props,
			TxFrom:        e.TxFrom,
			TxTo:          e.TxTo,
			AuthorAgentID: e.AuthorAgentId,
		})
	}
	return edges, nil
}

// ── Streaming: prefix watch ─────────────────────────────────────────────────

// ErrWatchStop, returned by a WatchPrefix handler, ends the watch cleanly:
// WatchPrefix returns nil rather than propagating it.
var ErrWatchStop = errors.New("statelet: watch stopped by handler")

// WatchPrefix streams every write whose key starts with prefix in cf, calling
// fn for each one until ctx is cancelled or the server ends the stream.
//
// Unlike SubscribeCommitted this is a live tail with no offsets: it does not
// replay history, and it does not reconnect — a dropped stream surfaces as an
// error so the caller can decide whether re-watching is safe. Return
// ErrWatchStop from fn to stop without an error; any other non-nil error stops
// the watch and is returned as-is.
func (c *Client) WatchPrefix(ctx context.Context, agentID string, cf uint32, prefix []byte, fn func(WatchEvent) error) error {
	stream, err := c.agent.WatchPrefix(ctx, &pb.AgentWatchPrefixRequest{
		AgentId: agentID, Cf: cf, Prefix: prefix,
	})
	if err != nil {
		return err
	}
	for {
		event, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return nil
			}
			return recvErr
		}
		if err := fn(WatchEvent{
			EventType: event.EventType,
			CF:        event.Cf,
			Key:       event.Key,
			Value:     event.Value,
			Seq:       event.Seq,
		}); err != nil {
			if errors.Is(err, ErrWatchStop) {
				return nil
			}
			return err
		}
	}
}
