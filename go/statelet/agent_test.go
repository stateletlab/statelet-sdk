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

// Round-trip tests for the agent-state surface: the request fields each
// wrapper marshals, and the decoding of the server's JSON step encoding.

import (
	"context"
	"io"
	"testing"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
	"google.golang.org/grpc"
)

// fakeAgentStub records the last request per RPC and replays canned responses.
type fakeAgentStub struct {
	pb.AgentStateServiceClient // embedded; only the used RPCs are overridden

	addStepReq  *pb.AgentAddStepRequest
	addEdgeReq  *pb.AgentAddEdgeRequest
	getEdgesReq *pb.AgentGetEdgesRequest
	traverseReq *pb.AgentTraverseRequest
	watchReq    *pb.AgentWatchPrefixRequest

	traverseResp *pb.AgentTraverseResponse
	watchScript  []*pb.AgentWatchEventProto
}

func (f *fakeAgentStub) AddStep(ctx context.Context, in *pb.AgentAddStepRequest, opts ...grpc.CallOption) (*pb.AgentAddStepResponse, error) {
	f.addStepReq = in
	return &pb.AgentAddStepResponse{StepId: 42}, nil
}

func (f *fakeAgentStub) AddEdge(ctx context.Context, in *pb.AgentAddEdgeRequest, opts ...grpc.CallOption) (*pb.AgentAddEdgeResponse, error) {
	f.addEdgeReq = in
	return &pb.AgentAddEdgeResponse{}, nil
}

func (f *fakeAgentStub) GetEdges(ctx context.Context, in *pb.AgentGetEdgesRequest, opts ...grpc.CallOption) (*pb.AgentGetEdgesResponse, error) {
	f.getEdgesReq = in
	return &pb.AgentGetEdgesResponse{Edges: []*pb.AgentEdgeProto{
		{PeerStepId: 7, EdgeType: "Informs", Props: []byte("p"), ValidFrom: 1, ValidTo: 0},
	}}, nil
}

func (f *fakeAgentStub) Traverse(ctx context.Context, in *pb.AgentTraverseRequest, opts ...grpc.CallOption) (*pb.AgentTraverseResponse, error) {
	f.traverseReq = in
	return f.traverseResp, nil
}

func (f *fakeAgentStub) WatchPrefix(ctx context.Context, in *pb.AgentWatchPrefixRequest, opts ...grpc.CallOption) (pb.AgentStateService_WatchPrefixClient, error) {
	f.watchReq = in
	return &fakeWatchStream{script: f.watchScript}, nil
}

type fakeWatchStream struct {
	grpc.ClientStream
	script []*pb.AgentWatchEventProto
	pos    int
}

func (s *fakeWatchStream) Recv() (*pb.AgentWatchEventProto, error) {
	if s.pos >= len(s.script) {
		return nil, io.EOF
	}
	ev := s.script[s.pos]
	s.pos++
	return ev, nil
}

func newTestAgentClient(stub pb.AgentStateServiceClient) *Client {
	return &Client{agent: stub, DefaultCF: 0}
}

func TestAddStepMarshalsOptions(t *testing.T) {
	stub := &fakeAgentStub{}
	c := newTestAgentClient(stub)

	id, err := c.AddStep(context.Background(), "agent-1", StepObserve, &AddStepOptions{
		Content:      []byte("saw something"),
		Metadata:     []byte(`{"m":1}`),
		Embedding:    []float32{0.5, 0.25},
		BranchID:     3,
		Scope:        ScopeTeam,
		ScopeOwner:   "team-a",
		FieldACLJSON: []byte(`[{"json_pointer":"/secret","min_scope":"private"}]`),
	})
	if err != nil {
		t.Fatalf("AddStep: %v", err)
	}
	if id != 42 {
		t.Fatalf("step id: want 42, got %d", id)
	}

	r := stub.addStepReq
	if r.AgentId != "agent-1" || r.StepType != "Observe" || r.BranchId != 3 {
		t.Fatalf("identity fields not marshalled: %+v", r)
	}
	if string(r.Content) != "saw something" || string(r.Metadata) != `{"m":1}` {
		t.Fatalf("payload not marshalled: %+v", r)
	}
	if len(r.Embedding) != 2 || r.Embedding[0] != 0.5 || r.Embedding[1] != 0.25 {
		t.Fatalf("embedding not marshalled: %+v", r.Embedding)
	}
	if r.Scope != "team" || r.ScopeOwner != "team-a" || len(r.FieldAclJson) == 0 {
		t.Fatalf("scope not marshalled: %+v", r)
	}
}

// A nil options pointer must still produce a well-formed request.
func TestAddStepNilOptions(t *testing.T) {
	stub := &fakeAgentStub{}
	c := newTestAgentClient(stub)
	if _, err := c.AddStep(context.Background(), "agent-1", StepThink, nil); err != nil {
		t.Fatalf("AddStep: %v", err)
	}
	r := stub.addStepReq
	if r.StepType != "Think" || r.BranchId != 0 || r.Scope != "" || len(r.Content) != 0 {
		t.Fatalf("unexpected defaults: %+v", r)
	}
}

func TestAddEdgeMarshalsOptions(t *testing.T) {
	stub := &fakeAgentStub{}
	c := newTestAgentClient(stub)

	err := c.AddEdge(context.Background(), 1, 2, EdgeSupersedes, &AddEdgeOptions{
		Props:         []byte("why"),
		ValidFrom:     100,
		ValidTo:       200,
		AuthorAgentID: "agent-1",
	})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	r := stub.addEdgeReq
	if r.SrcStepId != 1 || r.DstStepId != 2 || r.EdgeType != "Supersedes" {
		t.Fatalf("edge identity not marshalled: %+v", r)
	}
	if r.ValidFrom != 100 || r.ValidTo != 200 || r.AuthorAgentId != "agent-1" || string(r.Props) != "why" {
		t.Fatalf("edge options not marshalled: %+v", r)
	}
}

// GetEdges must default to "forward" — the server rejects an empty direction.
func TestGetEdgesDefaultsToForward(t *testing.T) {
	stub := &fakeAgentStub{}
	c := newTestAgentClient(stub)

	edges, err := c.GetEdges(context.Background(), 5, nil)
	if err != nil {
		t.Fatalf("GetEdges: %v", err)
	}
	if stub.getEdgesReq.Direction != "forward" {
		t.Fatalf("direction: want forward, got %q", stub.getEdgesReq.Direction)
	}
	if len(edges) != 1 || edges[0].PeerStepID != 7 || edges[0].Type != EdgeInforms {
		t.Fatalf("edges not decoded: %+v", edges)
	}

	if _, err := c.GetEdges(context.Background(), 5, &GetEdgesOptions{
		Direction: Backward, Type: EdgeTriggers, AtTimestamp: 9,
	}); err != nil {
		t.Fatalf("GetEdges: %v", err)
	}
	r := stub.getEdgesReq
	if r.Direction != "backward" || r.EdgeType != "Triggers" || r.AtTimestamp != 9 {
		t.Fatalf("filters not marshalled: %+v", r)
	}
}

// The server encodes steps with serde: `id`/`timestamp`/`metadata` as a byte
// array, and the scope tag nested under `scope`.
func TestTraverseDecodesServerStepJSON(t *testing.T) {
	stepJSON := []byte(`{
		"id": 11,
		"agent_id": "agent-1",
		"step_type": "Act",
		"timestamp": 1712345678000,
		"branch_id": 2,
		"embedding_id": 99,
		"metadata": [123, 125],
		"scope": {
			"scope": "private",
			"owner": "agent-1",
			"field_acl": [{"json_pointer": "/cot", "min_scope": "team"}]
		}
	}`)
	stub := &fakeAgentStub{traverseResp: &pb.AgentTraverseResponse{
		StepJsons: [][]byte{stepJSON},
		Edges:     []*pb.AgentEdgeProto{{PeerStepId: 12, EdgeType: "Triggers"}},
	}}
	c := newTestAgentClient(stub)

	got, err := c.Traverse(context.Background(), 11, Both, 3)
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	if stub.traverseReq.StartStepId != 11 || stub.traverseReq.Direction != "both" || stub.traverseReq.MaxDepth != 3 {
		t.Fatalf("traverse args not marshalled: %+v", stub.traverseReq)
	}
	if len(got.Steps) != 1 {
		t.Fatalf("want 1 step, got %d", len(got.Steps))
	}
	s := got.Steps[0]
	if s.ID != 11 || s.AgentID != "agent-1" || s.Type != StepAct || s.Timestamp != 1712345678000 || s.BranchID != 2 {
		t.Fatalf("step fields not decoded: %+v", s)
	}
	if s.EmbeddingID == nil || *s.EmbeddingID != 99 {
		t.Fatalf("embedding_id not decoded: %+v", s.EmbeddingID)
	}
	if string(s.Metadata) != "{}" {
		t.Fatalf("metadata byte array not decoded: %q", s.Metadata)
	}
	if s.Scope != ScopePrivate || s.ScopeOwner != "agent-1" {
		t.Fatalf("scope not decoded: %+v", s)
	}
	if len(s.FieldACL) != 1 || s.FieldACL[0].JSONPointer != "/cot" || s.FieldACL[0].MinScope != ScopeTeam {
		t.Fatalf("field_acl not decoded: %+v", s.FieldACL)
	}
	if len(s.Raw) == 0 {
		t.Fatal("raw step json not retained")
	}
	if len(got.Edges) != 1 || got.Edges[0].PeerStepID != 12 || got.Edges[0].Type != EdgeTriggers {
		t.Fatalf("edges not decoded: %+v", got.Edges)
	}
}

// A step written before scope tagging carries no `scope` and must decode as
// world-readable (the server's serde default).
func TestDecodeStepUntaggedScopeIsWorld(t *testing.T) {
	s, err := decodeStep([]byte(`{"id":1,"agent_id":"a","step_type":"Tool","timestamp":0,"branch_id":0,"embedding_id":null,"metadata":[]}`))
	if err != nil {
		t.Fatalf("decodeStep: %v", err)
	}
	if s.Scope != ScopeWorld || s.ScopeOwner != "" {
		t.Fatalf("untagged step must decode as world: %+v", s)
	}
	if s.EmbeddingID != nil {
		t.Fatalf("null embedding_id must decode as nil, got %v", *s.EmbeddingID)
	}
	if s.Type != StepTool {
		t.Fatalf("step_type: want Tool, got %q", s.Type)
	}
}

func TestWatchPrefixStreamsUntilEOF(t *testing.T) {
	stub := &fakeAgentStub{watchScript: []*pb.AgentWatchEventProto{
		{EventType: "put", Cf: 0, Key: []byte("state:a"), Value: []byte("1"), Seq: 1},
		{EventType: "delete", Cf: 0, Key: []byte("state:b"), Seq: 2},
	}}
	c := newTestAgentClient(stub)

	var got []WatchEvent
	err := c.WatchPrefix(context.Background(), "agent-1", 0, []byte("state:"), func(ev WatchEvent) error {
		got = append(got, ev)
		return nil
	})
	if err != nil {
		t.Fatalf("WatchPrefix: %v", err)
	}
	if stub.watchReq.AgentId != "agent-1" || string(stub.watchReq.Prefix) != "state:" {
		t.Fatalf("watch args not marshalled: %+v", stub.watchReq)
	}
	if len(got) != 2 || got[0].EventType != "put" || got[1].EventType != "delete" || got[1].Seq != 2 {
		t.Fatalf("events not surfaced: %+v", got)
	}
}

// ErrWatchStop ends the watch cleanly; other handler errors propagate.
func TestWatchPrefixHandlerStop(t *testing.T) {
	script := []*pb.AgentWatchEventProto{
		{EventType: "put", Key: []byte("k1")},
		{EventType: "put", Key: []byte("k2")},
	}
	c := newTestAgentClient(&fakeAgentStub{watchScript: script})

	seen := 0
	err := c.WatchPrefix(context.Background(), "a", 0, nil, func(WatchEvent) error {
		seen++
		return ErrWatchStop
	})
	if err != nil {
		t.Fatalf("ErrWatchStop must end the watch cleanly, got %v", err)
	}
	if seen != 1 {
		t.Fatalf("handler must stop after the first event, saw %d", seen)
	}
}

func TestStepAndEdgeTypeValidity(t *testing.T) {
	for _, s := range []StepType{StepObserve, StepThink, StepAct, StepTool, StepResult} {
		if !s.Valid() {
			t.Fatalf("%q must be a valid step type", s)
		}
	}
	if StepType("observe").Valid() {
		t.Fatal("step types are case-sensitive server-side")
	}
	for _, e := range []EdgeType{
		EdgeTriggers, EdgeInforms, EdgeBranches, EdgeMerges,
		EdgeSupersedes, EdgeDerivedFrom, EdgeContradicts,
	} {
		if !e.Valid() {
			t.Fatalf("%q must be a valid edge type", e)
		}
	}
	if EdgeType("Causes").Valid() {
		t.Fatal("unknown edge type must not validate")
	}
}
