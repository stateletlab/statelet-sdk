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

// Round-trip test for the declarative graph query surface: GraphQuery must
// marshal GraphQueryOptions onto the proto request and decode every
// GraphQueryValue kind back to its natural Go type.

import (
	"context"
	"testing"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
	"google.golang.org/grpc"
)

// fakeGraphStub captures the GraphQueryRequest and replays a canned response.
type fakeGraphStub struct {
	pb.StateletClient // embedded; only GraphQuery overridden
	gotReq            *pb.GraphQueryRequest
	resp              *pb.GraphQueryResponse
}

func (f *fakeGraphStub) GraphQuery(ctx context.Context, in *pb.GraphQueryRequest, opts ...grpc.CallOption) (*pb.GraphQueryResponse, error) {
	f.gotReq = in
	return f.resp, nil
}

func TestGraphQuerySendsOptions(t *testing.T) {
	stub := &fakeGraphStub{resp: &pb.GraphQueryResponse{}}
	c := &Client{stub: stub}

	_, err := c.GraphQuery(context.Background(), "MATCH (n) RETURN n", &GraphQueryOptions{
		GraphName: "g",
		MaxRows:   25,
		AsOf:      1737000000000,
		TxAsOf:    1737000000001,
	})
	if err != nil {
		t.Fatalf("GraphQuery: %v", err)
	}
	if stub.gotReq.Cypher != "MATCH (n) RETURN n" {
		t.Errorf("cypher = %q", stub.gotReq.Cypher)
	}
	if stub.gotReq.GraphName != "g" {
		t.Errorf("graph_name = %q, want g", stub.gotReq.GraphName)
	}
	if stub.gotReq.MaxRows != 25 {
		t.Errorf("max_rows = %d, want 25", stub.gotReq.MaxRows)
	}
	if stub.gotReq.AsOf != 1737000000000 || stub.gotReq.TxAsOf != 1737000000001 {
		t.Errorf("as_of/tx_as_of = %d/%d", stub.gotReq.AsOf, stub.gotReq.TxAsOf)
	}
}

func TestGraphQueryNilOptionsLeavesServerDefaults(t *testing.T) {
	stub := &fakeGraphStub{resp: &pb.GraphQueryResponse{}}
	c := &Client{stub: stub}

	res, err := c.GraphQuery(context.Background(), "MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("GraphQuery: %v", err)
	}
	if stub.gotReq.GraphName != "" || stub.gotReq.MaxRows != 0 ||
		stub.gotReq.AsOf != 0 || stub.gotReq.TxAsOf != 0 {
		t.Errorf("nil options must leave every knob zero, got %+v", stub.gotReq)
	}
	if len(res.Rows) != 0 || len(res.Columns) != 0 || len(res.Warnings) != 0 {
		t.Errorf("empty response should decode to an empty result, got %+v", res)
	}
}

func TestGraphQueryDecodesEveryValueKind(t *testing.T) {
	stub := &fakeGraphStub{resp: &pb.GraphQueryResponse{
		Columns: []string{"nul", "i", "d", "s", "b", "j"},
		Rows: []*pb.GraphQueryRow{{Values: []*pb.GraphQueryValue{
			{Kind: pb.GraphQueryValue_NULL},
			{Kind: pb.GraphQueryValue_INT, IntValue: 42},
			{Kind: pb.GraphQueryValue_DOUBLE, DblValue: 0.5},
			{Kind: pb.GraphQueryValue_STRING, StrValue: "knows"},
			{Kind: pb.GraphQueryValue_BOOL, BoolValue: true},
			{Kind: pb.GraphQueryValue_JSON, JsonValue: []byte(`{"name":"ada"}`)},
		}}},
		Warnings: []string{"label scan truncated at the frontier cap"},
	}}
	c := &Client{stub: stub}

	res, err := c.GraphQuery(context.Background(), "MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("GraphQuery: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 6 {
		t.Fatalf("want one 6-column row, got %+v", res.Rows)
	}
	row := res.Rows[0]
	if row[0].Interface() != nil {
		t.Errorf("NULL decoded to %v", row[0].Interface())
	}
	if row[1].Interface() != int64(42) {
		t.Errorf("INT decoded to %v", row[1].Interface())
	}
	if row[2].Interface() != 0.5 {
		t.Errorf("DOUBLE decoded to %v", row[2].Interface())
	}
	if row[3].Interface() != "knows" {
		t.Errorf("STRING decoded to %v", row[3].Interface())
	}
	if row[4].Interface() != true {
		t.Errorf("BOOL decoded to %v", row[4].Interface())
	}
	if string(row[5].JSON) != `{"name":"ada"}` {
		t.Errorf("JSON decoded to %s", row[5].JSON)
	}
	if len(res.Warnings) != 1 {
		t.Errorf("warnings = %v", res.Warnings)
	}

	maps := res.Maps()
	if len(maps) != 1 || maps[0]["s"] != "knows" || maps[0]["i"] != int64(42) {
		t.Errorf("Maps() = %+v", maps)
	}
	if v, ok := maps[0]["nul"]; !ok || v != nil {
		t.Errorf("Maps() must keep the NULL column, got %+v", maps[0])
	}
}
