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

// Round-trip test for result grouping / field-collapse (epic #1427, Phase 3):
// VectorSearchGrouped must marshal the GroupSpec onto the proto request fields
// (group_field/group_size/groups/group_overfetch/group_missing_as_own) and
// surface the response group_key on VectorSearchResult.

import (
	"context"
	"testing"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
	"google.golang.org/grpc"
)

// fakeVectorStub captures the VectorSearchRequest and returns a canned grouped
// response so we can assert the kwargs round-trip and group_key is surfaced.
type fakeVectorStub struct {
	pb.StateletClient // embedded; only VectorSearch overridden
	gotReq           *pb.VectorSearchRequest
	resp             *pb.VectorSearchResponse
}

func (f *fakeVectorStub) VectorSearch(ctx context.Context, in *pb.VectorSearchRequest, opts ...grpc.CallOption) (*pb.VectorSearchResponse, error) {
	f.gotReq = in
	return f.resp, nil
}

func TestVectorSearchGroupedRoundTrip(t *testing.T) {
	stub := &fakeVectorStub{
		resp: &pb.VectorSearchResponse{Results: []*pb.VectorSearchResult{
			{Id: 10, Distance: 0.1, GroupKey: "docA"},
			{Id: 20, Distance: 0.2, GroupKey: "docB"},
		}},
	}
	c := newTestClient(stub)

	got, err := c.VectorSearchGrouped(context.Background(), "idx", []float32{0, 0, 0, 0}, 5, 0, GroupSpec{
		Field:        "doc_id",
		GroupSize:    2,
		Groups:       3,
		Overfetch:    4,
		MissingAsOwn: true,
	})
	if err != nil {
		t.Fatalf("VectorSearchGrouped: %v", err)
	}

	// Grouping kwargs round-tripped onto the proto request.
	r := stub.gotReq
	if r.GroupField != "doc_id" || r.GroupSize != 2 || r.Groups != 3 || r.GroupOverfetch != 4 || !r.GroupMissingAsOwn {
		t.Fatalf("grouping kwargs not marshalled: %+v", r)
	}

	// group_key surfaced on each result.
	if len(got) != 2 || got[0].GroupKey != "docA" || got[1].GroupKey != "docB" {
		t.Fatalf("group_key not surfaced: %+v", got)
	}
	if got[0].ID != 10 || got[1].ID != 20 {
		t.Fatalf("unexpected ids: %+v", got)
	}
}

// Default (plain) VectorSearch must leave grouping off (empty group_field).
func TestVectorSearchPlainNoGrouping(t *testing.T) {
	stub := &fakeVectorStub{resp: &pb.VectorSearchResponse{}}
	c := newTestClient(stub)
	if _, err := c.VectorSearch(context.Background(), "idx", []float32{0}, 1, 0); err != nil {
		t.Fatalf("VectorSearch: %v", err)
	}
	if stub.gotReq.GroupField != "" {
		t.Fatalf("plain search must not set group_field, got %q", stub.gotReq.GroupField)
	}
}
