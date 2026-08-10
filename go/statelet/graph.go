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

// Declarative graph queries — the GraphQuery RPC (an openCypher subset).

import (
	"context"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
)

// GraphValueKind tags which member of a GraphValue is meaningful.
type GraphValueKind = pb.GraphQueryValue_Kind

// The GraphValue kinds, re-exported so callers need not import the proto
// package to switch on one.
const (
	GraphValueNull   = pb.GraphQueryValue_NULL
	GraphValueInt    = pb.GraphQueryValue_INT
	GraphValueDouble = pb.GraphQueryValue_DOUBLE
	GraphValueString = pb.GraphQueryValue_STRING
	GraphValueBool   = pb.GraphQueryValue_BOOL
	GraphValueJSON   = pb.GraphQueryValue_JSON
)

// GraphValue is one projected column value. Exactly one field is meaningful,
// selected by Kind; JSON carries the hydrated node-property blob verbatim.
type GraphValue struct {
	Kind   GraphValueKind
	Int    int64
	Double float64
	Str    string
	Bool   bool
	JSON   []byte
}

// Interface returns the value as its natural Go type — nil, int64, float64,
// string, bool or []byte — for callers that would rather not switch on Kind.
func (v GraphValue) Interface() any {
	switch v.Kind {
	case GraphValueInt:
		return v.Int
	case GraphValueDouble:
		return v.Double
	case GraphValueString:
		return v.Str
	case GraphValueBool:
		return v.Bool
	case GraphValueJSON:
		return v.JSON
	default:
		return nil
	}
}

// GraphQueryOptions carries the out-of-band knobs of a GraphQuery. The zero
// value means "let the gateway decide": the default graph, no extra row cap and
// no temporal filter on either axis.
type GraphQueryOptions struct {
	// GraphName is the graph index to query (empty ⇒ the gateway's default).
	GraphName string
	// MaxRows caps the returned rows regardless of any LIMIT in the query
	// (0 ⇒ no extra cap; a parsed LIMIT still applies).
	MaxRows uint32
	// AsOf / TxAsOf are the bitemporal point-in-time the query is evaluated
	// against, in ms (0 ⇒ current). An `AS OF <valid>[, <tx>]` clause in the
	// query text overrides them.
	AsOf   uint64
	TxAsOf uint64
}

// GraphQueryResult is the projected result set. Warnings is non-empty when the
// result may be incomplete — e.g. a label scan hit the per-shard frontier cap.
type GraphQueryResult struct {
	Columns  []string
	Rows     [][]GraphValue
	Warnings []string
}

// Maps renders the rows as column-keyed maps of natural Go values.
func (r *GraphQueryResult) Maps() []map[string]any {
	out := make([]map[string]any, len(r.Rows))
	for i, row := range r.Rows {
		m := make(map[string]any, len(row))
		for j, v := range row {
			if j < len(r.Columns) {
				m[r.Columns[j]] = v.Interface()
			}
		}
		out[i] = m
	}
	return out
}

// GraphQuery runs a read-only openCypher-subset query.
//
// Gateway-only: the gateway parses and plans the query, then compiles it to
// engine traversal primitives. The subset covers MATCH path patterns, WHERE
// over node properties, RETURN / ORDER BY / LIMIT, a bitemporal
// `AS OF <valid>[, <tx>]` clause, and the retrieval procedures db.vectorSearch
// / db.hybridSearch / db.graphRag. CREATE / MERGE are rejected.
//
// Pass opts == nil for the defaults (see GraphQueryOptions).
//
// Named query parameters ($q) parse but are not resolvable yet, so a
// vector-seeded procedure needs an inline literal:
// db.vectorSearch([0.1, 0.2, ...], 5).
func (c *Client) GraphQuery(ctx context.Context, cypher string, opts *GraphQueryOptions) (*GraphQueryResult, error) {
	req := &pb.GraphQueryRequest{Cypher: cypher}
	if opts != nil {
		req.GraphName = opts.GraphName
		req.MaxRows = opts.MaxRows
		req.AsOf = opts.AsOf
		req.TxAsOf = opts.TxAsOf
	}
	resp, err := c.stub.GraphQuery(ctx, req)
	if err != nil {
		return nil, err
	}
	rows := make([][]GraphValue, len(resp.Rows))
	for i, row := range resp.Rows {
		values := make([]GraphValue, len(row.Values))
		for j, v := range row.Values {
			values[j] = GraphValue{
				Kind:   v.Kind,
				Int:    v.IntValue,
				Double: v.DblValue,
				Str:    v.StrValue,
				Bool:   v.BoolValue,
				JSON:   v.JsonValue,
			}
		}
		rows[i] = values
	}
	return &GraphQueryResult{
		Columns:  resp.Columns,
		Rows:     rows,
		Warnings: resp.Warnings,
	}, nil
}
