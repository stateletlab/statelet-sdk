// Package statelet provides a Go gRPC client for the Statelet distributed key-value store.
package statelet

import (
	"context"
	"fmt"

	pb "github.com/stateletlab/statelet-sdk/go/statelet/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// VectorSearchResult holds a single nearest-neighbor result.
type VectorSearchResult struct {
	ID       uint64
	Distance float32
	// GroupKey is the field-collapse group key (epic #1427) — the candidate's
	// GroupSpec.Field payload value rendered to its canonical string. Empty
	// unless the search set a GroupSpec; re-bucket on it to present grouped
	// results.
	GroupKey string
}

// GroupSpec configures result grouping / field-collapse for VectorSearchGrouped
// (epic #1427): collapse results to at most GroupSize hits per distinct value of
// the payload Field, returning up to Groups distinct group keys, ordered by
// ascending distance. The analogue of Qdrant query_groups / Weaviate groupBy /
// Milvus grouping_field.
type GroupSpec struct {
	// Field is the payload field to group by (empty ⇒ grouping off).
	Field string
	// GroupSize is the max hits per group (0 ⇒ 1, one-best-per-group).
	GroupSize uint32
	// Groups is the number of distinct group keys to return (0 ⇒ falls back to k).
	Groups uint32
	// Overfetch is the candidate over-fetch multiplier (0 ⇒ default 4, capped
	// server-side).
	Overfetch uint32
	// MissingAsOwn, when true, returns candidates missing Field as their own
	// singleton group (empty GroupKey) instead of dropping them (default).
	MissingAsOwn bool
}

// VectorIndexConfig holds HNSW index configuration.
type VectorIndexConfig struct {
	Dim            uint32
	Metric         pb.VectorDistanceMetric // VECTOR_L2, VECTOR_COSINE, VECTOR_INNER_PRODUCT
	M              uint32
	MMax0          uint32
	EfConstruction uint32
	EfSearch       uint32
}

// WriteOp represents a single operation within a batch write.
type WriteOp struct {
	Op    pb.WriteOp // PUT, DELETE, MERGE
	CF    uint32
	Key   []byte
	Value []byte
}

// Client is a synchronous gRPC client for Statelet.
//
// One Client speaks both services on the connection: the KV/vector surface
// (Statelet) and the agent-state surface (AgentStateService, see agent.go).
type Client struct {
	conn      *grpc.ClientConn
	stub      pb.StateletClient
	agent     pb.AgentStateServiceClient
	DefaultCF uint32
}

// NewClient connects to a Statelet node at the given address (e.g. "127.0.0.1:7379").
//
// Agent-state calls are served by the gateway (default "127.0.0.1:9379"); a
// data node answers the KV and vector surface only.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("statelet: dial %s: %w", addr, err)
	}
	return &Client{
		conn:      conn,
		stub:      pb.NewStateletClient(conn),
		agent:     pb.NewAgentStateServiceClient(conn),
		DefaultCF: 0,
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ── KV operations ───────────────────────────────────────────────────────────

// Ping performs a liveness check. Returns "PONG".
func (c *Client) Ping(ctx context.Context) (string, error) {
	resp, err := c.stub.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		return "", err
	}
	return resp.Message, nil
}

// Put writes a single key-value pair.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.PutCF(ctx, c.DefaultCF, key, value)
}

// PutCF writes a single key-value pair to a specific column family.
func (c *Client) PutCF(ctx context.Context, cf uint32, key, value []byte) error {
	_, err := c.stub.Put(ctx, &pb.PutRequest{Cf: cf, Key: key, Value: value})
	return err
}

// Get reads the value for a key. Returns (nil, nil) if not found.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.GetCF(ctx, c.DefaultCF, key)
}

// GetCF reads the value for a key from a specific column family.
func (c *Client) GetCF(ctx context.Context, cf uint32, key []byte) ([]byte, error) {
	resp, err := c.stub.Get(ctx, &pb.GetRequest{Cf: cf, Key: key})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return resp.Value, nil
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.DeleteCF(ctx, c.DefaultCF, key)
}

// DeleteCF removes a key from a specific column family.
func (c *Client) DeleteCF(ctx context.Context, cf uint32, key []byte) error {
	_, err := c.stub.Delete(ctx, &pb.DeleteRequest{Cf: cf, Key: key})
	return err
}

// Merge merges an operand into the existing value.
func (c *Client) Merge(ctx context.Context, key, value []byte) error {
	return c.MergeCF(ctx, c.DefaultCF, key, value)
}

// MergeCF merges an operand into the existing value in a specific column family.
func (c *Client) MergeCF(ctx context.Context, cf uint32, key, value []byte) error {
	_, err := c.stub.Merge(ctx, &pb.MergeRequest{Cf: cf, Key: key, Value: value})
	return err
}

// ScanEntry is a single key-value pair returned by Scan.
type ScanEntry struct {
	Key   []byte
	Value []byte
}

// Scan returns up to limit entries whose keys start with prefix, beginning at
// cursor (nil for the first page). It returns the entries and the next cursor;
// the next cursor is nil when there are no more results.
//
// cf uses *uint32 so that nil selects the client's default CF while an explicit
// 0 selects CF 0.
func (c *Client) Scan(ctx context.Context, prefix, cursor []byte, limit uint32, cf *uint32) ([]ScanEntry, []byte, error) {
	effectiveCF := c.DefaultCF
	if cf != nil {
		effectiveCF = *cf
	}
	resp, err := c.stub.Scan(ctx, &pb.ScanRequest{
		Cf:     effectiveCF,
		Prefix: prefix,
		Cursor: cursor,
		Limit:  limit,
	})
	if err != nil {
		return nil, nil, err
	}
	entries := make([]ScanEntry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = ScanEntry{Key: e.Key, Value: e.Value}
	}
	var next []byte
	if len(resp.NextCursor) > 0 {
		next = resp.NextCursor
	}
	return entries, next, nil
}

// BatchWrite atomically applies a batch of write operations.
func (c *Client) BatchWrite(ctx context.Context, ops []WriteOp) error {
	entries := make([]*pb.WriteEntry, len(ops))
	for i, op := range ops {
		cf := op.CF
		if cf == 0 {
			cf = c.DefaultCF
		}
		entries[i] = &pb.WriteEntry{
			Cf:    cf,
			Op:    op.Op,
			Key:   op.Key,
			Value: op.Value,
		}
	}
	_, err := c.stub.BatchWrite(ctx, &pb.BatchWriteRequest{Entries: entries})
	return err
}

// ── Vector operations ───────────────────────────────────────────────────────

// CreateVectorIndex creates or reconfigures an HNSW vector index.
func (c *Client) CreateVectorIndex(ctx context.Context, name string, cfg VectorIndexConfig) error {
	_, err := c.stub.CreateVectorIndex(ctx, &pb.CreateVectorIndexRequest{
		IndexName: name,
		Config: &pb.VectorIndexConfig{
			Dim:            cfg.Dim,
			Metric:         cfg.Metric,
			M:              cfg.M,
			MMax0:          cfg.MMax0,
			EfConstruction: cfg.EfConstruction,
			EfSearch:       cfg.EfSearch,
		},
	})
	return err
}

// DropVectorIndex drops an HNSW vector index.
func (c *Client) DropVectorIndex(ctx context.Context, name string) error {
	_, err := c.stub.DropVectorIndex(ctx, &pb.DropVectorIndexRequest{IndexName: name})
	return err
}

// VectorPut inserts or updates a vector.
func (c *Client) VectorPut(ctx context.Context, indexName string, vectorID uint64, vector []float32) error {
	_, err := c.stub.VectorPut(ctx, &pb.VectorPutRequest{
		IndexName: indexName,
		VectorId:  vectorID,
		Vector:    vector,
	})
	return err
}

// VectorDelete removes a vector from the index.
func (c *Client) VectorDelete(ctx context.Context, indexName string, vectorID uint64) error {
	_, err := c.stub.VectorDelete(ctx, &pb.VectorDeleteRequest{
		IndexName: indexName,
		VectorId:  vectorID,
	})
	return err
}

// VectorSearch performs approximate nearest neighbor search.
func (c *Client) VectorSearch(ctx context.Context, indexName string, query []float32, k uint32, efSearch uint32) ([]VectorSearchResult, error) {
	return c.VectorSearchReranked(ctx, indexName, query, k, efSearch, nil)
}

// RerankSpec configures the optional second-stage reranker for VectorSearch.
//
// Mirrors Weaviate `.with_additional({rerank:{property,query}})` and Pinecone
// `inference.rerank(model, query, documents, rank_fields)`:
//
//   - Model "cross-encoder" runs a loaded cross-encoder over passages hydrated
//     from the KV store via PassageField (a key template with {id}/{index}
//     tokens, e.g. "doc:{index}:{id}:text") using QueryText. Requires a
//     reranker loaded on the gateway; otherwise auto-downgrades to score-fusion.
//   - Model "score-fusion" (the default, "" ⇒ "score-fusion") is LLM-free: it
//     re-sorts by SignalBlend*norm_distance + (1-SignalBlend)*aux_signal. With
//     0 < SignalBlend < 1 on a quantized index the gateway fetches the
//     full-precision vector and blends the exact distance (Qdrant
//     prefetch→rescore lift); SignalBlend == 1.0 is a pure re-sort.
//
// See docs/reranking.md for details.
type RerankSpec struct {
	RerankK      uint32
	Model        string
	PassageField string
	SignalBlend  float32
	QueryText    string
}

func (r *RerankSpec) toProto(validateOnly bool) *pb.RerankSpec {
	return &pb.RerankSpec{
		Enabled:      true,
		RerankK:      r.RerankK,
		Model:        r.Model,
		PassageField: r.PassageField,
		SignalBlend:  r.SignalBlend,
		QueryText:    r.QueryText,
		ValidateOnly: validateOnly,
	}
}

// VectorSearchReranked is VectorSearch with an optional second-stage reranker.
// Pass rerank == nil for no rerank (identical to VectorSearch).
func (c *Client) VectorSearchReranked(ctx context.Context, indexName string, query []float32, k uint32, efSearch uint32, rerank *RerankSpec) ([]VectorSearchResult, error) {
	req := &pb.VectorSearchRequest{
		IndexName: indexName,
		Query:     query,
		K:         k,
		EfSearch:  efSearch,
	}
	if rerank != nil {
		req.Rerank = rerank.toProto(false)
	}
	resp, err := c.stub.VectorSearch(ctx, req)
	if err != nil {
		return nil, err
	}
	results := make([]VectorSearchResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = VectorSearchResult{ID: r.Id, Distance: r.Distance, GroupKey: r.GroupKey}
	}
	return results, nil
}

// VectorSearchGrouped is VectorSearch with result grouping / field-collapse
// (epic #1427). See GroupSpec. Grouping is mutually exclusive with rerank/MMR;
// it is exact on single-shard deployments and best-effort across shards (tune
// via GroupSpec.Overfetch). Each result's group value is surfaced on
// VectorSearchResult.GroupKey.
func (c *Client) VectorSearchGrouped(ctx context.Context, indexName string, query []float32, k uint32, efSearch uint32, group GroupSpec) ([]VectorSearchResult, error) {
	req := &pb.VectorSearchRequest{
		IndexName:         indexName,
		Query:             query,
		K:                 k,
		EfSearch:          efSearch,
		GroupField:        group.Field,
		GroupSize:         group.GroupSize,
		Groups:            group.Groups,
		GroupOverfetch:    group.Overfetch,
		GroupMissingAsOwn: group.MissingAsOwn,
	}
	resp, err := c.stub.VectorSearch(ctx, req)
	if err != nil {
		return nil, err
	}
	results := make([]VectorSearchResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = VectorSearchResult{ID: r.Id, Distance: r.Distance, GroupKey: r.GroupKey}
	}
	return results, nil
}

// RerankValidate performs a dry-run pre-flight validation of a RerankSpec.
//
// It issues a validate_only vector search that validates the PassageField
// template (and, for Model == "cross-encoder", that a reranker is loaded on the
// gateway) without executing the search. Returns nil when the spec is valid; the
// underlying InvalidArgument / FailedPrecondition gRPC status otherwise. Mirrors
// Weaviate's "property exists?" / Pinecone's "rank_fields valid?" pre-flight.
func (c *Client) RerankValidate(ctx context.Context, indexName string, rerank RerankSpec) error {
	_, err := c.stub.VectorSearch(ctx, &pb.VectorSearchRequest{
		IndexName: indexName,
		Query:     nil,
		K:         1,
		Rerank:    rerank.toProto(true),
	})
	return err
}

// VectorGet retrieves a stored vector by id. Returns (nil, nil) if not found.
func (c *Client) VectorGet(ctx context.Context, indexName string, vectorID uint64) ([]float32, error) {
	resp, err := c.stub.VectorGet(ctx, &pb.VectorGetRequest{
		IndexName: indexName,
		VectorId:  vectorID,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, nil
	}
	return resp.Vector, nil
}
