package ai.statelet.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import statelet.v1.StateletProto.*;
import statelet.v1.StateletGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Synchronous gRPC client for Statelet.
 *
 * <pre>{@code
 * try (StateletClient client = new StateletClient("127.0.0.1", 7379)) {
 *     client.put("hello".getBytes(), "world".getBytes());
 *     byte[] value = client.get("hello".getBytes()).orElse(null);
 *     client.delete("hello".getBytes());
 * }
 * }</pre>
 */
public class StateletClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final StateletGrpc.StateletBlockingStub stub;
    private final int defaultCf;

    public StateletClient(String host, int port) {
        this(host, port, 0);
    }

    public StateletClient(String host, int port, int defaultCf) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = StateletGrpc.newBlockingStub(channel);
        this.defaultCf = defaultCf;
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ── KV operations ───────────────────────────────────────────────────

    /** Liveness check. Returns "PONG". */
    public String ping() {
        PingResponse resp = stub.ping(PingRequest.getDefaultInstance());
        return resp.getMessage();
    }

    /** Write a single key-value pair using the default column family. */
    public void put(byte[] key, byte[] value) {
        put(key, value, defaultCf);
    }

    /** Write a single key-value pair. */
    public void put(byte[] key, byte[] value, int cf) {
        stub.put(PutRequest.newBuilder()
                .setCf(cf)
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build());
    }

    /** Read the value for a key. Returns empty if not found. */
    public Optional<byte[]> get(byte[] key) {
        return get(key, defaultCf);
    }

    /** Read the value for a key. Returns empty if not found. */
    public Optional<byte[]> get(byte[] key, int cf) {
        GetResponse resp = stub.get(GetRequest.newBuilder()
                .setCf(cf)
                .setKey(ByteString.copyFrom(key))
                .build());
        return resp.getFound() ? Optional.of(resp.getValue().toByteArray()) : Optional.empty();
    }

    /** Delete a key. */
    public void delete(byte[] key) {
        delete(key, defaultCf);
    }

    /** Delete a key. */
    public void delete(byte[] key, int cf) {
        stub.delete(DeleteRequest.newBuilder()
                .setCf(cf)
                .setKey(ByteString.copyFrom(key))
                .build());
    }

    /** Merge an operand into the existing value. */
    public void merge(byte[] key, byte[] value) {
        merge(key, value, defaultCf);
    }

    /** Merge an operand into the existing value. */
    public void merge(byte[] key, byte[] value, int cf) {
        stub.merge(MergeRequest.newBuilder()
                .setCf(cf)
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build());
    }

    /** Atomically apply a batch of write operations. */
    public void batchWrite(List<WriteEntryBuilder> entries) {
        BatchWriteRequest.Builder req = BatchWriteRequest.newBuilder();
        for (WriteEntryBuilder e : entries) {
            req.addEntries(e.build(defaultCf));
        }
        stub.batchWrite(req.build());
    }

    // ── Vector operations ───────────────────────────────────────────────

    /** Create or reconfigure an HNSW vector index. */
    public void createVectorIndex(String name, VectorIndexConfigBuilder config) {
        stub.createVectorIndex(CreateVectorIndexRequest.newBuilder()
                .setIndexName(name)
                .setConfig(config.build())
                .build());
    }

    /** Drop an HNSW vector index. */
    public void dropVectorIndex(String name) {
        stub.dropVectorIndex(DropVectorIndexRequest.newBuilder()
                .setIndexName(name)
                .build());
    }

    /** Insert or update a vector. */
    public void vectorPut(String indexName, long vectorId, List<Float> vector) {
        stub.vectorPut(VectorPutRequest.newBuilder()
                .setIndexName(indexName)
                .setVectorId(vectorId)
                .addAllVector(vector)
                .build());
    }

    /** Remove a vector. */
    public void vectorDelete(String indexName, long vectorId) {
        stub.vectorDelete(VectorDeleteRequest.newBuilder()
                .setIndexName(indexName)
                .setVectorId(vectorId)
                .build());
    }

    /** Approximate nearest neighbor search. */
    public List<VectorResult> vectorSearch(String indexName, List<Float> query, int k) {
        return vectorSearch(indexName, query, k, 0);
    }

    /** Approximate nearest neighbor search with custom ef_search. */
    public List<VectorResult> vectorSearch(String indexName, List<Float> query, int k, int efSearch) {
        return vectorSearch(indexName, query, k, efSearch, null);
    }

    /**
     * Approximate nearest neighbor search with an optional second-stage
     * reranker. Pass {@code rerank == null} for no rerank.
     *
     * <p>Build the spec with {@code RerankSpec.newBuilder()}, mirroring Weaviate
     * {@code .with_additional({rerank:{property,query}})} and Pinecone
     * {@code inference.rerank}. See {@code docs/reranking.md}.
     */
    public List<VectorResult> vectorSearch(String indexName, List<Float> query, int k, int efSearch, RerankSpec rerank) {
        VectorSearchRequest.Builder req = VectorSearchRequest.newBuilder()
                .setIndexName(indexName)
                .addAllQuery(query)
                .setK(k)
                .setEfSearch(efSearch);
        if (rerank != null) {
            req.setRerank(rerank);
        }
        VectorSearchResponse resp = stub.vectorSearch(req.build());
        List<VectorResult> results = new ArrayList<>();
        for (VectorSearchResult r : resp.getResultsList()) {
            results.add(new VectorResult(r.getId(), r.getDistance(), r.getGroupKey()));
        }
        return results;
    }

    /**
     * Approximate nearest neighbor search with result grouping / field-collapse
     * (epic #1427).
     *
     * <p>Collapse results to at most {@code group.groupSize()} hits per distinct
     * value of {@code group.field()}, returning up to {@code group.groups()}
     * distinct group keys (each result's value surfaced on
     * {@link VectorResult#groupKey()}). Grouping is mutually exclusive with MMR;
     * it is exact on single-shard deployments and best-effort across shards
     * (tune via {@code group.overfetch()}). The analogue of Qdrant
     * {@code query_groups} / Weaviate {@code groupBy} / Milvus
     * {@code grouping_field}.
     */
    public List<VectorResult> vectorSearchGrouped(String indexName, List<Float> query, int k, int efSearch, GroupSpec group) {
        VectorSearchRequest req = VectorSearchRequest.newBuilder()
                .setIndexName(indexName)
                .addAllQuery(query)
                .setK(k)
                .setEfSearch(efSearch)
                .setGroupField(group.field())
                .setGroupSize(group.groupSize())
                .setGroups(group.groups())
                .setGroupOverfetch(group.overfetch())
                .setGroupMissingAsOwn(group.missingAsOwn())
                .build();
        VectorSearchResponse resp = stub.vectorSearch(req);
        List<VectorResult> results = new ArrayList<>();
        for (VectorSearchResult r : resp.getResultsList()) {
            results.add(new VectorResult(r.getId(), r.getDistance(), r.getGroupKey()));
        }
        return results;
    }

    /**
     * Dry-run pre-flight validation of a {@link RerankSpec}.
     *
     * <p>Issues a {@code validate_only} vector search that validates the
     * {@code passage_field} template (and, for {@code model="cross-encoder"},
     * that a reranker is loaded on the gateway) without executing the search.
     * Returns normally when the spec is valid; throws
     * {@link io.grpc.StatusRuntimeException} ({@code INVALID_ARGUMENT} /
     * {@code FAILED_PRECONDITION}) otherwise. Mirrors Weaviate's "property
     * exists?" / Pinecone's "rank_fields valid?" pre-flight.
     */
    public void rerankValidate(String indexName, RerankSpec rerank) {
        RerankSpec spec = rerank.toBuilder()
                .setEnabled(true)
                .setValidateOnly(true)
                .build();
        stub.vectorSearch(VectorSearchRequest.newBuilder()
                .setIndexName(indexName)
                .setK(1)
                .setRerank(spec)
                .build());
    }

    /** Retrieve a stored vector by id. */
    public Optional<List<Float>> vectorGet(String indexName, long vectorId) {
        VectorGetResponse resp = stub.vectorGet(VectorGetRequest.newBuilder()
                .setIndexName(indexName)
                .setVectorId(vectorId)
                .build());
        return resp.getFound() ? Optional.of(resp.getVectorList()) : Optional.empty();
    }

    // ── Declarative graph query (openCypher subset) ─────────────────────

    /** Run a read-only openCypher-subset query with the default options. */
    public GraphQueryResult graphQuery(String cypher) {
        return graphQuery(cypher, GraphQueryOptions.defaults());
    }

    /**
     * Run a read-only openCypher-subset query.
     *
     * <p>Gateway-only: the gateway parses and plans the query, then compiles it
     * to engine traversal primitives. The subset covers {@code MATCH} path
     * patterns, {@code WHERE} over node properties, {@code RETURN} /
     * {@code ORDER BY} / {@code LIMIT}, a bitemporal
     * {@code AS OF <valid>[, <tx>]} clause, and the retrieval procedures
     * {@code db.vectorSearch} / {@code db.hybridSearch} / {@code db.graphRag}.
     * {@code CREATE} / {@code MERGE} are rejected.
     *
     * <p>Named query parameters ({@code $q}) parse but are not resolvable yet,
     * so a vector-seeded procedure needs an inline literal —
     * {@code db.vectorSearch([0.1, 0.2, ...], 5)}.
     */
    public GraphQueryResult graphQuery(String cypher, GraphQueryOptions options) {
        GraphQueryResponse resp = stub.graphQuery(GraphQueryRequest.newBuilder()
                .setGraphName(options.graphName())
                .setCypher(cypher)
                .setMaxRows(options.maxRows())
                .setAsOf(options.asOf())
                .setTxAsOf(options.txAsOf())
                .build());
        List<List<GraphValue>> rows = new ArrayList<>(resp.getRowsCount());
        for (GraphQueryRow row : resp.getRowsList()) {
            List<GraphValue> values = new ArrayList<>(row.getValuesCount());
            for (GraphQueryValue v : row.getValuesList()) {
                values.add(GraphValue.of(v));
            }
            rows.add(values);
        }
        return new GraphQueryResult(resp.getColumnsList(), rows, resp.getWarningsList());
    }

    // ── Helper types ────────────────────────────────────────────────────

    /**
     * Out-of-band knobs for {@link StateletClient#graphQuery}.
     *
     * @param graphName graph index to query (empty ⇒ the gateway's default graph)
     * @param maxRows   hard cap on returned rows regardless of any {@code LIMIT}
     *                  in the query (0 ⇒ no extra cap; a parsed {@code LIMIT}
     *                  still applies)
     * @param asOf      valid-time the query is evaluated against, in ms
     *                  (0 ⇒ current); an {@code AS OF} clause in the query text
     *                  overrides it
     * @param txAsOf    transaction-time the query is evaluated against, in ms
     *                  (0 ⇒ current)
     */
    public record GraphQueryOptions(String graphName, int maxRows, long asOf, long txAsOf) {
        /** Let the gateway decide: default graph, no extra cap, no temporal filter. */
        public static GraphQueryOptions defaults() {
            return new GraphQueryOptions("", 0, 0L, 0L);
        }

        /** Query a named graph, defaults elsewhere. */
        public GraphQueryOptions(String graphName) {
            this(graphName, 0, 0L, 0L);
        }
    }

    /**
     * One projected column value. Exactly one member is meaningful, selected by
     * {@code kind}; {@code jsonValue} carries the hydrated {@code ROLE_NodeProp}
     * blob for a whole node, verbatim.
     */
    public record GraphValue(GraphQueryValue.Kind kind, long intValue, double doubleValue,
                             String stringValue, boolean boolValue, byte[] jsonValue) {

        static GraphValue of(GraphQueryValue v) {
            return new GraphValue(v.getKind(), v.getIntValue(), v.getDblValue(),
                    v.getStrValue(), v.getBoolValue(), v.getJsonValue().toByteArray());
        }

        /**
         * The value as its natural Java type: {@code null} for NULL, else
         * {@code Long} / {@code Double} / {@code String} / {@code Boolean} /
         * {@code byte[]}. An unknown kind (a newer server) reads as {@code null}.
         */
        public Object asObject() {
            return switch (kind) {
                case INT -> intValue;
                case DOUBLE -> doubleValue;
                case STRING -> stringValue;
                case BOOL -> boolValue;
                case JSON -> jsonValue;
                default -> null;
            };
        }
    }

    /**
     * The projected result set of a {@link StateletClient#graphQuery}.
     *
     * @param columns  {@code RETURN} column names, in projection order
     * @param rows     result rows, each in {@code columns} order
     * @param warnings non-fatal warnings; non-empty when the result may be
     *                 incomplete (e.g. a label scan hit the per-shard frontier cap)
     */
    public record GraphQueryResult(List<String> columns, List<List<GraphValue>> rows,
                                   List<String> warnings) {
    }

    /**
     * A single nearest-neighbor result. {@code groupKey} is the field-collapse
     * group key (epic #1427) — the candidate's {@code group_field} payload value
     * rendered to its canonical string, or empty when the search was not grouped.
     */
    public record VectorResult(long id, float distance, String groupKey) {
        /** Back-compat constructor for ungrouped results (empty group key). */
        public VectorResult(long id, float distance) {
            this(id, distance, "");
        }
    }

    /**
     * Result grouping / field-collapse options for
     * {@link StateletClient#vectorSearchGrouped} (epic #1427).
     *
     * @param field        payload field to group by (empty ⇒ grouping off)
     * @param groupSize    max hits per group (0 ⇒ 1, one-best-per-group)
     * @param groups       number of distinct group keys to return (0 ⇒ falls back to k)
     * @param overfetch    candidate over-fetch multiplier (0 ⇒ default 4, capped server-side)
     * @param missingAsOwn when true, candidates missing {@code field} become their
     *                     own singleton group (empty group key) instead of being
     *                     dropped (default)
     */
    public record GroupSpec(String field, int groupSize, int groups, int overfetch, boolean missingAsOwn) {
        /** Convenience: group by {@code field}, defaults elsewhere, missing-field dropped. */
        public GroupSpec(String field) {
            this(field, 0, 0, 0, false);
        }
    }

    /** Builder for batch write entries. */
    public static class WriteEntryBuilder {
        private final WriteOp op;
        private final byte[] key;
        private final byte[] value;
        private final Integer cf;

        private WriteEntryBuilder(WriteOp op, byte[] key, byte[] value, Integer cf) {
            this.op = op;
            this.key = key;
            this.value = value;
            this.cf = cf;
        }

        public static WriteEntryBuilder put(byte[] key, byte[] value) {
            return new WriteEntryBuilder(WriteOp.PUT, key, value, null);
        }

        public static WriteEntryBuilder delete(byte[] key) {
            return new WriteEntryBuilder(WriteOp.DELETE, key, new byte[0], null);
        }

        public static WriteEntryBuilder merge(byte[] key, byte[] value) {
            return new WriteEntryBuilder(WriteOp.MERGE, key, value, null);
        }

        public WriteEntryBuilder withCf(int cf) {
            return new WriteEntryBuilder(op, key, value, cf);
        }

        WriteEntry build(int defaultCf) {
            return WriteEntry.newBuilder()
                    .setCf(cf != null ? cf : defaultCf)
                    .setOp(op)
                    .setKey(ByteString.copyFrom(key))
                    .setValue(ByteString.copyFrom(value))
                    .build();
        }
    }

    /** Builder for vector index configuration. */
    public static class VectorIndexConfigBuilder {
        private final int dim;
        private VectorDistanceMetric metric = VectorDistanceMetric.VECTOR_L2;
        private int m = 16;
        private int mMax0 = 0;
        private int efConstruction = 200;
        private int efSearch = 64;

        public VectorIndexConfigBuilder(int dim) {
            this.dim = dim;
        }

        public VectorIndexConfigBuilder metric(VectorDistanceMetric metric) {
            this.metric = metric;
            return this;
        }

        public VectorIndexConfigBuilder m(int m) {
            this.m = m;
            return this;
        }

        public VectorIndexConfigBuilder mMax0(int mMax0) {
            this.mMax0 = mMax0;
            return this;
        }

        public VectorIndexConfigBuilder efConstruction(int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        public VectorIndexConfigBuilder efSearch(int efSearch) {
            this.efSearch = efSearch;
            return this;
        }

        VectorIndexConfig build() {
            return VectorIndexConfig.newBuilder()
                    .setDim(dim)
                    .setMetric(metric)
                    .setM(m)
                    .setMMax0(mMax0)
                    .setEfConstruction(efConstruction)
                    .setEfSearch(efSearch)
                    .build();
        }
    }
}
