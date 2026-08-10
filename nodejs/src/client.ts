import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

// Resolved against the package root, so it works the same from `src/` (ts-node)
// and from the compiled `dist/`. The proto is shipped inside the published
// tarball — see the `files` list in package.json — because a consumer of the
// npm package has no repository checkout to reach back into.
const DEFAULT_PROTO_PATH = path.join(__dirname, "..", "proto", "statelet.proto");
const PROTO_LOADER_OPTIONS: protoLoader.Options = {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  bytes: Buffer,
};

export const WriteOp = Object.freeze({
  PUT: "PUT",
  DELETE: "DELETE",
  MERGE: "MERGE",
} as const);

export type WriteOpValue = (typeof WriteOp)[keyof typeof WriteOp];

export const VectorDistanceMetric = Object.freeze({
  VECTOR_L2: "VECTOR_L2",
  VECTOR_COSINE: "VECTOR_COSINE",
  VECTOR_INNER_PRODUCT: "VECTOR_INNER_PRODUCT",
} as const);

export type VectorDistanceMetricValue =
  (typeof VectorDistanceMetric)[keyof typeof VectorDistanceMetric];

export const VectorIndexType = Object.freeze({
  VECTOR_INDEX_HNSW: "VECTOR_INDEX_HNSW",
  VECTOR_INDEX_PQ_HNSW: "VECTOR_INDEX_PQ_HNSW",
  VECTOR_INDEX_SQ_HNSW: "VECTOR_INDEX_SQ_HNSW",
  VECTOR_INDEX_IVF_PQ: "VECTOR_INDEX_IVF_PQ",
  VECTOR_INDEX_IVF_SQ: "VECTOR_INDEX_IVF_SQ",
  VECTOR_INDEX_SPFRESH: "VECTOR_INDEX_SPFRESH",
} as const);

export type VectorIndexTypeValue = (typeof VectorIndexType)[keyof typeof VectorIndexType];

const METRIC_ALIASES: Record<string, VectorDistanceMetricValue> = Object.freeze({
  l2: VectorDistanceMetric.VECTOR_L2,
  cosine: VectorDistanceMetric.VECTOR_COSINE,
  inner_product: VectorDistanceMetric.VECTOR_INNER_PRODUCT,
  innerproduct: VectorDistanceMetric.VECTOR_INNER_PRODUCT,
});

const INDEX_TYPE_ALIASES: Record<string, VectorIndexTypeValue> = Object.freeze({
  hnsw: VectorIndexType.VECTOR_INDEX_HNSW,
  pq_hnsw: VectorIndexType.VECTOR_INDEX_PQ_HNSW,
  sq_hnsw: VectorIndexType.VECTOR_INDEX_SQ_HNSW,
  ivf_pq: VectorIndexType.VECTOR_INDEX_IVF_PQ,
  ivf_sq: VectorIndexType.VECTOR_INDEX_IVF_SQ,
  spfresh: VectorIndexType.VECTOR_INDEX_SPFRESH,
});

let cachedProto: any = null;

export interface PlainMetadata {
  [key: string]: string | Buffer | Array<string | Buffer>;
}

export interface StateletClientOptions {
  defaultCf?: number;
  metadata?: grpc.Metadata | PlainMetadata;
  credentials?: grpc.ChannelCredentials;
  channelOptions?: grpc.ChannelOptions;
  protoPath?: string;
}

export interface BatchWriteEntryInput {
  op: WriteOpValue | string;
  key: Buffer | Uint8Array | string;
  value?: Buffer | Uint8Array | string;
  cf?: number;
}

export interface VectorIndexConfig {
  dim: number;
  metric: VectorDistanceMetricValue;
  m: number;
  mMax0: number;
  efConstruction: number;
  efSearch: number;
  indexType: VectorIndexTypeValue;
  nlist: number;
  nprobe: number;
  pqNumSub: number;
  pqNumCentroids: number;
  pqMaxIter: number;
  ivfMaxIter: number;
  splitThreshold: number;
  mergeThreshold: number;
  compactDeleteRatio: number;
  nlistCoarse: number;
  nprobeCoarse: number;
}

export interface ScanOptions {
  cursor?: Buffer | Uint8Array | string | null;
  limit?: number;
  cf?: number;
  metadata?: grpc.Metadata | PlainMetadata;
}

export interface ScanResult {
  entries: Array<{ key: Buffer; value: Buffer }>;
  nextCursor: Buffer | null;
  hasMore: boolean;
  partialFailure: boolean;
}

export interface VectorSearchResult {
  id: bigint;
  distance: number;
  /**
   * Field-collapse group key (epic #1427). Empty string unless the search set a
   * {@link GroupSpec}; otherwise the candidate's `groupField` payload value
   * rendered to its canonical string. Re-bucket on this to present grouped
   * results.
   */
  groupKey: string;
}

/**
 * Result grouping / field-collapse options for
 * {@link StateletClient.vectorSearchGrouped} (epic #1427): collapse results to at
 * most `groupSize` hits per distinct value of the payload `field`, returning up
 * to `groups` distinct group keys, ordered by ascending distance. The analogue
 * of Qdrant `query_groups` / Weaviate `groupBy` / Milvus `grouping_field`.
 */
export interface GroupSpec {
  /** Payload field to group by (empty ⇒ grouping off). */
  field: string;
  /** Max hits per group (`0` ⇒ 1, one-best-per-group). */
  groupSize?: number;
  /** Number of distinct group keys to return (`0` ⇒ falls back to `k`). */
  groups?: number;
  /** Candidate over-fetch multiplier (`0` ⇒ default 4, capped server-side). */
  overfetch?: number;
  /**
   * When `true`, candidates missing `field` are returned as their own singleton
   * group (empty `groupKey`) instead of being dropped (default).
   */
  missingAsOwn?: boolean;
}

/**
 * Optional second-stage reranker for {@link StateletClient.vectorSearch}.
 *
 * Mirrors Weaviate `.with_additional({rerank:{property,query}})` and Pinecone
 * `inference.rerank(model, query, documents, rank_fields)`:
 *
 * - `model: "cross-encoder"` runs a loaded cross-encoder over passages hydrated
 *   from the KV store via `passageField` (a key template with `{id}`/`{index}`
 *   tokens, e.g. `"doc:{index}:{id}:text"`) using `queryText`. Requires a
 *   reranker on the gateway; otherwise auto-downgrades to score-fusion.
 * - `model: "score-fusion"` (the default) is LLM-free: re-sort by
 *   `signalBlend * norm_distance + (1 - signalBlend) * aux_signal`. With
 *   `0 < signalBlend < 1` on a quantized index the gateway blends the exact
 *   distance (Qdrant prefetch→rescore lift); `signalBlend === 1` is a pure
 *   re-sort. See `docs/reranking.md`.
 */
export interface RerankSpec {
  enabled?: boolean;
  rerankK?: number;
  model?: string;
  passageField?: string;
  signalBlend?: number;
  queryText?: string;
  validateOnly?: boolean;
}

function rerankToWire(rerank: RerankSpec, validateOnly?: boolean): Record<string, unknown> {
  return {
    enabled: rerank.enabled ?? true,
    rerankK: rerank.rerankK ?? 0,
    model: rerank.model ?? "",
    passageField: rerank.passageField ?? "",
    signalBlend: rerank.signalBlend ?? 0,
    queryText: rerank.queryText ?? "",
    validateOnly: validateOnly ?? rerank.validateOnly ?? false,
  };
}

/** Which member of a {@link GraphValue} is meaningful. */
export const GraphValueKind = Object.freeze({
  NULL: "NULL",
  INT: "INT",
  DOUBLE: "DOUBLE",
  STRING: "STRING",
  BOOL: "BOOL",
  JSON: "JSON",
} as const);

export type GraphValueKindValue = (typeof GraphValueKind)[keyof typeof GraphValueKind];

/**
 * One projected column value of a {@link StateletClient.graphQuery}. Exactly one
 * member is meaningful, selected by `kind`; `jsonValue` carries the hydrated
 * `ROLE_NodeProp` blob for a whole node, verbatim.
 */
export interface GraphValue {
  kind: GraphValueKindValue;
  intValue: bigint;
  dblValue: number;
  strValue: string;
  boolValue: boolean;
  jsonValue: Buffer;
}

/**
 * Out-of-band knobs for {@link StateletClient.graphQuery}. Every field is
 * optional; omitting them all means "let the gateway decide" — the default
 * graph, no extra row cap, no temporal filter on either axis.
 */
export interface GraphQueryOptions {
  /** Graph index to query (empty ⇒ the gateway's default graph). */
  graphName?: string;
  /**
   * Hard cap on returned rows regardless of any `LIMIT` in the query
   * (`0` ⇒ no extra cap; a parsed `LIMIT` still applies).
   */
  maxRows?: number;
  /**
   * Valid-time the query is evaluated against, in ms (`0` ⇒ current). An
   * `AS OF` clause in the query text overrides it.
   */
  asOf?: bigint | number | string;
  /** Transaction-time the query is evaluated against, in ms (`0` ⇒ current). */
  txAsOf?: bigint | number | string;
  metadata?: grpc.Metadata | PlainMetadata;
}

/**
 * The projected result set of a {@link StateletClient.graphQuery}. `warnings` is
 * non-empty when the result may be incomplete — e.g. a label scan hit the
 * per-shard frontier cap, so the anchor set was truncated.
 */
export interface GraphQueryResult {
  /** `RETURN` column names, in projection order. */
  columns: string[];
  /** Result rows, each in `columns` order. */
  rows: GraphValue[][];
  /** Non-fatal query warnings. */
  warnings: string[];
}

/**
 * A {@link GraphValue} as its natural JS type: `null` for NULL, else `bigint` /
 * `number` / `string` / `boolean` / `Buffer`. An unknown kind (a newer server)
 * reads as `null`.
 */
export function graphValueToJs(value: GraphValue): bigint | number | string | boolean | Buffer | null {
  switch (value.kind) {
    case GraphValueKind.INT:
      return value.intValue;
    case GraphValueKind.DOUBLE:
      return value.dblValue;
    case GraphValueKind.STRING:
      return value.strValue;
    case GraphValueKind.BOOL:
      return value.boolValue;
    case GraphValueKind.JSON:
      return value.jsonValue;
    default:
      return null;
  }
}

/** The rows of a graph query as column-keyed objects of natural JS values. */
export function graphRowsToObjects(result: GraphQueryResult): Array<Record<string, ReturnType<typeof graphValueToJs>>> {
  return result.rows.map((row) => {
    const obj: Record<string, ReturnType<typeof graphValueToJs>> = {};
    row.forEach((value, i) => {
      const column = result.columns[i];
      if (column !== undefined) {
        obj[column] = graphValueToJs(value);
      }
    });
    return obj;
  });
}

export interface VectorBatchPutEntryInput {
  vectorId: bigint | number | string;
  vector: number[];
}

export interface VectorSampleResult {
  vectors: number[];
  dim: number;
  count: number;
}

export interface GetNodeStatsResult {
  nodeId: bigint;
  shardStats: unknown[];
  cpuUsagePercent: number;
  memoryUsedBytes: bigint;
  memoryTotalBytes: bigint;
  diskUsedBytes: bigint;
  diskTotalBytes: bigint;
  walBytesWrittenTotal: number;
  blockCacheHitsTotal: number;
  blockCacheMissesTotal: number;
  blockCacheEvictionsTotal: number;
  memtableFreezeTotal: number;
  memtableFlushTotal: number;
  walFileSizeBytes: bigint;
  vectorIndexStats: unknown[];
}

function loadProto(protoPath: string = DEFAULT_PROTO_PATH): any {
  if (cachedProto && protoPath === DEFAULT_PROTO_PATH) {
    return cachedProto;
  }

  const packageDefinition = protoLoader.loadSync(protoPath, PROTO_LOADER_OPTIONS);
  const loaded = grpc.loadPackageDefinition(packageDefinition) as any;
  const statelet = loaded.statelet?.v1;
  if (!statelet) {
    throw new Error(`Unable to load Statelet protobuf package from ${protoPath}`);
  }

  if (protoPath === DEFAULT_PROTO_PATH) {
    cachedProto = statelet;
  }
  return statelet;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && (value as Record<string, unknown>).constructor === Object;
}

function toBuffer(value: Buffer | Uint8Array | string, fieldName: string): Buffer {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  if (typeof value === "string") {
    return Buffer.from(value, "utf8");
  }
  throw new TypeError(`${fieldName} must be a Buffer, Uint8Array, or string`);
}

function toOptionalBuffer(
  value: Buffer | Uint8Array | string | null | undefined,
  fieldName: string
): Buffer {
  if (value === undefined || value === null) {
    return Buffer.alloc(0);
  }
  return toBuffer(value, fieldName);
}

function toUInt64String(value: bigint | number | string, fieldName: string): string {
  if (typeof value === "bigint") {
    if (value < 0n) {
      throw new RangeError(`${fieldName} must be non-negative`);
    }
    return value.toString();
  }

  if (typeof value === "number") {
    if (!Number.isInteger(value) || value < 0) {
      throw new RangeError(`${fieldName} must be a non-negative integer`);
    }
    return String(value);
  }

  if (typeof value === "string" && /^\d+$/.test(value)) {
    return value;
  }

  throw new TypeError(`${fieldName} must be a bigint, number, or decimal string`);
}

function toBigInt(value: string | number | bigint): bigint {
  return BigInt(typeof value === "string" ? value : String(value));
}

function normalizeMetric(value?: VectorDistanceMetricValue | string): VectorDistanceMetricValue {
  if (!value) {
    return VectorDistanceMetric.VECTOR_L2;
  }
  if (Object.values(VectorDistanceMetric).includes(value as VectorDistanceMetricValue)) {
    return value as VectorDistanceMetricValue;
  }
  const alias = METRIC_ALIASES[String(value).toLowerCase()];
  if (alias) {
    return alias;
  }
  throw new TypeError(`Unsupported vector metric: ${value}`);
}

function normalizeIndexType(value?: VectorIndexTypeValue | string): VectorIndexTypeValue {
  if (!value) {
    return VectorIndexType.VECTOR_INDEX_HNSW;
  }
  if (Object.values(VectorIndexType).includes(value as VectorIndexTypeValue)) {
    return value as VectorIndexTypeValue;
  }
  const alias = INDEX_TYPE_ALIASES[String(value).toLowerCase()];
  if (alias) {
    return alias;
  }
  throw new TypeError(`Unsupported vector index type: ${value}`);
}

function normalizeMetadata(input?: grpc.Metadata | PlainMetadata): grpc.Metadata {
  if (!input) {
    return new grpc.Metadata();
  }

  if (input instanceof grpc.Metadata) {
    return input;
  }

  if (!isPlainObject(input)) {
    throw new TypeError("metadata must be a grpc.Metadata or a plain object");
  }

  const metadata = new grpc.Metadata();
  for (const [key, value] of Object.entries(input)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        metadata.add(key, item);
      }
      continue;
    }
    metadata.set(key, value as string | Buffer);
  }
  return metadata;
}

function mergeMetadata(base?: grpc.Metadata | PlainMetadata, override?: grpc.Metadata | PlainMetadata): grpc.Metadata {
  const merged = new grpc.Metadata();

  for (const source of [base, override]) {
    const normalized = normalizeMetadata(source);
    const map = normalized.getMap();
    for (const [key, value] of Object.entries(map)) {
      merged.set(key, value);
    }
  }

  return merged;
}

function normalizeVector(vector: number[], fieldName: string): number[] {
  if (!Array.isArray(vector)) {
    throw new TypeError(`${fieldName} must be an array of numbers`);
  }
  return vector.map((value) => Number(value));
}

function normalizeWriteEntry(entry: WriteEntryBuilder | BatchWriteEntryInput, defaultCf: number) {
  if (entry instanceof WriteEntryBuilder) {
    return entry.build(defaultCf);
  }
  if (!isPlainObject(entry)) {
    throw new TypeError("Batch entries must be WriteEntryBuilder instances or plain objects");
  }

  const op = String(entry.op || "").toUpperCase() as WriteOpValue;
  if (!Object.values(WriteOp).includes(op)) {
    throw new TypeError(`Unsupported write op: ${entry.op}`);
  }

  return {
    cf: entry.cf ?? defaultCf,
    op,
    key: toBuffer(entry.key, "entry.key"),
    value: op === WriteOp.DELETE ? Buffer.alloc(0) : toOptionalBuffer(entry.value, "entry.value"),
  };
}

function normalizeVectorConfig(config: VectorIndexConfigBuilder | Partial<VectorIndexConfig>): VectorIndexConfig {
  if (config instanceof VectorIndexConfigBuilder) {
    return config.build();
  }
  if (!isPlainObject(config)) {
    throw new TypeError("config must be a VectorIndexConfigBuilder or plain object");
  }

  const dim = Number(config.dim);
  if (!Number.isInteger(dim) || dim <= 0) {
    throw new RangeError("config.dim must be a positive integer");
  }

  return {
    dim,
    metric: normalizeMetric(config.metric),
    m: Number(config.m ?? 16),
    mMax0: Number(config.mMax0 ?? 0),
    efConstruction: Number(config.efConstruction ?? 200),
    efSearch: Number(config.efSearch ?? 64),
    indexType: normalizeIndexType(config.indexType),
    nlist: Number(config.nlist ?? 0),
    nprobe: Number(config.nprobe ?? 0),
    pqNumSub: Number(config.pqNumSub ?? 0),
    pqNumCentroids: Number(config.pqNumCentroids ?? 0),
    pqMaxIter: Number(config.pqMaxIter ?? 0),
    ivfMaxIter: Number(config.ivfMaxIter ?? 0),
    splitThreshold: Number(config.splitThreshold ?? 0),
    mergeThreshold: Number(config.mergeThreshold ?? 0),
    compactDeleteRatio: Number(config.compactDeleteRatio ?? 0),
    nlistCoarse: Number(config.nlistCoarse ?? 0),
    nprobeCoarse: Number(config.nprobeCoarse ?? 0),
  };
}

export class WriteEntryBuilder {
  private readonly op: WriteOpValue;
  private readonly key: Buffer | Uint8Array | string;
  private readonly value: Buffer | Uint8Array | string;
  private readonly cf: number | null;

  private constructor(
    op: WriteOpValue,
    key: Buffer | Uint8Array | string,
    value: Buffer | Uint8Array | string = Buffer.alloc(0),
    cf: number | null = null
  ) {
    this.op = op;
    this.key = key;
    this.value = value;
    this.cf = cf;
  }

  static put(key: Buffer | Uint8Array | string, value: Buffer | Uint8Array | string): WriteEntryBuilder {
    return new WriteEntryBuilder(WriteOp.PUT, key, value);
  }

  static delete(key: Buffer | Uint8Array | string): WriteEntryBuilder {
    return new WriteEntryBuilder(WriteOp.DELETE, key, Buffer.alloc(0));
  }

  static merge(key: Buffer | Uint8Array | string, value: Buffer | Uint8Array | string): WriteEntryBuilder {
    return new WriteEntryBuilder(WriteOp.MERGE, key, value);
  }

  withCf(cf: number): WriteEntryBuilder {
    return new WriteEntryBuilder(this.op, this.key, this.value, cf);
  }

  build(defaultCf: number) {
    return {
      cf: this.cf ?? defaultCf,
      op: this.op,
      key: toBuffer(this.key, "key"),
      value: this.op === WriteOp.DELETE ? Buffer.alloc(0) : toOptionalBuffer(this.value, "value"),
    };
  }
}

export class VectorIndexConfigBuilder {
  private readonly config: VectorIndexConfig;

  constructor(dim: number) {
    const normalizedDim = Number(dim);
    if (!Number.isInteger(normalizedDim) || normalizedDim <= 0) {
      throw new RangeError("dim must be a positive integer");
    }

    this.config = {
      dim: normalizedDim,
      metric: VectorDistanceMetric.VECTOR_L2,
      m: 16,
      mMax0: 0,
      efConstruction: 200,
      efSearch: 64,
      indexType: VectorIndexType.VECTOR_INDEX_HNSW,
      nlist: 0,
      nprobe: 0,
      pqNumSub: 0,
      pqNumCentroids: 0,
      pqMaxIter: 0,
      ivfMaxIter: 0,
      splitThreshold: 0,
      mergeThreshold: 0,
      compactDeleteRatio: 0,
      nlistCoarse: 0,
      nprobeCoarse: 0,
    };
  }

  metric(metric: VectorDistanceMetricValue | string): this {
    this.config.metric = normalizeMetric(metric);
    return this;
  }

  m(value: number): this {
    this.config.m = Number(value);
    return this;
  }

  mMax0(value: number): this {
    this.config.mMax0 = Number(value);
    return this;
  }

  efConstruction(value: number): this {
    this.config.efConstruction = Number(value);
    return this;
  }

  efSearch(value: number): this {
    this.config.efSearch = Number(value);
    return this;
  }

  indexType(value: VectorIndexTypeValue | string): this {
    this.config.indexType = normalizeIndexType(value);
    return this;
  }

  nlist(value: number): this {
    this.config.nlist = Number(value);
    return this;
  }

  nprobe(value: number): this {
    this.config.nprobe = Number(value);
    return this;
  }

  pqNumSub(value: number): this {
    this.config.pqNumSub = Number(value);
    return this;
  }

  pqNumCentroids(value: number): this {
    this.config.pqNumCentroids = Number(value);
    return this;
  }

  pqMaxIter(value: number): this {
    this.config.pqMaxIter = Number(value);
    return this;
  }

  ivfMaxIter(value: number): this {
    this.config.ivfMaxIter = Number(value);
    return this;
  }

  splitThreshold(value: number): this {
    this.config.splitThreshold = Number(value);
    return this;
  }

  mergeThreshold(value: number): this {
    this.config.mergeThreshold = Number(value);
    return this;
  }

  compactDeleteRatio(value: number): this {
    this.config.compactDeleteRatio = Number(value);
    return this;
  }

  nlistCoarse(value: number): this {
    this.config.nlistCoarse = Number(value);
    return this;
  }

  nprobeCoarse(value: number): this {
    this.config.nprobeCoarse = Number(value);
    return this;
  }

  build(): VectorIndexConfig {
    return { ...this.config };
  }
}

export class StateletClient {
  public readonly target: string;
  public readonly defaultCf: number;
  private readonly defaultMetadata: grpc.Metadata;
  private readonly client: any;

  constructor(targetOrHost: string, portOrOptions?: number | StateletClientOptions, maybeDefaultCf?: number) {
    const parsed = this.parseConstructorArgs(targetOrHost, portOrOptions, maybeDefaultCf);
    const statelet = loadProto(parsed.protoPath);

    this.target = parsed.target;
    this.defaultCf = parsed.defaultCf;
    this.defaultMetadata = parsed.metadata;
    this.client = new statelet.Statelet(parsed.target, parsed.credentials, parsed.channelOptions);
  }

  private parseConstructorArgs(
    targetOrHost: string,
    portOrOptions?: number | StateletClientOptions,
    maybeDefaultCf?: number
  ) {
    if (typeof targetOrHost !== "string" || targetOrHost.length === 0) {
      throw new TypeError("targetOrHost must be a non-empty string");
    }

    let target: string;
    let options: StateletClientOptions = {};

    if (typeof portOrOptions === "number") {
      target = `${targetOrHost}:${portOrOptions}`;
      options.defaultCf = maybeDefaultCf ?? 0;
    } else if (portOrOptions === undefined || portOrOptions === null) {
      target = targetOrHost;
    } else if (isPlainObject(portOrOptions)) {
      target = targetOrHost;
      options = portOrOptions as StateletClientOptions;
    } else {
      throw new TypeError("Second constructor argument must be a port number or options object");
    }

    return {
      target,
      defaultCf: options.defaultCf ?? 0,
      metadata: normalizeMetadata(options.metadata),
      credentials: options.credentials ?? grpc.credentials.createInsecure(),
      channelOptions: options.channelOptions,
      protoPath: options.protoPath ?? DEFAULT_PROTO_PATH,
    };
  }

  waitForReady(deadlineMs: number = 5000): Promise<void> {
    const deadline = new Date(Date.now() + deadlineMs);
    return new Promise((resolve, reject) => {
      this.client.waitForReady(deadline, (err: Error | null) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  close(): void {
    this.client.close();
  }

  private call(method: string, request: object, metadata?: grpc.Metadata | PlainMetadata): Promise<any> {
    return new Promise((resolve, reject) => {
      const mergedMetadata = mergeMetadata(this.defaultMetadata, metadata);
      this.client[method](request, mergedMetadata, (err: Error | null, response: any) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(response);
      });
    });
  }

  async ping(metadata?: grpc.Metadata | PlainMetadata): Promise<string> {
    const resp = await this.call("Ping", {}, metadata);
    return resp.message;
  }

  async put(
    key: Buffer | Uint8Array | string,
    value: Buffer | Uint8Array | string,
    cf: number = this.defaultCf,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call("Put", { cf, key: toBuffer(key, "key"), value: toBuffer(value, "value") }, metadata);
  }

  async get(
    key: Buffer | Uint8Array | string,
    cf: number = this.defaultCf,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<Buffer | null> {
    const resp = await this.call("Get", { cf, key: toBuffer(key, "key") }, metadata);
    return resp.found ? Buffer.from(resp.value) : null;
  }

  async delete(
    key: Buffer | Uint8Array | string,
    cf: number = this.defaultCf,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call("Delete", { cf, key: toBuffer(key, "key") }, metadata);
  }

  async merge(
    key: Buffer | Uint8Array | string,
    value: Buffer | Uint8Array | string,
    cf: number = this.defaultCf,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call("Merge", { cf, key: toBuffer(key, "key"), value: toBuffer(value, "value") }, metadata);
  }

  async batchWrite(entries: Array<WriteEntryBuilder | BatchWriteEntryInput>, metadata?: grpc.Metadata | PlainMetadata): Promise<void> {
    if (!Array.isArray(entries)) {
      throw new TypeError("entries must be an array");
    }
    await this.call("BatchWrite", { entries: entries.map((entry) => normalizeWriteEntry(entry, this.defaultCf)) }, metadata);
  }

  async scan(prefix: Buffer | Uint8Array | string = Buffer.alloc(0), options: ScanOptions = {}): Promise<ScanResult> {
    const { cursor = null, limit = 100, cf = this.defaultCf, metadata } = options;
    const resp = await this.call(
      "Scan",
      { cf, prefix: toOptionalBuffer(prefix, "prefix"), cursor: toOptionalBuffer(cursor, "cursor"), limit },
      metadata
    );
    return {
      entries: resp.entries.map((entry: { key: Buffer; value: Buffer }) => ({
        key: Buffer.from(entry.key),
        value: Buffer.from(entry.value),
      })),
      nextCursor: resp.nextCursor && resp.nextCursor.length > 0 ? Buffer.from(resp.nextCursor) : null,
      hasMore: resp.hasMore,
      partialFailure: resp.partialFailure,
    };
  }

  async deleteByPrefix(
    prefix: Buffer | Uint8Array | string,
    options: { cf?: number; metadata?: grpc.Metadata | PlainMetadata } = {}
  ): Promise<number> {
    const { cf = this.defaultCf, metadata } = options;
    const resp = await this.call("DeleteByPrefix", { cf, prefix: toBuffer(prefix, "prefix") }, metadata);
    return resp.deleted;
  }

  async createVectorIndex(
    name: string,
    config: VectorIndexConfigBuilder | Partial<VectorIndexConfig>,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call("CreateVectorIndex", { indexName: name, config: normalizeVectorConfig(config) }, metadata);
  }

  async dropVectorIndex(name: string, metadata?: grpc.Metadata | PlainMetadata): Promise<void> {
    await this.call("DropVectorIndex", { indexName: name }, metadata);
  }

  async vectorPut(
    indexName: string,
    vectorId: bigint | number | string,
    vector: number[],
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call(
      "VectorPut",
      { indexName, vectorId: toUInt64String(vectorId, "vectorId"), vector: normalizeVector(vector, "vector") },
      metadata
    );
  }

  async vectorDelete(
    indexName: string,
    vectorId: bigint | number | string,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call("VectorDelete", { indexName, vectorId: toUInt64String(vectorId, "vectorId") }, metadata);
  }

  async vectorSearch(
    indexName: string,
    query: number[],
    k: number,
    efSearch: number = 0,
    metadata?: grpc.Metadata | PlainMetadata,
    rerank?: RerankSpec
  ): Promise<VectorSearchResult[]> {
    const req: Record<string, unknown> = { indexName, query: normalizeVector(query, "query"), k, efSearch };
    if (rerank) {
      req.rerank = rerankToWire(rerank);
    }
    const resp = await this.call("VectorSearch", req, metadata);
    return resp.results.map((result: { id: string | number | bigint; distance: number; groupKey?: string }) => ({
      id: toBigInt(result.id),
      distance: result.distance,
      groupKey: result.groupKey ?? "",
    }));
  }

  /**
   * Approximate nearest neighbor search with result grouping / field-collapse
   * (epic #1427).
   *
   * Collapse results to at most `group.groupSize` hits per distinct value of
   * `group.field`, returning up to `group.groups` distinct group keys (each
   * result's value surfaced on {@link VectorSearchResult.groupKey}). Grouping is
   * mutually exclusive with MMR; it is exact on single-shard deployments and
   * best-effort across shards (tune via `group.overfetch`).
   */
  async vectorSearchGrouped(
    indexName: string,
    query: number[],
    k: number,
    group: GroupSpec,
    efSearch: number = 0,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<VectorSearchResult[]> {
    const req: Record<string, unknown> = {
      indexName,
      query: normalizeVector(query, "query"),
      k,
      efSearch,
      groupField: group.field,
      groupSize: group.groupSize ?? 0,
      groups: group.groups ?? 0,
      groupOverfetch: group.overfetch ?? 0,
      groupMissingAsOwn: group.missingAsOwn ?? false,
    };
    const resp = await this.call("VectorSearch", req, metadata);
    return resp.results.map((result: { id: string | number | bigint; distance: number; groupKey?: string }) => ({
      id: toBigInt(result.id),
      distance: result.distance,
      groupKey: result.groupKey ?? "",
    }));
  }

  /**
   * Dry-run pre-flight validation of a {@link RerankSpec}.
   *
   * Issues a `validateOnly` vector search that validates the `passageField`
   * template (and, for `model: "cross-encoder"`, that a reranker is loaded on
   * the gateway) without executing the search. Resolves when the spec is valid;
   * rejects with an INVALID_ARGUMENT / FAILED_PRECONDITION gRPC error otherwise.
   * Mirrors Weaviate's "property exists?" / Pinecone's "rank_fields valid?".
   */
  async rerankValidate(
    indexName: string,
    rerank: RerankSpec,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call(
      "VectorSearch",
      { indexName, query: [], k: 1, rerank: rerankToWire(rerank, true) },
      metadata
    );
  }

  async vectorGet(
    indexName: string,
    vectorId: bigint | number | string,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<number[] | null> {
    const resp = await this.call("VectorGet", { indexName, vectorId: toUInt64String(vectorId, "vectorId") }, metadata);
    return resp.found ? resp.vector.map((value: number) => Number(value)) : null;
  }

  async vectorBatchPut(
    indexName: string,
    vectors: VectorBatchPutEntryInput[],
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<number> {
    if (!Array.isArray(vectors)) {
      throw new TypeError("vectors must be an array");
    }
    const resp = await this.call(
      "VectorBatchPut",
      {
        indexName,
        vectors: vectors.map((entry) => ({
          vectorId: toUInt64String(entry.vectorId, "entry.vectorId"),
          vector: normalizeVector(entry.vector, "entry.vector"),
        })),
      },
      metadata
    );
    return resp.inserted;
  }

  async vectorBatchDelete(
    indexName: string,
    vectorIds: Array<bigint | number | string>,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<number> {
    if (!Array.isArray(vectorIds)) {
      throw new TypeError("vectorIds must be an array");
    }
    const resp = await this.call(
      "VectorBatchDelete",
      { indexName, vectorIds: vectorIds.map((id) => toUInt64String(id, "vectorId")) },
      metadata
    );
    return resp.deleted;
  }

  async vectorTrain(
    indexName: string,
    centroids: number[] = [],
    dim: number = 0,
    metadata?: grpc.Metadata | PlainMetadata
  ): Promise<void> {
    await this.call("VectorTrain", { indexName, centroids: normalizeVector(centroids, "centroids"), dim }, metadata);
  }

  async vectorSample(indexName: string, maxSamples: number, metadata?: grpc.Metadata | PlainMetadata): Promise<VectorSampleResult> {
    const resp = await this.call("VectorSample", { indexName, maxSamples }, metadata);
    return { vectors: resp.vectors.map((value: number) => Number(value)), dim: resp.dim, count: resp.count };
  }

  /**
   * Run a read-only openCypher-subset query (the `GraphQuery` RPC).
   *
   * Gateway-only: the gateway parses and plans the query, then compiles it to
   * engine traversal primitives. The subset covers `MATCH` path patterns,
   * `WHERE` over node properties, `RETURN` / `ORDER BY` / `LIMIT`, a bitemporal
   * `AS OF <valid>[, <tx>]` clause, and the retrieval procedures
   * `db.vectorSearch` / `db.hybridSearch` / `db.graphRag`. `CREATE` / `MERGE`
   * are rejected.
   *
   * Named query parameters (`$q`) parse but are not resolvable yet, so a
   * vector-seeded procedure needs an inline literal:
   * `db.vectorSearch([0.1, 0.2, ...], 5)`.
   */
  async graphQuery(cypher: string, options: GraphQueryOptions = {}): Promise<GraphQueryResult> {
    if (typeof cypher !== "string" || cypher.length === 0) {
      throw new TypeError("cypher must be a non-empty string");
    }
    const resp = await this.call(
      "GraphQuery",
      {
        graphName: options.graphName ?? "",
        cypher,
        maxRows: options.maxRows ?? 0,
        asOf: toUInt64String(options.asOf ?? 0, "asOf"),
        txAsOf: toUInt64String(options.txAsOf ?? 0, "txAsOf"),
      },
      options.metadata
    );
    return {
      columns: resp.columns ?? [],
      rows: (resp.rows ?? []).map((row: { values?: any[] }) =>
        (row.values ?? []).map(
          (value: any): GraphValue => ({
            kind: value.kind as GraphValueKindValue,
            intValue: toBigInt(value.intValue ?? 0),
            dblValue: value.dblValue ?? 0,
            strValue: value.strValue ?? "",
            boolValue: value.boolValue ?? false,
            jsonValue: Buffer.from(value.jsonValue ?? []),
          })
        )
      ),
      warnings: resp.warnings ?? [],
    };
  }

  async getNodeStats(metadata?: grpc.Metadata | PlainMetadata): Promise<GetNodeStatsResult> {
    const resp = await this.call("GetNodeStats", {}, metadata);
    return {
      nodeId: toBigInt(resp.nodeId),
      shardStats: resp.shardStats,
      cpuUsagePercent: resp.cpuUsagePercent,
      memoryUsedBytes: toBigInt(resp.memoryUsedBytes),
      memoryTotalBytes: toBigInt(resp.memoryTotalBytes),
      diskUsedBytes: toBigInt(resp.diskUsedBytes),
      diskTotalBytes: toBigInt(resp.diskTotalBytes),
      walBytesWrittenTotal: resp.walBytesWrittenTotal,
      blockCacheHitsTotal: resp.blockCacheHitsTotal,
      blockCacheMissesTotal: resp.blockCacheMissesTotal,
      blockCacheEvictionsTotal: resp.blockCacheEvictionsTotal,
      memtableFreezeTotal: resp.memtableFreezeTotal,
      memtableFlushTotal: resp.memtableFlushTotal,
      walFileSizeBytes: toBigInt(resp.walFileSizeBytes),
      vectorIndexStats: resp.vectorIndexStats,
    };
  }
}
