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

// Round-trip test for result grouping / field-collapse (epic #1427, Phase 3):
// vectorSearchGrouped must marshal the GroupSpec onto the VectorSearch request
// (group_field / group_size / groups / group_overfetch / group_missing_as_own)
// and surface the response group_key on each VectorSearchResult.
//
// Drives the SDK against a minimal in-process gRPC server loaded from the same
// canonical proto, so the wire field names are exercised end-to-end.

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const { StateletClient } = require("../dist/index.js");

const PROTO_PATH = path.join(__dirname, "..", "proto", "statelet.proto");
const LOADER_OPTS = {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  bytes: Buffer,
};

function loadStatelet() {
  const def = protoLoader.loadSync(PROTO_PATH, LOADER_OPTS);
  return grpc.loadPackageDefinition(def).statelet;
}

// Start a server whose VectorSearch records the request and echoes the grouping
// inputs back so the test can assert both the request marshalling and the
// group_key surfacing on the client.
function startServer(captured) {
  return new Promise((resolve, reject) => {
    const statelet = loadStatelet();
    const server = new grpc.Server();
    server.addService(statelet.v1.Statelet.service, {
      VectorSearch: (call, callback) => {
        captured.req = call.request;
        callback(null, {
          results: [
            { id: "10", distance: 0.1, groupKey: call.request.groupField || "" },
            { id: "20", distance: 0.2, groupKey: "docB" },
          ],
        });
      },
    });
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, port) => {
      if (err) return reject(err);
      resolve({ server, port });
    });
  });
}

test("vectorSearchGrouped round-trips grouping kwargs and surfaces groupKey", async () => {
  const captured = {};
  const { server, port } = await startServer(captured);
  const client = new StateletClient(`127.0.0.1:${port}`);
  try {
    const results = await client.vectorSearchGrouped("idx", [0, 0, 0, 0], 5, {
      field: "doc_id",
      groupSize: 2,
      groups: 3,
      overfetch: 4,
      missingAsOwn: true,
    });

    // Grouping kwargs marshalled onto the wire request.
    assert.equal(captured.req.groupField, "doc_id");
    assert.equal(Number(captured.req.groupSize), 2);
    assert.equal(Number(captured.req.groups), 3);
    assert.equal(Number(captured.req.groupOverfetch), 4);
    assert.equal(captured.req.groupMissingAsOwn, true);

    // group_key surfaced on each result.
    assert.equal(results.length, 2);
    assert.equal(results[0].groupKey, "doc_id");
    assert.equal(results[1].groupKey, "docB");
    assert.equal(results[0].id, 10n);
  } finally {
    client.close();
    await new Promise((r) => server.tryShutdown(() => r()));
  }
});

test("plain vectorSearch leaves grouping off and still exposes groupKey", async () => {
  const captured = {};
  const { server, port } = await startServer(captured);
  const client = new StateletClient(`127.0.0.1:${port}`);
  try {
    const results = await client.vectorSearch("idx", [0, 0, 0, 0], 1);
    assert.equal(captured.req.groupField, "");
    // groupKey present (empty for the non-grouped first result echoed by server).
    assert.equal(results[0].groupKey, "");
  } finally {
    client.close();
    await new Promise((r) => server.tryShutdown(() => r()));
  }
});
