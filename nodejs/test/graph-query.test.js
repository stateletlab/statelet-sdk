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

// Round-trip test for the declarative graph query surface: graphQuery must
// marshal the options onto the GraphQuery request (graph_name / cypher /
// max_rows / as_of / tx_as_of) and decode every GraphQueryValue kind back to a
// natural JS value.
//
// Drives the SDK against a minimal in-process gRPC server loaded from the same
// canonical proto, so the wire field names are exercised end-to-end.

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const { StateletClient, graphRowsToObjects, graphValueToJs } = require("../dist/index.js");

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

// A server whose GraphQuery records the request and replays one row carrying
// every value kind, plus a warning.
function startServer(captured) {
  return new Promise((resolve, reject) => {
    const statelet = loadStatelet();
    const server = new grpc.Server();
    server.addService(statelet.v1.Statelet.service, {
      GraphQuery: (call, callback) => {
        captured.req = call.request;
        callback(null, {
          columns: ["nul", "i", "d", "s", "b", "j"],
          rows: [
            {
              values: [
                { kind: "NULL" },
                { kind: "INT", intValue: "42" },
                { kind: "DOUBLE", dblValue: 0.5 },
                { kind: "STRING", strValue: "knows" },
                { kind: "BOOL", boolValue: true },
                { kind: "JSON", jsonValue: Buffer.from('{"name":"ada"}') },
              ],
            },
          ],
          warnings: ["label scan truncated at the frontier cap"],
        });
      },
    });
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, port) => {
      if (err) return reject(err);
      resolve({ server, port });
    });
  });
}

test("graphQuery marshals every option and decodes every value kind", async () => {
  const captured = {};
  const { server, port } = await startServer(captured);
  const client = new StateletClient(`127.0.0.1:${port}`);
  try {
    const result = await client.graphQuery("MATCH (n) RETURN n", {
      graphName: "g",
      maxRows: 25,
      asOf: 1737000000000n,
      txAsOf: 1737000000001n,
    });

    assert.equal(captured.req.cypher, "MATCH (n) RETURN n");
    assert.equal(captured.req.graphName, "g");
    assert.equal(Number(captured.req.maxRows), 25);
    assert.equal(String(captured.req.asOf), "1737000000000");
    assert.equal(String(captured.req.txAsOf), "1737000000001");

    assert.deepEqual(result.columns, ["nul", "i", "d", "s", "b", "j"]);
    assert.equal(result.warnings.length, 1);

    const row = result.rows[0];
    assert.equal(graphValueToJs(row[0]), null);
    assert.equal(graphValueToJs(row[1]), 42n);
    assert.equal(graphValueToJs(row[2]), 0.5);
    assert.equal(graphValueToJs(row[3]), "knows");
    assert.equal(graphValueToJs(row[4]), true);
    assert.equal(graphValueToJs(row[5]).toString(), '{"name":"ada"}');

    const objects = graphRowsToObjects(result);
    assert.equal(objects.length, 1);
    assert.equal(objects[0].i, 42n);
    assert.equal(objects[0].s, "knows");
    assert.equal(objects[0].nul, null);
  } finally {
    client.close();
    await new Promise((r) => server.tryShutdown(() => r()));
  }
});

test("graphQuery defaults leave every knob at the server default", async () => {
  const captured = {};
  const { server, port } = await startServer(captured);
  const client = new StateletClient(`127.0.0.1:${port}`);
  try {
    await client.graphQuery("MATCH (n) RETURN n");
    assert.equal(captured.req.graphName, "");
    assert.equal(Number(captured.req.maxRows), 0);
    assert.equal(Number(captured.req.asOf), 0);
    assert.equal(Number(captured.req.txAsOf), 0);
  } finally {
    client.close();
    await new Promise((r) => server.tryShutdown(() => r()));
  }
});

test("graphQuery rejects an empty query before hitting the wire", async () => {
  const client = new StateletClient("127.0.0.1:1");
  try {
    await assert.rejects(() => client.graphQuery(""), TypeError);
  } finally {
    client.close();
  }
});
