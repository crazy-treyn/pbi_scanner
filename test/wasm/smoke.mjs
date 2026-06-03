import * as duckdb from "@duckdb/duckdb-wasm";
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import Worker from "web-worker";

const require = createRequire(import.meta.url);
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const extensionPath = resolve(
  repoRoot,
  process.env.PBI_WASM_EXTENSION_PATH ||
    "build/wasm_eh/extension/pbi_scanner/pbi_scanner.duckdb_extension.wasm",
);

const xmlaResponse = `<?xml version="1.0" encoding="utf-8"?>
<root>
  <schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:schema>
      <xsd:complexType name="row">
        <xsd:sequence>
          <xsd:element name="probe_ok" type="xsd:int" />
        </xsd:sequence>
      </xsd:complexType>
    </xsd:schema>
  </schema>
  <row><probe_ok>1</probe_ok></row>
</root>`;

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type, X-Transport-Caps-Negotiation-Flags, X-AS-ActivityID, X-AS-SessionID, X-AS-Get-Next, X-PowerBI-ResourceKey",
    "Access-Control-Expose-Headers": "*",
    ...extra,
  };
}

function listen(server) {
  return new Promise((resolveListen) => {
    server.listen(0, "127.0.0.1", () => {
      resolveListen(server.address().port);
    });
  });
}

async function startExtensionServer() {
  const artifact = readFileSync(extensionPath);
  const server = createServer((req, res) => {
    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      res.end();
      return;
    }
    if (!req.url.endsWith("pbi_scanner.duckdb_extension.wasm")) {
      res.writeHead(404, corsHeaders());
      res.end("not found");
      return;
    }
    res.writeHead(200, corsHeaders({ "Content-Type": "application/wasm" }));
    res.end(artifact);
  });
  const port = await listen(server);
  return { server, url: `http://127.0.0.1:${port}` };
}

async function startXmlaServer() {
  const requests = [];
  const server = createServer((req, res) => {
    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      res.end();
      return;
    }
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      requests.push({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization || "",
        body,
      });
      res.writeHead(
        200,
        corsHeaders({
          "Content-Type": "text/xml",
          "X-Transport-Caps-Negotiation-Flags": "0,0,0,0,0",
        }),
      );
      res.end(xmlaResponse);
    });
  });
  const port = await listen(server);
  return { server, requests, url: `http://127.0.0.1:${port}/xmla` };
}

async function instantiateDuckDB() {
  const dist = dirname(require.resolve("@duckdb/duckdb-wasm"));
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: resolve(dist, "duckdb-mvp.wasm"),
      mainWorker: resolve(dist, "duckdb-node-mvp.worker.cjs"),
    },
    eh: {
      mainModule: resolve(dist, "duckdb-eh.wasm"),
      mainWorker: resolve(dist, "duckdb-node-eh.worker.cjs"),
    },
  });
  const logger = new duckdb.ConsoleLogger();
  const worker = new Worker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await db.open({ allowUnsignedExtensions: true });
  return { db, worker };
}

async function main() {
  const extensionServer = await startExtensionServer();
  const xmlaServer = await startXmlaServer();
  const { db, worker } = await instantiateDuckDB();
  const conn = await db.connect();

  try {
    await conn.query(`SET custom_extension_repository='${extensionServer.url}'`);
    await conn.query("INSTALL pbi_scanner");
    await conn.query("LOAD pbi_scanner");

    const functions = await conn.query(`
      SELECT function_name
      FROM duckdb_functions()
      WHERE function_name IN ('dax_query', 'pbi_tables', 'pbi_columns', 'pbi_measures', 'pbi_relationships')
      ORDER BY function_name
    `);
    const names = functions.toArray().map((row) => row.function_name);
    for (const expected of ["dax_query", "pbi_columns", "pbi_measures", "pbi_relationships", "pbi_tables"]) {
      if (!names.includes(expected)) {
        throw new Error(`missing registered function ${expected}`);
      }
    }

    const result = await conn.query(`
      SELECT *
      FROM dax_query(
        'Data Source=${xmlaServer.url};Initial Catalog=mock;',
        'EVALUATE ROW("probe_ok", 1)',
        auth_mode := 'access_token',
        access_token := 'mock-token'
      )
    `);
    const rows = result.toArray();
    if (rows.length !== 1 || Number(rows[0].probe_ok) !== 1) {
      throw new Error(`unexpected dax_query result: ${JSON.stringify(rows)}`);
    }
    if (!xmlaServer.requests.some((request) => request.authorization === "Bearer mock-token")) {
      throw new Error("mock server did not observe expected Authorization header");
    }

    let sawUnsupportedAuth = false;
    try {
      await conn.query(`
        SELECT *
        FROM dax_query(
          'Data Source=${xmlaServer.url};Initial Catalog=mock;',
          'EVALUATE ROW("probe_ok", 1)',
          auth_mode := 'azure_cli'
        )
      `);
    } catch (error) {
      sawUnsupportedAuth = String(error.message || error).includes("azure_cli auth is not supported");
    }
    if (!sawUnsupportedAuth) {
      throw new Error("azure_cli WASM auth error was not observed");
    }

    console.log("pbi_scanner WASM smoke test passed");
  } finally {
    await conn.close();
    await db.terminate();
    worker.terminate();
    extensionServer.server.close();
    xmlaServer.server.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
