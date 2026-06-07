import { createServer } from "node:http";
import { dirname, extname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { chromium } from "playwright";

const require = createRequire(import.meta.url);
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const extensionPath = resolve(
  repoRoot,
  process.env.PBI_WASM_EXTENSION_PATH ||
    "build/wasm_eh/extension/pbi_scanner/pbi_scanner.duckdb_extension.wasm",
);

function requiredEnv(name) {
  const value = (process.env[name] || "").trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers":
      "Authorization, Content-Type, X-Transport-Caps-Negotiation-Flags, X-AS-ActivityID, X-AS-SessionID, X-AS-Get-Next, X-PowerBI-ResourceKey",
    "Access-Control-Allow-Private-Network": "true",
    "Access-Control-Expose-Headers": "*",
    ...extra,
  };
}

function contentType(path) {
  switch (extname(path)) {
    case ".html":
      return "text/html";
    case ".js":
    case ".mjs":
      return "text/javascript";
    case ".wasm":
      return "application/wasm";
    default:
      return "application/octet-stream";
  }
}

function listen(server) {
  return new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", rejectListen);
      resolveListen(server.address().port);
    });
  });
}

function bundleKeyForPlatform() {
  const platform = process.env.PBI_WASM_DUCKDB_PLATFORM;
  if (platform === "wasm_mvp") {
    return "mvp";
  }
  if (platform === "wasm_eh" || !platform) {
    return "eh";
  }
  throw new Error(`unsupported PBI_WASM_DUCKDB_PLATFORM: ${platform}`);
}

function normalizeValue(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (ArrayBuffer.isView(value)) {
    return Array.from(value);
  }
  if (Array.isArray(value)) {
    return value.map(normalizeValue);
  }
  if (typeof value === "object") {
    const result = {};
    for (const key of Object.keys(value).sort()) {
      result[key] = normalizeValue(value[key]);
    }
    return result;
  }
  if (typeof value === "number" && Object.is(value, -0)) {
    return 0;
  }
  return value;
}

async function startLiveServer() {
  const duckdbDist = dirname(require.resolve("@duckdb/duckdb-wasm"));
  const arrowDist = dirname(require.resolve("apache-arrow"));
  const flatbuffersDist = dirname(dirname(require.resolve("flatbuffers")));
  const tslibDist = dirname(require.resolve("tslib"));
  const artifact = readFileSync(extensionPath);
  const targetXmlaUrl = requiredEnv("PBI_WASM_TARGET_XMLA_URL");
  const accessToken = requiredEnv("PBI_WASM_ACCESS_TOKEN");
  const extensionRequests = [];
  const xmlaRequests = [];
  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>pbi_scanner live wasm test</title>
    <script type="importmap">
      {
        "imports": {
          "apache-arrow": "/apache-arrow/Arrow.dom.mjs",
          "flatbuffers": "/flatbuffers/mjs/flatbuffers.js",
          "tslib": "/tslib/tslib.es6.mjs"
        }
      }
    </script>
  </head>
  <body>
    <script type="module">
      import * as duckdb from "/duckdb/duckdb-browser.mjs";

      ${normalizeValue.toString()}

      window.runLivePbi = async (config) => {
        const bundles = {
          mvp: {
            mainModule: "/duckdb/duckdb-mvp.wasm",
            mainWorker: "/duckdb/duckdb-browser-mvp.worker.js",
          },
          eh: {
            mainModule: "/duckdb/duckdb-eh.wasm",
            mainWorker: "/duckdb/duckdb-browser-eh.worker.js",
          },
        };
        const bundle = bundles[config.bundleKey];
        if (!bundle) {
          throw new Error("unknown DuckDB-Wasm bundle key: " + config.bundleKey);
        }

        const logger = new duckdb.ConsoleLogger();
        const worker = new Worker(bundle.mainWorker);
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        await db.open({ allowUnsignedExtensions: true });
        const conn = await db.connect();

        try {
          const versionResult = await conn.query("PRAGMA version");
          console.log("duckdb_version", JSON.stringify(versionResult.toArray()));
          await conn.query("SET custom_extension_repository='" + config.extensionRepository + "'");
          await conn.query("INSTALL pbi_scanner");
          await conn.query("LOAD pbi_scanner");

          const result = await conn.query(config.sql);
          const rows = result.toArray().map(normalizeValue);
          return {
            columns: result.schema.fields.map((field) => field.name),
            rowCount: rows.length,
            rows,
          };
        } finally {
          await conn.close();
          await db.terminate();
          worker.terminate();
        }
      };
    </script>
  </body>
</html>`;

  const server = createServer((req, res) => {
    if (req.method === "OPTIONS") {
      res.writeHead(204, corsHeaders());
      res.end();
      return;
    }

    const url = new URL(req.url, "http://127.0.0.1");
    if (url.pathname === "/") {
      res.writeHead(200, corsHeaders({ "Content-Type": "text/html" }));
      res.end(html);
      return;
    }

    if (url.pathname === "/xmla") {
      let body = "";
      req.setEncoding("utf8");
      req.on("data", (chunk) => {
        body += chunk;
      });
      req.on("end", async () => {
        xmlaRequests.push({
          method: req.method,
          url: req.url,
          authorization: req.headers.authorization || "",
          byteLength: Buffer.byteLength(body),
        });
        try {
          const headers = {
            "Authorization": req.headers.authorization || `Bearer ${accessToken}`,
            "Content-Type": req.headers["content-type"] || "text/xml",
          };
          for (const header of [
            "x-transport-caps-negotiation-flags",
            "x-as-activityid",
            "x-as-sessionid",
            "x-as-get-next",
            "x-powerbi-resourcekey",
          ]) {
            if (req.headers[header]) {
              headers[header] = req.headers[header];
            }
          }
          const upstream = await fetch(targetXmlaUrl, {
            method: req.method,
            headers,
            body,
          });
          const responseBody = Buffer.from(await upstream.arrayBuffer());
          const responseHeaders = corsHeaders({
            "Content-Type": upstream.headers.get("content-type") || "text/xml",
          });
          for (const header of [
            "x-transport-caps-negotiation-flags",
            "x-as-activityid",
            "x-as-sessionid",
            "x-as-get-next",
            "x-powerbi-resourcekey",
          ]) {
            const value = upstream.headers.get(header);
            if (value) {
              responseHeaders[header] = value;
            }
          }
          res.writeHead(upstream.status, responseHeaders);
          res.end(responseBody);
        } catch (error) {
          res.writeHead(502, corsHeaders({ "Content-Type": "text/plain" }));
          res.end(String(error && error.message ? error.message : error));
        }
      });
      return;
    }

    if (url.pathname.startsWith("/duckdb/")) {
      const filePath = resolve(duckdbDist, url.pathname.slice("/duckdb/".length));
      if (!filePath.startsWith(duckdbDist)) {
        res.writeHead(403, corsHeaders());
        res.end("forbidden");
        return;
      }
      try {
        res.writeHead(200, corsHeaders({ "Content-Type": contentType(filePath) }));
        res.end(readFileSync(filePath));
      } catch {
        res.writeHead(404, corsHeaders());
        res.end("not found");
      }
      return;
    }

    if (url.pathname.startsWith("/apache-arrow/")) {
      const filePath = resolve(arrowDist, url.pathname.slice("/apache-arrow/".length));
      if (!filePath.startsWith(arrowDist)) {
        res.writeHead(403, corsHeaders());
        res.end("forbidden");
        return;
      }
      try {
        res.writeHead(200, corsHeaders({ "Content-Type": contentType(filePath) }));
        res.end(readFileSync(filePath));
      } catch {
        res.writeHead(404, corsHeaders());
        res.end("not found");
      }
      return;
    }

    if (url.pathname.startsWith("/tslib/")) {
      const filePath = resolve(tslibDist, url.pathname.slice("/tslib/".length));
      if (!filePath.startsWith(tslibDist)) {
        res.writeHead(403, corsHeaders());
        res.end("forbidden");
        return;
      }
      try {
        res.writeHead(200, corsHeaders({ "Content-Type": contentType(filePath) }));
        res.end(readFileSync(filePath));
      } catch {
        res.writeHead(404, corsHeaders());
        res.end("not found");
      }
      return;
    }

    if (url.pathname.startsWith("/flatbuffers/")) {
      const filePath = resolve(flatbuffersDist, url.pathname.slice("/flatbuffers/".length));
      if (!filePath.startsWith(flatbuffersDist)) {
        res.writeHead(403, corsHeaders());
        res.end("forbidden");
        return;
      }
      try {
        res.writeHead(200, corsHeaders({ "Content-Type": contentType(filePath) }));
        res.end(readFileSync(filePath));
      } catch {
        res.writeHead(404, corsHeaders());
        res.end("not found");
      }
      return;
    }

    if (url.pathname.endsWith("pbi_scanner.duckdb_extension.wasm")) {
      extensionRequests.push(url.pathname);
      res.writeHead(200, corsHeaders({ "Content-Type": "application/wasm" }));
      res.end(artifact);
      return;
    }

    res.writeHead(404, corsHeaders());
    res.end("not found");
  });
  const port = await listen(server);
  return {
    server,
    extensionRequests,
    xmlaRequests,
    url: `http://127.0.0.1:${port}`,
  };
}

async function main() {
  const connectionString = requiredEnv("PBI_WASM_CONNECTION_STRING");
  const dax = requiredEnv("PBI_WASM_DAX");
  const accessToken = requiredEnv("PBI_WASM_ACCESS_TOKEN");
  const server = await startLiveServer();
  let browser;

  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error(`[browser:error] ${error.message}`));
    await page.goto(server.url, { waitUntil: "load" });
    await page.waitForFunction(() => typeof window.runLivePbi === "function");

    const proxiedConnectionString = connectionString.replace(
      /Data Source=[^;]+/i,
      `Data Source=${server.url}/xmla`,
    );
    const escapedConnection = proxiedConnectionString.replaceAll("'", "''");
    const escapedDax = dax.replaceAll("'", "''");
    const escapedToken = accessToken.replaceAll("'", "''");
    const sql = `
      SELECT *
      FROM dax_query(
        '${escapedConnection}',
        '${escapedDax}',
        auth_mode := 'access_token',
        access_token := '${escapedToken}'
      )
    `;

    const result = await page.evaluate(
      ({ bundleKey, extensionRepository, sql }) =>
        window.runLivePbi({ bundleKey, extensionRepository, sql }),
      {
        bundleKey: bundleKeyForPlatform(),
        extensionRepository: server.url,
        sql,
      },
    );

    console.log(JSON.stringify({ ok: true, result }));
  } finally {
    if (server.extensionRequests.length) {
      console.error(`extension requests: ${server.extensionRequests.join(", ")}`);
    }
    if (server.xmlaRequests.length) {
      console.error(
        `xmla requests: ${server.xmlaRequests
          .map((request) => `${request.method} ${request.url} auth=${request.authorization || "<none>"} bytes=${request.byteLength}`)
          .join(", ")}`,
      );
    } else {
      console.error("xmla requests: <none>");
    }
    if (browser) {
      await browser.close();
    }
    server.server.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
