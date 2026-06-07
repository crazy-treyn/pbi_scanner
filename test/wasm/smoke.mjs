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

function patchDuckDbMvpWorker(source) {
  if (!source.includes("_setThrew") || source.includes("function _setThrew(")) {
    return source;
  }
  const marker = "Module._setTempRet0=_setTempRet0;";
  const patch =
    "var __pbiScannerThrew=0,__pbiScannerThrewValue=0;" +
    "function _setThrew(threw,value){" +
    "if(threw){if(!__pbiScannerThrew){__pbiScannerThrew=threw;__pbiScannerThrewValue=value;}}" +
    "else{__pbiScannerThrew=0;__pbiScannerThrewValue=value;}" +
    "}" +
    "Module._setThrew=_setThrew;";
  if (!source.includes(marker)) {
    return patch + source;
  }
  return source.replace(marker, marker + patch);
}

async function startBrowserAssetServer() {
  const duckdbDist = dirname(require.resolve("@duckdb/duckdb-wasm"));
  const arrowDist = dirname(require.resolve("apache-arrow"));
  const flatbuffersDist = dirname(dirname(require.resolve("flatbuffers")));
  const tslibDist = dirname(require.resolve("tslib"));
  const artifact = readFileSync(extensionPath);
  const extensionRequests = [];
  const xmlaRequests = [];
  const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>pbi_scanner wasm smoke</title>
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

      async function expectQueryError(conn, sql, expectedSubstring, label) {
        try {
          await conn.query(sql);
        } catch (error) {
          const message = String(error && error.message ? error.message : error);
          if (message.includes(expectedSubstring)) {
            return;
          }
          throw new Error(label + ": unexpected error message: " + message);
        }
        throw new Error(label + ": expected query to fail");
      }

      window.runPbiSmoke = async (config) => {
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

          const functions = await conn.query(\`
            SELECT function_name
            FROM duckdb_functions()
            WHERE function_name IN ('dax_query', 'pbi_tables', 'pbi_columns', 'pbi_measures', 'pbi_relationships')
            ORDER BY function_name
          \`);
          const names = functions.toArray().map((row) => row.function_name);
          for (const expected of ["dax_query", "pbi_columns", "pbi_measures", "pbi_relationships", "pbi_tables"]) {
            if (!names.includes(expected)) {
              throw new Error("missing registered function " + expected);
            }
          }

          const result = await conn.query(\`
            SELECT *
            FROM dax_query(
              'Data Source=\${config.xmlaUrl};Initial Catalog=mock;',
              'EVALUATE ROW("probe_ok", 1)',
              auth_mode := 'access_token',
              access_token := 'mock-token'
            )
          \`);
          const rows = result.toArray();
          if (rows.length !== 1 || Number(rows[0].probe_ok) !== 1) {
            throw new Error("unexpected dax_query result: " + JSON.stringify(rows));
          }

          let negativeChecks = "skipped";
          if (config.bundleKey === "eh") {
            await expectQueryError(
              conn,
              \`
                SELECT *
                FROM dax_query(
                  'Data Source=\${config.xmlaUrl};Initial Catalog=mock;',
                  'EVALUATE ROW("probe_ok", 1)',
                  auth_mode := 'azure_cli'
                )
              \`,
              "azure_cli auth is not supported",
              "azure_cli WASM auth rejection",
            );

            await expectQueryError(
              conn,
              \`
                SELECT *
                FROM dax_query(
                  'Data Source=\${config.xmlaUrl};Initial Catalog=mock;',
                  'EVALUATE ROW("probe_ok", 1)',
                  auth_mode := 'service_principal',
                  tenant_id := 'tenant',
                  client_id := 'client',
                  client_secret := 'secret'
                )
              \`,
              "service_principal auth is not supported",
              "service_principal WASM auth rejection",
            );

            await expectQueryError(
              conn,
              \`
                SELECT *
                FROM dax_query(
                  'Data Source=powerbi://api.powerbi.com/v1.0/myorg/Example%20Workspace;Initial Catalog=example;',
                  'EVALUATE ROW("probe_ok", 1)',
                  auth_mode := 'access_token',
                  access_token := 'mock-token'
                )
              \`,
              "powerbi:// locators are not supported directly in DuckDB-Wasm",
              "powerbi:// WASM bind rejection",
            );
            negativeChecks = "passed";
          }

          return {
            rows: rows.map((row) => ({ probe_ok: Number(row.probe_ok) })),
            functions: names,
            negativeChecks,
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
      req.on("end", () => {
        xmlaRequests.push({
          method: req.method,
          url: req.url,
          authorization: req.headers.authorization || "",
          headers: req.headers,
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
      return;
    }

    if (url.pathname.startsWith("/duckdb/")) {
      const fileName = url.pathname.slice("/duckdb/".length);
      const filePath = resolve(duckdbDist, fileName);
      if (!filePath.startsWith(duckdbDist)) {
        res.writeHead(403, corsHeaders());
        res.end("forbidden");
        return;
      }
      try {
        if (url.pathname.endsWith("duckdb-browser-mvp.worker.js")) {
          res.writeHead(200, corsHeaders({ "Content-Type": "text/javascript" }));
          res.end(patchDuckDbMvpWorker(readFileSync(filePath, "utf8")));
          return;
        }
        res.writeHead(200, corsHeaders({ "Content-Type": contentType(filePath) }));
        res.end(readFileSync(filePath));
      } catch {
        res.writeHead(404, corsHeaders());
        res.end("not found");
      }
      return;
    }

    if (url.pathname.startsWith("/apache-arrow/")) {
      const fileName = url.pathname.slice("/apache-arrow/".length);
      const filePath = resolve(arrowDist, fileName);
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
      const fileName = url.pathname.slice("/tslib/".length);
      const filePath = resolve(tslibDist, fileName);
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
      const fileName = url.pathname.slice("/flatbuffers/".length);
      const filePath = resolve(flatbuffersDist, fileName);
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
  return { server, extensionRequests, xmlaRequests, url: `http://127.0.0.1:${port}` };
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

async function main() {
  const assetServer = await startBrowserAssetServer();
  let browser;

  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error(`[browser:error] ${error.message}`));
    await page.goto(assetServer.url, { waitUntil: "load" });
    await page.waitForFunction(() => typeof window.runPbiSmoke === "function");

    const smokeResult = await page.evaluate(
      ({ bundleKey, extensionRepository, xmlaUrl }) =>
        window.runPbiSmoke({ bundleKey, extensionRepository, xmlaUrl }),
      {
        bundleKey: bundleKeyForPlatform(),
        extensionRepository: assetServer.url,
        xmlaUrl: `${assetServer.url}/xmla`,
      },
    );

    if (!assetServer.xmlaRequests.some((request) => request.authorization === "Bearer mock-token")) {
      throw new Error("mock server did not observe expected Authorization header");
    }

    console.log(
      `pbi_scanner WASM browser smoke test passed (duckdb bundle=${bundleKeyForPlatform()}, platform=${
        process.env.PBI_WASM_DUCKDB_PLATFORM || "wasm_eh"
      }, rows=${JSON.stringify(smokeResult.rows)}, negative_checks=${smokeResult.negativeChecks})`,
    );
  } finally {
    if (assetServer.extensionRequests.length) {
      console.log(`extension requests: ${assetServer.extensionRequests.join(", ")}`);
    }
    if (assetServer.xmlaRequests.length) {
      console.log(
        `xmla requests: ${assetServer.xmlaRequests
          .map((request) => `${request.method} ${request.url} auth=${request.authorization || "<none>"}`)
          .join(", ")}`,
      );
    } else {
      console.log("xmla requests: <none>");
    }
    if (browser) {
      await browser.close();
    }
    assetServer.server.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
