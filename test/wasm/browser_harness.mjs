import { createServer } from "node:http";
import { dirname, extname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { chromium } from "playwright";

const require = createRequire(import.meta.url);
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

export const xmlaResponse = `<?xml version="1.0" encoding="utf-8"?>
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

export function getRepoRoot() {
  return repoRoot;
}

export function getExtensionPath() {
  return resolve(
    repoRoot,
    process.env.PBI_WASM_EXTENSION_PATH ||
      "build/wasm_eh/extension/pbi_scanner/pbi_scanner.duckdb_extension.wasm",
  );
}

export function bundleKeyForPlatform() {
  const platform = process.env.PBI_WASM_DUCKDB_PLATFORM;
  if (platform === "wasm_mvp") {
    return "mvp";
  }
  if (platform === "wasm_eh" || !platform) {
    return "eh";
  }
  throw new Error(`unsupported PBI_WASM_DUCKDB_PLATFORM: ${platform}`);
}

export function corsHeaders(extra = {}) {
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

export function patchDuckDbMvpWorker(source) {
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

export async function startBrowserAssetServer({ pageTitle, pageModuleScript }) {
  const extensionPath = getExtensionPath();
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
    <title>${pageTitle}</title>
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
${pageModuleScript}
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

export async function withBrowserPage(assetServer, fn) {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error(`[browser:error] ${error.message}`));
    await page.goto(assetServer.url, { waitUntil: "load" });
    return await fn(page);
  } finally {
    await browser.close();
  }
}
