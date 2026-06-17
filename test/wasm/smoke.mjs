import {
  bundleKeyForPlatform,
  startBrowserAssetServer,
  withBrowserPage,
} from "./browser_harness.mjs";

const smokePageModuleScript = `
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
`;

async function main() {
  const assetServer = await startBrowserAssetServer({
    pageTitle: "pbi_scanner wasm smoke",
    pageModuleScript: smokePageModuleScript,
  });

  try {
    const smokeResult = await withBrowserPage(assetServer, async (page) => {
      await page.waitForFunction(() => typeof window.runPbiSmoke === "function");
      return page.evaluate(
        ({ bundleKey, extensionRepository, xmlaUrl }) =>
          window.runPbiSmoke({ bundleKey, extensionRepository, xmlaUrl }),
        {
          bundleKey: bundleKeyForPlatform(),
          extensionRepository: assetServer.url,
          xmlaUrl: `${assetServer.url}/xmla`,
        },
      );
    });

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
    assetServer.server.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
