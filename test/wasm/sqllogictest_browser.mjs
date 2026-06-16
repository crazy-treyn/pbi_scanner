export function sqllogictestBrowserModuleScript() {
  return `
      import * as duckdb from "/duckdb/duckdb-browser.mjs";

      function formatCell(value) {
        if (value === null || value === undefined) {
          return "NULL";
        }
        if (typeof value === "bigint") {
          return value.toString();
        }
        if (typeof value === "boolean") {
          return value ? "true" : "false";
        }
        if (value instanceof Date) {
          return value.toISOString();
        }
        return String(value);
      }

      function formatRows(result) {
        const rows = result.toArray();
        return rows.map((row) => Object.values(row).map(formatCell).join("\\t"));
      }

      function normalizeError(message) {
        return String(message || "");
      }

      async function createSession(config) {
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
        return { db, conn, worker };
      }

      async function requireExtension(conn, extensionRepository, extensionName) {
        if (extensionName !== "pbi_scanner") {
          throw new Error("unsupported require extension: " + extensionName);
        }
        await conn.query("SET custom_extension_repository='" + extensionRepository + "'");
        await conn.query("INSTALL pbi_scanner");
        await conn.query("LOAD pbi_scanner");
      }

      async function runRecord(session, record, config) {
        if (record.type === "require") {
          await requireExtension(session.conn, config.extensionRepository, record.extension);
          return { status: "pass" };
        }

        if (record.type === "query") {
          const result = await session.conn.query(record.sql);
          const actual = formatRows(result);
          if (actual.length !== record.expected.length) {
            return {
              status: "fail",
              message:
                "row count mismatch: expected " +
                record.expected.length +
                " got " +
                actual.length +
                " (" +
                JSON.stringify(actual) +
                ")",
            };
          }
          for (let i = 0; i < record.expected.length; i++) {
            if (actual[i] !== record.expected[i]) {
              return {
                status: "fail",
                message:
                  "row " +
                  (i + 1) +
                  " mismatch: expected " +
                  JSON.stringify(record.expected[i]) +
                  " got " +
                  JSON.stringify(actual[i]),
              };
            }
          }
          return { status: "pass" };
        }

        if (record.type === "statement") {
          if (record.mode === "ok") {
            await session.conn.query(record.sql);
            return { status: "pass" };
          }
          if (record.mode === "error") {
            if (config.skipStatementErrors) {
              return { status: "skip", message: "statement error checks skipped for wasm_mvp" };
            }
            try {
              await session.conn.query(record.sql);
              return { status: "fail", message: "expected statement error but query succeeded" };
            } catch (error) {
              const message = normalizeError(error && error.message ? error.message : error);
              const expected = (record.expectedError || []).join("\\n");
              if (!message.includes(expected)) {
                return {
                  status: "fail",
                  message:
                    "unexpected error message: expected substring " +
                    JSON.stringify(expected) +
                    " in " +
                    JSON.stringify(message),
                };
              }
              return { status: "pass" };
            }
          }
          return { status: "skip", message: "unsupported statement mode " + record.mode };
        }

        return { status: "skip", message: "unsupported record type " + record.type };
      }

      window.runSqllogictestFile = async (config) => {
        const session = await createSession(config);
        const results = [];
        try {
          for (let i = 0; i < config.records.length; i++) {
            const record = config.records[i];
            try {
              const outcome = await runRecord(session, record, config);
              results.push({ index: i, record, ...outcome });
            } catch (error) {
              results.push({
                index: i,
                record,
                status: "fail",
                message: normalizeError(error && error.message ? error.message : error),
              });
            }
          }
        } finally {
          await session.conn.close();
          await session.db.terminate();
          session.worker.terminate();
        }
        return results;
      };
`;
}
