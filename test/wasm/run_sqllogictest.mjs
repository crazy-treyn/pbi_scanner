#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  bundleKeyForPlatform,
  getExtensionPath,
  getRepoRoot,
  startBrowserAssetServer,
  withBrowserPage,
} from "./browser_harness.mjs";
import { parseSqllogictest } from "./sqllogictest_parser.mjs";
import { sqllogictestBrowserModuleScript } from "./sqllogictest_browser.mjs";

function parseArgs(argv) {
  const options = {
    tests: [],
    verbose: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--test") {
      options.tests.push(resolve(argv[++i]));
      continue;
    }
    if (arg === "--verbose") {
      options.verbose = true;
      continue;
    }
    throw new Error(`unknown argument: ${arg}`);
  }
  if (!options.tests.length) {
    options.tests.push(resolve(getRepoRoot(), "wasm_sql/pbi_scanner_wasm.test"));
  }
  return options;
}

function describeRecord(record) {
  if (record.type === "require") {
    return `require ${record.extension}`;
  }
  if (record.type === "query") {
    return `query ${record.types}`;
  }
  return `statement ${record.mode}`;
}

function summarizeFailures(fileName, results) {
  const failures = results.filter((result) => result.status === "fail");
  if (!failures.length) {
    return;
  }
  console.error(`\n${fileName} failures:`);
  for (const failure of failures) {
    console.error(`  [${failure.index + 1}] ${describeRecord(failure.record)}`);
    console.error(`      ${failure.message}`);
  }
}

async function runTestFile(testPath, options) {
  const content = readFileSync(testPath, "utf8");
  const parsed = parseSqllogictest(content, testPath);
  const platform = process.env.PBI_WASM_DUCKDB_PLATFORM || "wasm_eh";
  const skipStatementErrors = platform === "wasm_mvp";

  const assetServer = await startBrowserAssetServer({
    pageTitle: "pbi_scanner wasm sqllogictest",
    pageModuleScript: sqllogictestBrowserModuleScript(),
  });

  try {
    const results = await withBrowserPage(assetServer, async (page) => {
      await page.waitForFunction(() => typeof window.runSqllogictestFile === "function");
      return page.evaluate(
        ({ bundleKey, extensionRepository, records, skipStatementErrors }) =>
          window.runSqllogictestFile({
            bundleKey,
            extensionRepository,
            records,
            skipStatementErrors,
          }),
        {
          bundleKey: bundleKeyForPlatform(),
          extensionRepository: assetServer.url,
          records: parsed.records,
          skipStatementErrors,
        },
      );
    });

    const passed = results.filter((result) => result.status === "pass").length;
    const failed = results.filter((result) => result.status === "fail").length;
    const skipped = results.filter((result) => result.status === "skip").length;

    if (options.verbose) {
      for (const result of results) {
        console.log(
          `[${result.status}] ${describeRecord(result.record)}${
            result.message ? `: ${result.message}` : ""
          }`,
        );
      }
    }

    if (failed > 0) {
      summarizeFailures(testPath, results);
      throw new Error(`${testPath}: ${failed} failed, ${passed} passed, ${skipped} skipped`);
    }

    console.log(
      `pbi_scanner WASM sqllogictest passed (${testPath}, platform=${platform}, records=${passed} passed, ${skipped} skipped)`,
    );
    return { passed, failed, skipped };
  } finally {
    assetServer.server.close();
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (!getExtensionPath()) {
    throw new Error("extension path is required");
  }

  let totalPassed = 0;
  let totalSkipped = 0;
  for (const testPath of options.tests) {
    const summary = await runTestFile(testPath, options);
    totalPassed += summary.passed;
    totalSkipped += summary.skipped;
  }

  const platform = process.env.PBI_WASM_DUCKDB_PLATFORM || "wasm_eh";
  console.log(
    `pbi_scanner WASM sqllogictest suite passed (platform=${platform}, records=${totalPassed} passed, ${totalSkipped} skipped)`,
  );
}

main().catch((error) => {
  console.error(error.message || error);
  process.exitCode = 1;
});
