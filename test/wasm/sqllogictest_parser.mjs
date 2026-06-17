const DIRECTIVE_RE = /^(query|statement|require)\b/;

function isDirectiveLine(line) {
  const trimmed = line.trim();
  return trimmed.startsWith("#") || DIRECTIVE_RE.test(trimmed);
}

function readUntilSeparator(lines, startIndex) {
  const blockLines = [];
  let index = startIndex;
  while (index < lines.length && lines[index].trim() !== "----") {
    blockLines.push(lines[index]);
    index++;
  }
  if (index < lines.length && lines[index].trim() === "----") {
    return { text: blockLines.join("\n").trim(), nextIndex: index + 1, hadSeparator: true };
  }
  return { text: blockLines.join("\n").trim(), nextIndex: index, hadSeparator: false };
}

function readSqlUntilDirective(lines, startIndex) {
  const sqlLines = [];
  let index = startIndex;
  while (index < lines.length) {
    const line = lines[index];
    if (!line.trim()) {
      return { sql: sqlLines.join("\n").trim(), nextIndex: index + 1 };
    }
    if (isDirectiveLine(line)) {
      return { sql: sqlLines.join("\n").trim(), nextIndex: index };
    }
    sqlLines.push(line);
    index++;
  }
  return { sql: sqlLines.join("\n").trim(), nextIndex: index };
}

function readExpectedBlock(lines, startIndex) {
  const expected = [];
  let index = startIndex;
  while (index < lines.length) {
    const line = lines[index];
    if (!line.trim()) {
      return { expected, nextIndex: index + 1 };
    }
    if (isDirectiveLine(line)) {
      return { expected, nextIndex: index };
    }
    expected.push(line);
    index++;
  }
  return { expected, nextIndex: index };
}

export function parseSqllogictest(content, fileName = "unknown.test") {
  const lines = content.replace(/\r\n/g, "\n").split("\n");
  const records = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index].trim();
    index++;
    if (!line || line.startsWith("#")) {
      continue;
    }

    if (line.startsWith("require ")) {
      records.push({
        type: "require",
        extension: line.slice("require ".length).trim(),
        line: index - 1,
      });
      continue;
    }

    if (line.startsWith("query ")) {
      const remainder = line.slice("query ".length).trim();
      const types = remainder.split(/\s+/)[0];
      const { text: sql, nextIndex: sqlEnd, hadSeparator } = readUntilSeparator(lines, index);
      if (!hadSeparator) {
        throw new Error(`${fileName}:${index}: expected ---- separator after query`);
      }
      index = sqlEnd;
      const { expected, nextIndex: expectedEnd } = readExpectedBlock(lines, index);
      index = expectedEnd;
      records.push({
        type: "query",
        types,
        sql,
        expected,
        line: index,
      });
      continue;
    }

    if (line.startsWith("statement ")) {
      const mode = line.slice("statement ".length).trim();
      if (!["ok", "error", "maybe"].includes(mode)) {
        throw new Error(`${fileName}:${index - 1}: unsupported statement mode "${mode}"`);
      }
      let sql;
      let expectedError = [];
      if (mode === "ok") {
        const parsedSql = readSqlUntilDirective(lines, index);
        sql = parsedSql.sql;
        index = parsedSql.nextIndex;
      } else {
        const { text, nextIndex: sqlEnd, hadSeparator } = readUntilSeparator(lines, index);
        if (!hadSeparator) {
          throw new Error(`${fileName}:${index}: expected ---- separator after statement ${mode}`);
        }
        sql = text;
        index = sqlEnd;
        const { expected, nextIndex: expectedEnd } = readExpectedBlock(lines, index);
        index = expectedEnd;
        expectedError = expected;
      }
      records.push({
        type: "statement",
        mode,
        sql,
        expectedError,
        line: index,
      });
      continue;
    }

    throw new Error(`${fileName}:${index - 1}: unsupported sqllogictest directive "${line}"`);
  }

  return { fileName, records };
}
