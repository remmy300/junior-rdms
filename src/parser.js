export function parseSql(sql) {
  sql = sql.trim().replace(/;$/, "");
  if (sql.startsWith("CREATE TABLE")) {
    return parseCreateTable(sql);
  } else if (sql.startsWith("INSERT INTO")) {
    return parseInsert(sql);
  } else if (sql.startsWith("SELECT")) {
    return parseSelect(sql);
  } else if (sql.startsWith("UPDATE")) {
    return ParseUpdate(sql);
  } else if (sql.startsWith("DELETE")) {
    return parseDelete(sql);
  } else {
    throw new Error("Unsupported SQL statement");
  }
}

function parseInsert(sql) {
  const match = sql.match(/INSERT INTO (\w+) \((.+)\) VALUES \((.+)\)/i);
  if (!match) throw new Error("Invalid INSERT syntax");

  const [, table, columnsStr, valuesStr] = match;
  const columns = columnsStr.split(",").map((c) => c.trim());
  const values = valuesStr.split(",").map((v) => {
    v = v.trim();
    if (v === "1" || v === "0") return Boolean(Number(v));
    if (v.startsWith("'") && v.endsWith("'")) return v.slice(1, -1);
    return Number(v);
  });

  return {
    command: "INSERT",
    table,
    columns,
    values,
  };
}

function parseSelect(sql) {
  const match = sql.match(
    /SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(\w+)\s*=\s*(.+))?/i
  );

  if (!match) throw new Error("Invalid SELECT syntax");

  const [, columnsPart, table, whereColumn, whereValueRaw] = match;

  const columns =
    columnsPart.trim() === "*"
      ? "*"
      : columnsPart.split(",").map((c) => c.trim());

  let where = null;

  if (whereColumn) {
    let value = whereValueRaw.trim();

    if (value === "1" || value === "0") {
      value = Boolean(Number(value));
    } else if (value.startsWith("'") && value.endsWith("'")) {
      value = value.slice(1, -1);
    } else if (!NaN(Number(value))) {
      value = Number(value);
    }

    where = {
      column: whereColumn,
      value,
    };
  }

  return {
    command: "SELECT",
    table,
    columns,
    where,
  };
}
