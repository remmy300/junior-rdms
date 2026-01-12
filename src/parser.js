export function parseSql(sql) {
  sql = sql.trim(sql);
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
