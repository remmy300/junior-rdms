export function parseSql(sql) {
  sql = sql.trim().replace(/;$/, "");
  if (/^CREATE TABLE/i.test(sql)) {
    return parseCreateTable(sql);
  } else if (/^INSERT\s+INTO/i.test(sql)) {
    return parseInsert(sql);
  } else if (/^SELECT/i.test(sql)) {
    return parseSelect(sql);
  } else if (/^UPDATE/i.test(sql)) {
    return ParseUpdate(sql);
  } else if (/^DELETE/i.test(sql)) {
    return parseDelete(sql);
  } else {
    throw new Error("Unsupported SQL statement");
  }
}

function parseInsert(sql) {
  const match = sql.match(
    /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i,
  );

  if (!match) throw new Error("Invalid INSERT syntax");

  const [, table, columnsStr, valuesStr] = match;

  const columns = columnsStr.split(",").map((c) => c.trim());

  const values = valuesStr.split(",").map((v) => {
    v = v.trim();

    if (v.startsWith("'") && v.endsWith("'")) {
      return v.slice(1, -1);
    }

    if (!isNaN(Number(v))) {
      return Number(v);
    }

    return v;
  });

  return { command: "INSERT", table, columns, values };
}

function parseWhere(whereRaw) {
  if (!whereRaw) return null;

  if (/ AND /i.test(whereRaw)) {
    const parts = whereRaw.split(/ AND /i);
    return {
      operator: "AND",
      conditions: parts.map(parseCondition),
    };
  }

  if (/ OR /i.test(whereRaw)) {
    const parts = whereRaw.split(/ OR /i);
    return {
      operator: "OR",
      conditions: parts.map(parseCondition),
    };
  }

  return {
    operator: "AND",
    conditions: [parseCondition(whereRaw)],
  };
}

function parseCondition(cond) {
  const match = cond.match(/(\w+)\s*(=|>|<|>=|<=)\s*(.+)/);
  if (!match) throw new Error("Invalid WHERE condition");

  let [, column, operator, value] = match;
  value = value.trim();

  if (value.startsWith("'") && value.endsWith("'")) {
    value = value.slice(1, -1);
  } else if (!isNaN(Number(value))) {
    value = Number(value);
  } else if (value.toLowerCase() === "true") {
    value = true;
  } else if (value.toLowerCase() === "false") {
    value = false;
  }
  return { column, operator, value };
}

function parseSelect(sql) {
  const match = sql.match(
    /SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+))?(?:\s+OFFSET\s+(\d+))?$/i,
  );

  if (!match) throw new Error("Invalid SELECT syntax");

  const [
    ,
    columnsPart,
    table,
    whereRaw,
    orderColumn,
    orderDirection,
    limitRaw,
    offsetRaw,
  ] = match;

  const columns =
    columnsPart.trim() === "*"
      ? "*"
      : columnsPart.trim().toUpperCase() === "COUNT(*)"
        ? ["COUNT(*)"]
        : columnsPart.split(",").map((c) => c.trim());

  const where = whereRaw ? parseWhere(whereRaw) : null;
  const orderBy = orderColumn
    ? { column: orderColumn, direction: orderDirection || "ASC" }
    : null;
  const limit = limitRaw ? Number(limitRaw) : null;
  const offset = offsetRaw ? Number(offsetRaw) : null;

  return {
    command: "SELECT",
    table,
    columns,
    where,
    orderBy,
    limit,
    offset,
  };
}

function ParseUpdate(sql) {
  const match = sql.match(
    /UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(\w+)\s*=\s*(.+))?$/i,
  );
  if (!match) throw new Error("Invalid UPDATE syntax");
  const [, table, setPart, whereColumn, whereValueRaw] = match;

  // parse SET columns
  const sets = {};
  setPart.split(",").forEach((pair) => {
    const [col, val] = pair.split("=").map((s) => s.trim());

    let value = val;

    if (typeof value === "string") {
      if (value.startsWith("'") && value.endsWith("'")) {
        value = value.slice(1, -1);
      } else if (!isNaN(Number(value))) {
        value = Number(value);
      }
    }

    sets[col] = value;
  });

  let where = null;

  if (whereColumn) {
    let value = whereValueRaw.trim();
    if (typeof value === "string") {
      if (value.startsWith("'") && value.endsWith("'")) {
        value = value.slice(1, -1);
      } else if (!isNaN(Number(value))) {
        value = Number(value);
      }
    }
    where = { column: whereColumn, value };
  }
  return {
    command: "UPDATE",
    table,
    sets,
    where,
  };
}

function parseDelete(sql) {
  const match = sql.match(
    /DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(\w+)\s*=\s*(.+))?/i,
  );

  if (!match) throw new Error("Invalid DELETE syntax");

  const [, table, whereColumn, whereValueRaw] = match;

  let where = null;

  if (whereColumn) {
    let value = whereValueRaw.trim();

    if (value.startsWith("'") && value.endsWith("'")) {
      value = value.slice(1, -1);
    } else if (!isNaN(Number(value))) {
      value = Number(value);
    }

    where = { column: whereColumn, value };
  }

  return {
    command: "DELETE",
    table,
    where,
  };
}
