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

    // Handle SQL NULL / undefined
    if (v.toLowerCase() === "null" || v.toLowerCase() === "undefined") {
      return null;
    }

    throw new Error(`Invalid value: ${v}`);
  });

  return { command: "INSERT", table, columns, values };
}

export function parseSelect(sql) {
  sql = sql.trim().replace(/;$/, "");

  // Check for DISTINCT
  let distinct = false;
  if (/^SELECT\s+DISTINCT/i.test(sql)) {
    distinct = true;
    sql = sql.replace(/^SELECT\s+DISTINCT\s+/i, "SELECT ");
  }

  // parse SELECT ...FROM...
  const selectMatch = sql.match(/^SELECT\s+(.+?)\s+FROM\s+(\w+)/i);
  if (!selectMatch) throw new Error("Invalid SELECT syntax");

  const columnsPart = selectMatch[1];
  const table = selectMatch[2];

  let where = null;
  let groupByColumns = null;
  let having = null;
  let orderBy = null;
  let limit = null;
  let offset = null;

  // Extract WHERE
  const whereMatch = sql.match(
    /\sWHERE\s+(.+?)(?=\sGROUP BY|\sORDER BY|\sLIMIT|\sOFFSET|$)/i,
  );
  if (whereMatch) {
    where = parseWhere(whereMatch[1]);
  }

  // Extract GROUP BY
  const groupByMatch = sql.match(/\sGROUP BY\s+([\w\s,]+)/i);
  if (groupByMatch) {
    groupByColumns = groupByMatch[1].split(",").map((c) => c.trim());
  }

  // Extract HAVING
  const havingMatch = sql.match(
    /\sHAVING\s+(.+?)(?=\sORDER BY|\sLIMIT|\sOFFSET|$)/i,
  );
  if (havingMatch) {
    having = parseWhere(havingMatch[1]); // reuse parseWhere for conditions
  }

  // Extract ORDER BY
  const orderByMatch = sql.match(/\sORDER BY\s+(\w+)(?:\s+(ASC|DESC))?/i);
  if (orderByMatch) {
    orderBy = { column: orderByMatch[1], direction: orderByMatch[2] || "ASC" };
  }

  // Extract LIMIT
  const limitMatch = sql.match(/\sLIMIT\s+(\d+)/i);
  if (limitMatch) {
    limit = Number(limitMatch[1]);
  }

  // Extract OFFSET
  const offsetMatch = sql.match(/\sOFFSET\s+(\d+)/i);
  if (offsetMatch) {
    offset = Number(offsetMatch[1]);
  }

  // Parse columns and handle aliases
  const columns = columnsPart.split(",").map((c) => {
    const trimmed = c.trim();

    // Check for COUNT(*) with optional alias
    let match = trimmed.match(/^COUNT\(\*\)\s*(?:AS\s+(\w+))?$/i);
    if (match) {
      return { column: "COUNT(*)", alias: match[1] || "COUNT(*)" };
    }

    // Check for other aggregates like SUM(id), AVG(id), with optional alias
    match = trimmed.match(/^(\w+\(\w+\))\s*(?:AS\s+(\w+))?$/i);
    if (match) {
      return { column: match[1], alias: match[2] || match[1] };
    }

    // Check for *
    if (trimmed === "*") return { column: "*", alias: "*" };

    // Regular column with optional alias
    match = trimmed.match(/^(\w+)\s*(?:AS\s+(\w+))?$/i);
    if (match) return { column: match[1], alias: match[2] || match[1] };

    throw new Error(`Invalid column syntax: ${trimmed}`);
  });

  return {
    command: "SELECT",
    table,
    columns,
    where,
    groupByColumns,
    having,
    orderBy,
    limit,
    offset,
    distinct,
  };
}

function parseWhere(whereRaw) {
  if (!whereRaw) return null;

  // Split by AND / OR
  let operator = "AND";
  let conditionsRaw = [whereRaw];

  if (/ AND /i.test(whereRaw)) {
    operator = "AND";
    conditionsRaw = whereRaw.split(/ AND /i);
  } else if (/ OR /i.test(whereRaw)) {
    operator = "OR";
    conditionsRaw = whereRaw.split(/ OR /i);
  }

  const conditions = conditionsRaw.map(parseCondition);
  return { operator, conditions };
}

function parseCondition(cond) {
  const match = cond.match(/([\w\(\)\*]+)\s*(=|>|<|>=|<=)\s*(.+)/i);
  if (!match) throw new Error("Invalid WHERE/HAVING condition");

  let [, column, operator, value] = match;
  value = value.trim();
  if (value.startsWith("'") && value.endsWith("'")) value = value.slice(1, -1);
  else if (!isNaN(Number(value))) value = Number(value);

  return { column, operator, value };
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
