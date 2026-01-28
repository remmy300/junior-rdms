export class Table {
  constructor(name, columns, primaryKey) {
    this.name = name;
    this.columns = columns;
    this.primaryKey = primaryKey;
    this.rows = [];
  }

  castValue(type, value) {
    if (value === null || value === undefined) {
      return null;
    }
    switch (type) {
      case "INT":
        return value === null ? null : Number(value);

      case "BOOLEAN":
        return value === "1" || value === 1 || value === true;

      case "TEXT":
        return value === null ? null : String(value);

      default:
        return value;
    }
  }

  getColumnType(columnName) {
    const col = this.columns.find((c) => c.name === columnName);
    return col ? col.type : null;
  }

  //INSERT INTO
  insert(rowObj) {
    const newRow = {};

    this.columns.forEach((col) => {
      if (!(col.name in rowObj)) {
        throw new Error(`Missing column ${col.name}`);
      }

      newRow[col.name] = this.castValue(col.type, rowObj[col.name]);
    });

    if (this.primaryKey) {
      const pk = this.primaryKey;
      if (this.rows.some((r) => r[pk] === newRow[pk])) {
        throw new Error(`Primary key ${pk} = ${newRow[pk]} already exists`);
      }
    }

    this.rows.push(newRow);
  }

  //SELECT
  select(
    columns = ["*"],
    where = null,
    groupByColumns = null,
    having = null,
    orderBy = null,
    limit = null,
    offset = null,
    distinct = false,
    joins = [],
  ) {
    let result = [...this.rows];

    //JOIN
    function joinTables(
      leftRows,
      rightTable,
      leftKey,
      rightKey,
      type,
      leftAlias,
      rightAlias,
    ) {
      const joined = [];
      const rightMatchedFlags = new Array(rightTable.rows.length).fill(false);

      for (const lRow of leftRows) {
        const lVal = lRow[`${leftAlias}.${leftKey}`] ?? lRow[leftKey];
        let matched = false;

        rightTable.rows.forEach((rRow, idx) => {
          const rVal = rRow[rightKey];
          if (lVal == null || rVal == null) return;

          if (lVal === rVal) {
            matched = true;
            rightMatchedFlags[idx] = true;

            // Prefix right row columns with alias
            const rPrefixed = {};
            for (const col of Object.keys(rRow)) {
              rPrefixed[`${rightAlias}.${col}`] = rRow[col];
            }

            joined.push({ ...lRow, ...rPrefixed });
          }
        });

        if ((type === "LEFT" || type === "FULL") && !matched) {
          const nullRight = {};
          for (const col of Object.keys(rightTable.rows[0] || {})) {
            nullRight[`${rightAlias}.${col}`] = null;
          }
          joined.push({ ...lRow, ...nullRight });
        }
      }

      if (type === "RIGHT" || type === "FULL") {
        rightTable.rows.forEach((rRow, idx) => {
          if (!rightMatchedFlags[idx]) {
            const nullLeft = {};
            for (const col of Object.keys(leftRows[0] || {})) {
              nullLeft[col] = null;
            }

            const rPrefixed = {};
            for (const col of Object.keys(rRow)) {
              rPrefixed[`${rightAlias}.${col}`] = rRow[col];
            }

            joined.push({ ...nullLeft, ...rPrefixed });
          }
        });
      }

      return joined;
    }

    //All JOINS(INNER,LEFT,RIGHT,FULL)
    for (const join of joins) {
      const leftAlias = join.leftAlias || join.leftTableName;
      const rightAlias = join.rightAlias || join.table.name;

      result = joinTables(
        result,
        join.table,
        join.on.left,
        join.on.right,
        join.type,
        leftAlias,
        rightAlias,
      );
    }

    function nonNullValues(rows, column) {
      return rows
        .map((r) => r[column])
        .filter((v) => v !== null && v !== undefined);
    }

    function countNonNull(rows, column) {
      return nonNullValues(rows, column).length;
    }

    if (!Array.isArray(columns)) columns = [columns];

    columns = columns.map((c) => {
      if (c.type) return c;

      const raw = c.column ?? c;

      // Aggregate
      const aggMatch = raw.match(/^(COUNT|SUM|AVG|MIN|MAX)\((\*|\w+)\)$/i);
      if (aggMatch) {
        return {
          type: "AGGREGATE",
          func: aggMatch[1].toUpperCase(),
          column: aggMatch[2],
          alias: c.alias || raw,
        };
      }

      if (raw === "*") {
        return { type: "STAR" };
      }

      // Regular column
      return {
        type: "COLUMN",
        column: raw,
        alias: c.alias || raw,
      };
    });

    columns = columns.map((c) => {
      if (typeof c === "string") {
        const match = c
          .trim()
          .match(/^(\w+\(\*\)|\w+\(\w+\)|\*|\w+)\s*(?:AS\s+(\w+))?$/i);
        if (!match) throw new Error(`Invalid column: ${c}`);
        return { column: match[1], alias: match[2] || match[1] };
      }
      return c;
    });

    //  WHERE

    if (where) {
      const evalCond = (row, cond) => {
        const type = this.getColumnType(cond.column);
        const rowValue = row[cond.column];
        const condValue = type ? this.castValue(type, cond.value) : cond.value;

        switch (cond.operator) {
          case "=":
            return rowValue === condValue;
          case ">":
            return rowValue > condValue;
          case "<":
            return rowValue < condValue;
          case ">=":
            return rowValue >= condValue;
          case "<=":
            return rowValue <= condValue;
          default:
            return false;
        }
      };

      result = result.filter((row) => {
        if (where.operator === "AND")
          return where.conditions.every((c) => evalCond(row, c));
        if (where.operator === "OR")
          return where.conditions.some((c) => evalCond(row, c));
        return evalCond(row, where.conditions[0]);
      });
    }

    if (columns.length === 1 && columns[0].type === "STAR") {
      return result;
    }

    // Aggregates without GROUP BY

    if (!groupByColumns && columns.some((c) => c.type === "AGGREGATE")) {
      const aggRow = {};

      columns.forEach((col) => {
        if (col.type !== "AGGREGATE") return;

        const values =
          col.column === "*" ? result : nonNullValues(result, col.column);

        switch (col.func) {
          case "COUNT":
            aggRow[col.alias] =
              col.column === "*" ? result.length : values.length;
            break;
          case "SUM": {
            aggRow[col.alias] = values.reduce((sum, v) => sum + Number(v), 0);
            break;
          }
          case "AVG": {
            aggRow[col.alias] =
              values.length === 0
                ? null
                : values.reduce((sum, v) => sum + Number(v), 0) / values.length;
            break;
          }
          case "MIN": {
            outRow[col.alias] =
              values.length === 0 ? null : Math.min(...values);
            break;
          }
          case "MAX": {
            outRow[col.alias] =
              values.length === 0 ? null : Math.max(...values);
            break;
          }
        }
      });

      return [aggRow];
    }

    //  GROUP BY & Aggregates

    if (groupByColumns && groupByColumns.length > 0) {
      const groups = {};

      for (const row of result) {
        const key = JSON.stringify(groupByColumns.map((col) => row[col]));
        if (!groups[key]) groups[key] = [];
        groups[key].push(row);
      }

      const aggregated = [];

      for (const groupRows of Object.values(groups)) {
        const outRow = {};
        const aggContext = {};

        const rows = groupRows;

        // Group columns
        groupByColumns.forEach((col) => {
          outRow[col] = groupRows[0][col];
        });

        // Aggregate columns
        columns.forEach((col) => {
          if (col.type !== "AGGREGATE") return;
          const values =
            col.column === "*" ? null : nonNullValues(rows, col.column);

          switch (col.func) {
            case "COUNT": {
              const key =
                col.column === "*" ? "COUNT(*)" : `COUNT(${col.column})`;

              const value = col.column === "*" ? rows.length : values.length;

              aggContext[key] = value;
              outRow[col.alias] = value;
              break;
            }

            case "SUM": {
              outRow[col.alias] = values.reduce((sum, v) => sum + Number(v), 0);
              break;
            }
            case "AVG": {
              outRow[col.alias] =
                values.length === 0
                  ? null
                  : values.reduce((sum, v) => sum + Number(v), 0) /
                    values.length;
              break;
            }
            case "MIN": {
              outRow[col.alias] =
                values.length === 0 ? null : Math.min(...values);

              break;
            }
            case "MAX": {
              outRow[col.alias] =
                values.length === 0 ? null : Math.max(...values);

              break;
            }
          }
        });

        aggregated.push({ row: outRow, __aggs: aggContext });
      }

      result = aggregated;
    }

    if (having && !columns.some((c) => c.type === "AGGREGATE")) {
      throw new Error("HAVING requires an aggregate query");
    }

    if (having) {
      const evalCond = (entry, cond) => {
        const { row, __aggs } = entry;

        // Aggregate expression: COUNT(col)
        const aggMatch = cond.column.match(
          /^(COUNT|SUM|AVG|MIN|MAX)\((\*|\w+)\)$/i,
        );

        if (aggMatch) {
          const key = `${aggMatch[1].toUpperCase()}(${aggMatch[2]})`;
          return compare(__aggs[key], cond.operator, cond.value);
        }

        // Alias
        if (cond.column in row) {
          return compare(row[cond.column], cond.operator, cond.value);
        }

        return false;
      };

      result = result.filter((entry) => {
        if (having.operator === "AND")
          return having.conditions.every((c) => evalCond(entry, c));
        if (having.operator === "OR")
          return having.conditions.some((c) => evalCond(entry, c));
        return evalCond(entry, having.conditions[0]);
      });

      result = result.map((e) => e.row);
    }

    // Helper
    function compare(left, operator, right) {
      switch (operator) {
        case "=":
          return left === right;
        case ">":
          return left > right;
        case "<":
          return left < right;
        case ">=":
          return left >= right;
        case "<=":
          return left <= right;
        default:
          return false;
      }
    }

    // DISTINCT
    if (distinct) {
      const seen = new Set();
      result = result.filter((row) => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    //  ORDER BY

    if (orderBy) {
      const { column, direction } = orderBy;
      result.sort((a, b) => {
        if (a[column] < b[column]) return direction === "ASC" ? -1 : 1;
        if (a[column] > b[column]) return direction === "ASC" ? 1 : -1;
        return 0;
      });
    }

    // OFFSET & LIMIT

    if (offset !== null) result = result.slice(offset);
    if (limit !== null) result = result.slice(0, limit);

    // COLUMN PROJECTION for non-aggregate

    if (
      !groupByColumns &&
      !columns.some((c) => /COUNT|SUM|AVG|MIN|MAX/.test(c.column))
    ) {
      if (!columns.some((c) => c.column === "*")) {
        result = result.map((row) => {
          const obj = {};
          columns.forEach((c) => (obj[c.alias] = row[c.column]));
          return obj;
        });
      }
    }

    if (columns.some((c) => c.column === "*")) return result;

    return result;
  }
}
