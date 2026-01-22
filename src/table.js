export class Table {
  constructor(name, columns, primaryKey) {
    this.name = name;
    this.columns = columns;
    this.primaryKey = primaryKey;
    this.rows = [];
  }

  castValue(type, value) {
    switch (type) {
      case "INT":
        return Number(value);

      case "BOOLEAN":
        return value === "1" || value === 1 || value === true;

      case "TEXT":
        return String(value);

      default:
        return value;
    }
  }

  getColumnType(columnName) {
    const col = this.columns.find((c) => c.name === columnName);
    return col ? col.type : null;
  }

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

  select(
    columns = "*",
    where = null,
    orderBy = null,
    limit = null,
    offset = null,
  ) {
    console.log("DEBUG: USING NEW SELECT()");
    console.log("DEBUG TYPES:", {
      limit,
      limitType: typeof limit,
      offset,
      offsetType: typeof offset,
    });

    let result = [...this.rows];

    // WHERE
    if (where) {
      result = result.filter((row) => {
        // Evaluate a single condition
        const evalCond = (cond) => {
          const colType = this.getColumnType(cond.column);
          const condValue = colType
            ? this.castValue(colType, cond.value)
            : cond.value;
          const rowValue = row[cond.column];

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

        // Combine conditions based on AND / OR
        if (where.operator === "AND") {
          return where.conditions.every(evalCond);
        } else if (where.operator === "OR") {
          return where.conditions.some(evalCond);
        }

        // Fallback: single condition
        return evalCond(where.conditions[0]);
      });
    }

    // ORDER BY
    if (orderBy) {
      const { column, direction } = orderBy;
      result.sort((a, b) => {
        if (a[column] < b[column]) return direction === "ASC" ? -1 : 1;
        if (a[column] > b[column]) return direction === "ASC" ? 1 : -1;
        return 0;
      });
    }

    // OFFSET
    if (offset !== null) {
      offset = Number(offset);
      result = result.slice(offset);
    }

    // LIMIT
    if (limit !== null) {
      limit = Number(limit);
      result = result.slice(0, limit);
    }

    if (
      Array.isArray(columns) &&
      columns.length === 1 &&
      columns[0] === "COUNT(*)"
    ) {
      return [{ "COUNT(*)": result.length }];
    }

    // COLUMN PROJECTION
    if (columns === "*") {
      return result;
    }

    return result.map((row) => {
      const obj = {};
      columns.forEach((col) => {
        obj[col] = row[col];
      });
      return obj;
    });
  }
}
