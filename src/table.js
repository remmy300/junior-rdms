export class Table {
  constructor(name, columns, primaryKey) {
    (this.name = name),
      (this.columns = columns),
      (this.primaryKey = primaryKey),
      (this.rows = []);
  }

  // insert a row and check the primary key uniqueness
  insert(rowObj) {
    if (this.primaryKey) {
      const pkValue = rowObj[this.primaryKey];
      if (this.rows.some((r) => r[this.primaryKey] === pkValue)) {
        throw new Error(
          `Primary key ${this.primaryKey} = ${pkValue} already exists`
        );
      }
    }
    this.rows.push(rowObj);
  }
  selectAll() {
    return this.rows;
  }

  //   SELECT column projection only

  select(columns = "*", where = null) {
    let result = [...this.rows];

    if (where) {
      result = result.filter((row) => row[where.column] === where.value);
    }
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
