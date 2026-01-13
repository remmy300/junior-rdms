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
}
