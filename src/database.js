import { Table } from "./table.js";

export class Database {
  //DB class to hold tables and allow access
  constructor() {
    this.tables = {};
  }

  createTable(name, columns, primaryKey) {
    if (this.tables[name]) {
      throw new Error(`Table ${name} already exists`);
    }
    this.tables[name] = new Table(name, columns, primaryKey);
  }

  getTable(name) {
    const table = this.tables[name];
    if (!table) {
      throw new Error(`Table ${name} does not exist`);
    }
    return table;
  }
}
