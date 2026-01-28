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
    if (!name) throw new Error("Table name required");

    // Try exact match first
    let table = this.tables[name];

    // Fallback to case-insensitive lookup
    if (!table) {
      const key = Object.keys(this.tables).find(
        (k) => k.toLowerCase() === String(name).toLowerCase(),
      );
      if (key) table = this.tables[key];
    }

    if (!table) {
      throw new Error(`Table ${name} does not exist`);
    }

    return table;
  }
}
