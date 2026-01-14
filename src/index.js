import { parseSql } from "./parser.js";
import { Database } from "./database.js";

const db = new Database();

db.createTable("users", ["id", "name", "email", "isActive"], "id");

const sql =
  "INSERT INTO users (id, name, email, isActive) VALUES (1, 'Jane', 'jane@example.com', 1);";

const selectSql = "SELECT * FROM users";

try {
  // insert
  const insertCmd = parseSql(sql);
  const row = {};

  insertCmd.columns.forEach((col, i) => {
    row[col] = insertCmd.values[i];
  });

  db.getTable(insertCmd.table).insert(row);

  // select
  const selectedcmd = parseSql(selectSql);
  const rows = db.getTable(selectedcmd.table).selectAll();

  console.log("SELECT results");
  console.log(rows);
} catch (err) {
  console.error("Error:", err.message);
}

// INSERT INTO CMD
// try {
//   const cmd = parseSql(sql);

//   const row = {};
//   // map through columns
//   cmd.columns.forEach((col, i) => {
//     row[col] = cmd.values[i];
//   });

//   const table = db.getTable(cmd.table);
//   table.insert(row);
//   console.log("Table row after insert");
//   console.log(table.selectAll());
// } catch (error) {}

// SQL parser
// try {
//   const result = parseSql(sql);
//   console.log("Parsed output");
//   console.log(result);
// } catch (err) {
//   console.error("Error:", err.message);
// }
