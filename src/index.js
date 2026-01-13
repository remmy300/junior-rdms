import { parseSql } from "./parser.js";
import { Database } from "./database.js";

const db = new Database();

db.createTable("users", ["id", "name", "email", "isActive"], "id");

const sql =
  "INSERT INTO users (id, name, email, isActive) VALUES (1, 'Jane', 'jane@example.com', 1);";

try {
  const cmd = parseSql(sql);

  const row = {};
  // map through columns
  cmd.columns.forEach((col, i) => {
    row[col] = cmd.values[i];
  });

  const table = db.getTable(cmd.table);
  table.insert(row);
  console.log("Table row after insert");
  console.log(table.selectAll());
} catch (error) {}

// try {
//   const result = parseSql(sql);
//   console.log("Parsed output");
//   console.log(result);
// } catch (err) {
//   console.error("Error:", err.message);
// }
