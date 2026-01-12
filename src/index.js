import { parseSql } from "./parser.js";

const sql =
  "INSERT INTO users (id, name, email, isActive) VALUES (1, 'Jane', 'jane@example.com', 1);";

try {
  const result = parseSql(sql);
  console.log("Parsed output");
  console.log(result);
} catch (err) {
  console.error("Error:", err.message);
}
