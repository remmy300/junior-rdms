import readline from "readline";
import { parseSql } from "./parser.js";
import { Database } from "./database.js";

const db = new Database();

// Runtime SQL input

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: "sql> ",
});

console.log("\nMini SQL Engine started");
rl.prompt();

rl.on("line", (line) => {
  const sql = line.trim();
  if (!sql) {
    rl.prompt();
    return;
  }

  try {
    const cmd = parseSql(sql);
    execute(cmd, db);
  } catch (err) {
    console.error("Error:", err.message);
  }

  rl.prompt();
});

db.createTable(
  "users",
  [
    { name: "id", type: "INT" },
    { name: "name", type: "TEXT" },
    { name: "email", type: "TEXT" },
    { name: "isActive", type: "BOOLEAN" },
  ],
  "id",
);

const initialInsert = parseSql(
  "INSERT INTO users (id, name, email, isActive) VALUES (1, 'Jane', 'jane@example.com', 1)",
);
const row1 = {};
initialInsert.columns.forEach((col, i) => {
  row1[col] = initialInsert.values[i];
});
db.getTable(initialInsert.table).insert(row1);

// SQL Executor function

function execute(cmd, db) {
  switch (cmd.command) {
    case "INSERT": {
      const table = db.getTable(cmd.table);
      const row = {};
      cmd.columns.forEach((col, i) => {
        row[col] = cmd.values[i];
      });
      table.insert(row);
      console.log("Row inserted successfully");
      break;
    }

    case "SELECT": {
      console.log("DEBUG CMD:", cmd);
      const table = db.getTable(cmd.table);
      const rows = table.select(
        cmd.columns,
        cmd.where,
        cmd.orderBy,
        cmd.limit,
        cmd.offset,
      );
      console.log("SELECT result:");
      console.log(rows);
      break;
    }

    case "UPDATE": {
      const table = db.getTable(cmd.table);
      table.rows.forEach((row) => {
        let whereOk = true;
        if (cmd.where) {
          const colType = table.columns.find(
            (c) => c.name === cmd.where.column,
          )?.type;
          const whereVal = colType
            ? table.castValue(colType, cmd.where.value)
            : cmd.where.value;
          whereOk = row[cmd.where.column] === whereVal;
        }

        if (whereOk) {
          Object.keys(cmd.sets).forEach((col) => {
            const colType = table.columns.find((c) => c.name === col)?.type;
            row[col] = colType
              ? table.castValue(colType, cmd.sets[col])
              : cmd.sets[col];
          });
        }
      });
      console.log("Table after UPDATE:");
      console.log(table.select());
      break;
    }
    case "DELETE": {
      const table = db.getTable(cmd.table);

      if (!cmd.where) {
        throw new Error("DELETE without WHERE is not allowed");
      }

      const before = table.rows.length;

      table.rows = table.rows.filter((row) => {
        const colType = table.columns.find(
          (c) => c.name === cmd.where.column,
        )?.type;
        const whereVal = colType
          ? table.castValue(colType, cmd.where.value)
          : cmd.where.value;
        return row[cmd.where.column] !== whereVal;
      });

      const after = table.rows.length;

      console.log(`${before - after} row(s) deleted`);
      break;
    }

    default:
      console.error(`Unsupported command: ${cmd.command}`);
  }
}

// INSERT example
const insertSql =
  "INSERT INTO users (id, name, email, isActive) VALUES (2, 'John', 'john@example.com', 0)";
const insertCmd = parseSql(insertSql);
execute(insertCmd, db);

// SELECT example
const selectSql = "SELECT * FROM users";
const selectCmd = parseSql(selectSql);
execute(selectCmd, db);

// UPDATE example
const updateSql =
  "UPDATE users SET email='helen@gmail.com', isActive=0 WHERE id=1";
const updateCmd = parseSql(updateSql);
execute(updateCmd, db);

// SELECT with WHERE example
const selectWhereSql = "SELECT * FROM users WHERE id=1";
const selectWhereCmd = parseSql(selectWhereSql);
execute(selectWhereCmd, db);
