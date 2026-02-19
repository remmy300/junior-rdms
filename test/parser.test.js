import test from "node:test";
import assert from "node:assert/strict";
import { parseSql, parseSelect } from "../src/parser.js";

test("parseSql handles CREATE TABLE", () => {
  const cmd = parseSql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
  assert.equal(cmd.command, "CREATE_TABLE");
  assert.equal(cmd.table, "users");
  assert.equal(cmd.primaryKey, "id");
  assert.deepEqual(cmd.columns, [
    { name: "id", type: "INT" },
    { name: "name", type: "TEXT" },
  ]);
});

test("parseSelect keeps >= and <= operators intact", () => {
  const ge = parseSelect("SELECT * FROM users WHERE salary >= 3000");
  const le = parseSelect("SELECT * FROM users WHERE salary <= 3000");

  assert.equal(ge.where.conditions[0].operator, ">=");
  assert.equal(ge.where.conditions[0].value, 3000);
  assert.equal(le.where.conditions[0].operator, "<=");
  assert.equal(le.where.conditions[0].value, 3000);
});
