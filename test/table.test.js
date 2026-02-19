import test from "node:test";
import assert from "node:assert/strict";
import { Table } from "../src/table.js";

function buildUsersTable() {
  const table = new Table(
    "users",
    [
      { name: "id", type: "INT" },
      { name: "salary", type: "INT" },
    ],
    "id",
  );

  table.insert({ id: 1, salary: 5000 });
  table.insert({ id: 2, salary: 3000 });
  table.insert({ id: 3, salary: null });
  return table;
}

test("MIN aggregate works without GROUP BY", () => {
  const table = buildUsersTable();
  const rows = table.select([{ column: "MIN(salary)", alias: "min_salary" }]);
  assert.deepEqual(rows, [{ min_salary: 3000 }]);
});

test("MAX aggregate works without GROUP BY", () => {
  const table = buildUsersTable();
  const rows = table.select([{ column: "MAX(salary)", alias: "max_salary" }]);
  assert.deepEqual(rows, [{ max_salary: 5000 }]);
});
