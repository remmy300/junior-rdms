# Mini JavaScript SQL Engine

A lightweight in-memory SQL engine written in JavaScript to explore parser and query-execution internals.

## Quick Start

```bash
npm install
npm start
```

Run tests:

```bash
npm test
```

## Implemented Features

- `CREATE TABLE` with typed columns: `INT`, `TEXT`, `BOOLEAN`
- Optional primary key enforcement
- `INSERT`, `UPDATE`, `DELETE`
- `SELECT` with:
- Projection (`*` or explicit columns with aliases)
- `WHERE` (`=`, `<`, `>`, `<=`, `>=`) with `AND` / `OR`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `GROUP BY` and `HAVING`
- `DISTINCT`
- `ORDER BY`, `LIMIT`, `OFFSET`
- `NULL`-safe aggregate handling

## Example Queries

```sql
CREATE TABLE users (id INT PRIMARY KEY, department TEXT, salary INT);
INSERT INTO users (id, department, salary) VALUES (1, 'Sales', 5000);
INSERT INTO users (id, department, salary) VALUES (2, 'HR', 3200);

SELECT department, AVG(salary) AS avg_salary
FROM users
GROUP BY department
HAVING AVG(salary) >= 3000
ORDER BY avg_salary DESC;
```

## Current Scope

- This project is intentionally in-memory and single-process.
- SQL coverage is intentionally partial and focused on learning.
- JOIN parser integration is not complete yet (join execution primitives exist in `src/table.js` but are not exposed via SQL parsing).
