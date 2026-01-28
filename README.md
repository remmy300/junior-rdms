# Mini JavaScript SQL Engine

A lightweight, in-memory SQL engine written in JavaScript.  
Supports SELECT queries with multiple joins, aggregates, GROUP BY, HAVING, WHERE, DISTINCT, ORDER BY, LIMIT, and OFFSET.

This project is a **learning-focused SQL engine** to experiment with query execution, aggregation, and join logic in pure JavaScript.

---

## Features

- **Table creation** with columns and primary key enforcement
- **Data insertion** with type casting (`INT`, `TEXT`, `BOOLEAN`)
- **SELECT queries** with:
  - Column projection (`*` or specific columns)
  - WHERE conditions (`=`, `<`, `>`, `<=`, `>=`)
  - Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
  - GROUP BY support
  - HAVING support for aggregates
  - DISTINCT rows
  - ORDER BY, LIMIT, and OFFSET
- **Joins**:
  - `INNER JOIN`
  - `LEFT JOIN`
  - `RIGHT JOIN`
  - `FULL JOIN`
- **NULL handling** and safe aggregate computation
- Fully **alias-aware** to avoid column ambiguity in joins

---
