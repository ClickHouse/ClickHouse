# SQLLogic Test: Known Failures Classification

Results from running the full [sqllogictest](https://github.com/gregrahn/sqllogictest)
corpus against ClickHouse (Feb 2026).

## Summary

| Test Mode | Passed | Failed | Total | Pass Rate |
|:----------|-------:|-------:|------:|----------:|
| Statements | 210,703 | 8 | 210,711 | 99.996% |
| Queries | 5,527,171 | 201,699 | 5,728,870 | 96.5% |
| **Total** | **5,737,874** | **201,707** | **5,939,581** | **96.6%** |

## Statement Failures (8)

All 8 are unsupported SQLite-specific features:

- `DROP INDEX` without `ON` clause
- `REINDEX`
- `CREATE TRIGGER` / `DROP TRIGGER`
- `UPDATE` without `WHERE` clause
- `UNIQUE` column constraint
- `INSERT OR REPLACE`
- `count(DISTINCT *)`

## Query Failures by Root Cause (201,699)

### Integer division semantics — ~85,000 (42%)

ClickHouse returns `Float64` for the `/` operator, while SQLite truncates to integer.
For example, `SELECT 35 / 50` returns `0.7` in ClickHouse but `0` in SQLite.

Manifests as hash mismatches in arithmetic expressions, including those inside
`CASE`, `COALESCE`, `NULLIF`, `WHERE` clauses, and subqueries.

| Subcategory | Count |
|:------------|------:|
| Direct integer division | 81,559 |
| Division inside subquery/IN | 3,391 |
| Division inside CASE | 1,165 |
| Division inside COALESCE | 501 |
| Division inside NULLIF | 397 |
| Division inside CAST NULL | 512 |
| Other arithmetic mismatches | 4,479 |

### Duplicate alias rejection — ~42,000 (21%)

ClickHouse rejects queries with duplicate column aliases like
`SELECT expr1 AS col, expr2 AS col` (Code 179), while SQLite allows them.

| Subcategory | Count |
|:------------|------:|
| Duplicate alias (Code 179) | 39,237 |
| Block structure mismatch from dup names (Code 352) | 2,724 |

### `NOT` operator precedence — ~34,000 (17%)

When `NOT` is followed by a parenthesized expression, ClickHouse parses `NOT (expr)`
as a function call `not(expr)`, giving it tight binding. In standard SQL, `NOT` has
low precedence (lower than `IS NULL`, `BETWEEN`, `IN`, `LIKE`).

This causes `NOT (x) IS NULL` to be parsed as `isNull(not(x))` instead of
`NOT (x IS NULL)`, and similarly for `BETWEEN`, `LIKE`, etc.

| Subcategory | Count |
|:------------|------:|
| `NOT (...) IS [NOT] NULL` (wrong row count) | 16,956 |
| Other `NOT` patterns (wrong row count) | 12,975 |
| `NOT (...) BETWEEN` (wrong row count) | 2,489 |
| `NOT` + `IS NULL` variant patterns (wrong row count) | 918 |
| `NOT (col) IS NOT NULL` on non-boolean (Code 43) | 484 |

### Type system strictness — ~16,000 (8%)

ClickHouse has a stricter type system than SQLite. Several patterns cause exceptions:

| Subcategory | Count |
|:------------|------:|
| No supertype for Int64/Float64 in CASE branches (Code 386) | 11,645 |
| Cannot convert NULL to non-Nullable type (Code 349) | 3,653 |

### Subquery and `IN` behavior — ~16,000 (8%)

Different NULL handling and row-count behavior in subqueries, `IN` clauses,
`EXISTS`, and set operations (`UNION`/`EXCEPT`/`INTERSECT`).

| Subcategory | Count |
|:------------|------:|
| `IN` subquery (row count) | 15,374 |
| `EXISTS` subquery (row count) | 565 |
| `UNION`/`EXCEPT`/`INTERSECT` (row count) | 34 |

### SQL compatibility gaps — ~2,600 (1%)

| Subcategory | Count |
|:------------|------:|
| Parenthesized FROM joins `(t1 CROSS JOIN t2)` (Code 62) | 989 |
| Non-aggregated column in GROUP BY HAVING (Code 215) | 908 |
| Non-aggregated column in COALESCE with GROUP BY (Code 50) | 517 |
| Rowid x-columns not available | 183 |

### Minor (< 100 total)

| Subcategory | Count |
|:------------|------:|
| Division by zero / inf conversion (Code 70) | 26 |
| Overflow inf/-inf | 15 |
| Syntax errors | 7 |
| Unknown function `totalDistinct` (Code 46) | 3 |
| Statement did not fail as expected | 1 |
