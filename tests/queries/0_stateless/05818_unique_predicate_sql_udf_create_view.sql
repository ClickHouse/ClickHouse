-- Tags: no-parallel
-- Tag no-parallel: creates a global SQL user defined function with the fixed name `__unique`.

-- The `UNIQUE` predicate in the body of a SQL user defined function named `__unique` is not a recursive call:
-- the AST-level expansion of user defined functions in `CREATE VIEW` must not reject it.
-- See https://github.com/ClickHouse/ClickHouse/pull/99877

DROP FUNCTION IF EXISTS __unique;
DROP VIEW IF EXISTS v_unique_udf_05818;

CREATE FUNCTION __unique AS x -> UNIQUE(SELECT 1);

SELECT __unique(1);

CREATE VIEW v_unique_udf_05818 AS SELECT __unique(1) AS u;
SELECT * FROM v_unique_udf_05818;

DROP VIEW v_unique_udf_05818;
DROP FUNCTION __unique;
