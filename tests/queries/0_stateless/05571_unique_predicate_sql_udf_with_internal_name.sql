-- Tags: no-parallel
-- Tag no-parallel: creates a global SQL user defined function with the fixed name `__unique`.

-- `UNIQUE(subquery)` is parsed into the operator `__unique`. A SQL user defined function with this name
-- must not replace the predicate, including the AST-level expansion of user defined functions
-- in `CREATE VIEW`, `ALTER TABLE ... DELETE` and `DELETE FROM`. An ordinary call `__unique(...)` still calls it.

DROP FUNCTION IF EXISTS __unique;
DROP VIEW IF EXISTS v_unique_udf;
DROP TABLE IF EXISTS t_unique_udf;

CREATE FUNCTION __unique AS x -> 42;

SELECT UNIQUE(SELECT 1 FROM numbers(2));
SELECT __unique(1);

CREATE TABLE t_unique_udf (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_unique_udf VALUES (1), (2);

ALTER TABLE t_unique_udf DELETE WHERE UNIQUE(SELECT 1 FROM numbers(2)) SETTINGS mutations_sync = 2;
SELECT count() FROM t_unique_udf;

DELETE FROM t_unique_udf WHERE UNIQUE(SELECT 1 FROM numbers(2));
SELECT count() FROM t_unique_udf;

CREATE VIEW v_unique_udf AS SELECT UNIQUE(SELECT 1) AS u;
SELECT * FROM v_unique_udf;
SELECT position(create_table_query, 'UNIQUE(') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'v_unique_udf';

DROP VIEW v_unique_udf;
DROP TABLE t_unique_udf;
DROP FUNCTION __unique;
