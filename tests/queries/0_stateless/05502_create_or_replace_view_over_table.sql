-- `CREATE OR REPLACE [MATERIALIZED] VIEW` must not replace a table and its data, just like `DROP VIEW` refuses to drop one.
-- https://github.com/ClickHouse/ClickHouse/issues/75690

DROP TABLE IF EXISTS t_05502;
DROP VIEW IF EXISTS v_05502;
DROP VIEW IF EXISTS v_05502_absent;

CREATE TABLE t_05502 (a Int) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05502 VALUES (1);

CREATE OR REPLACE VIEW t_05502 AS SELECT 2 AS b; -- { serverError INCORRECT_QUERY }
CREATE OR REPLACE MATERIALIZED VIEW t_05502 ENGINE = Memory AS SELECT 2 AS b; -- { serverError INCORRECT_QUERY }
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_05502';
SELECT * FROM t_05502;


-- Replacing a view still works.
CREATE VIEW v_05502 AS SELECT 1 AS a;
CREATE OR REPLACE VIEW v_05502 AS SELECT 2 AS b;
SELECT * FROM v_05502;

-- A missing view is still created.
CREATE OR REPLACE VIEW v_05502_absent AS SELECT 3 AS c;
SELECT * FROM v_05502_absent;

DROP VIEW v_05502_absent;
DROP VIEW v_05502;
DROP TABLE t_05502;
