-- A `PREWHERE` on a view is executed by the table that the view reads from, as its own `PREWHERE`.

DROP VIEW IF EXISTS v_prewhere_all;
DROP VIEW IF EXISTS v_prewhere_columns;
DROP VIEW IF EXISTS v_prewhere_definer;
DROP VIEW IF EXISTS v_prewhere_of_view;
DROP VIEW IF EXISTS v_prewhere_merge;
DROP VIEW IF EXISTS v_prewhere_subcolumns;
DROP VIEW IF EXISTS v_prewhere_none;
DROP VIEW IF EXISTS v_prewhere_stateful;
DROP VIEW IF EXISTS v_prewhere_aggregate;
DROP VIEW IF EXISTS v_prewhere_final;
DROP VIEW IF EXISTS v_prewhere_array_join;
DROP VIEW IF EXISTS v_prewhere_own_prewhere;
DROP VIEW IF EXISTS v_prewhere_limit;
DROP VIEW IF EXISTS v_prewhere_numbers;
DROP VIEW IF EXISTS v_prewhere_empty;
DROP VIEW IF EXISTS v_prewhere_parameterized;
DROP TABLE IF EXISTS t_prewhere_view_empty;
DROP TABLE IF EXISTS m_prewhere_view;
DROP TABLE IF EXISTS t_prewhere_view;
DROP TABLE IF EXISTS t_prewhere_view_other;
DROP TABLE IF EXISTS t_prewhere_view_subcolumns;

CREATE TABLE t_prewhere_view (a UInt64, b UInt64, s String, arr Array(UInt64), flag UInt8 MATERIALIZED a % 2, d UInt64 ALIAS a * 2)
ENGINE = ReplacingMergeTree ORDER BY a;
INSERT INTO t_prewhere_view SELECT number, 100 - number, toString(number), [number, 5] FROM numbers(100);
CREATE TABLE t_prewhere_view_other (a UInt64) ENGINE = Memory;
INSERT INTO t_prewhere_view_other VALUES (200), (7);
CREATE TABLE m_prewhere_view (a UInt64, b UInt64) ENGINE = Merge(currentDatabase(), '^t_prewhere_view$');

CREATE VIEW v_prewhere_all AS SELECT * FROM t_prewhere_view;
CREATE VIEW v_prewhere_columns AS SELECT b AS x, s, flag, d, a + b AS total FROM t_prewhere_view WHERE a >= 10;
CREATE VIEW v_prewhere_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM t_prewhere_view WHERE b > 10;
CREATE VIEW v_prewhere_of_view AS SELECT x AS y, total FROM v_prewhere_columns;
CREATE VIEW v_prewhere_merge AS SELECT a, b FROM m_prewhere_view WHERE b > 3;

SELECT 'the same rows as with WHERE';
SELECT count(), sum(a) FROM v_prewhere_all PREWHERE a < 10;
SELECT count(), sum(a) FROM v_prewhere_all WHERE a < 10;
-- Columns that the view renames or calculates, and `MATERIALIZED` and `ALIAS` columns of the table.
SELECT count(), sum(x) FROM v_prewhere_columns PREWHERE x >= 50 AND s != '40' AND total = 100 AND d > 30 AND flag;
SELECT count(), sum(x) FROM v_prewhere_columns WHERE x >= 50 AND s != '40' AND total = 100 AND d > 30 AND flag;
SELECT count(), sum(total) FROM v_prewhere_columns PREWHERE flag WHERE x < 60;
SELECT count(), sum(total) FROM v_prewhere_columns WHERE flag AND x < 60;
-- A lambda that uses a column of the view, a set, and a subquery.
SELECT count() FROM v_prewhere_all PREWHERE arrayExists(e -> e = a + 1, arr) OR a = 5;
SELECT count() FROM v_prewhere_all WHERE arrayExists(e -> e = a + 1, arr) OR a = 5;
SELECT count() FROM v_prewhere_all PREWHERE a IN t_prewhere_view_other;
SELECT count() FROM v_prewhere_all PREWHERE a IN (SELECT a FROM t_prewhere_view WHERE a < 10);
SELECT count() FROM v_prewhere_all PREWHERE a > (SELECT max(a) FROM t_prewhere_view_other WHERE a < 100);
-- A condition without a column of the view, and a condition that the query also selects.
SELECT count() FROM v_prewhere_all PREWHERE 1;
SELECT a, a < 2 FROM v_prewhere_all PREWHERE a < 2 ORDER BY a;
-- A view of a view, a view of a `Merge` table, and a view that selects with the rights of its definer.
SELECT count(), sum(y) FROM v_prewhere_of_view PREWHERE y >= 50 AND total = 100;
SELECT count(), sum(a) FROM v_prewhere_merge PREWHERE a < 10;
SELECT count(), sum(a) FROM v_prewhere_definer PREWHERE a < 10;
-- A parameterized view, and a view of a table without data.
CREATE VIEW v_prewhere_parameterized AS SELECT a, b FROM t_prewhere_view WHERE a < {bound:UInt64};
SELECT count(), sum(a) FROM v_prewhere_parameterized(bound = 50) PREWHERE b < 60;
SELECT count() FROM v_prewhere_parameterized(bound = 50) PREWHERE 1;
CREATE TABLE t_prewhere_view_empty (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_prewhere_empty AS SELECT a FROM t_prewhere_view_empty;
SELECT count() FROM v_prewhere_empty PREWHERE a < 10;
-- The view is not inlined into the query.
SELECT count() FROM v_prewhere_all PREWHERE a < 10 SETTINGS analyzer_inline_views = 1;
-- It applies to the view before a join.
SELECT count() FROM v_prewhere_all AS l LEFT JOIN v_prewhere_all AS r ON l.a = r.a + 1 PREWHERE l.a < 10;
SELECT count() FROM v_prewhere_all AS l LEFT JOIN v_prewhere_all AS r ON l.a = r.a + 1 PREWHERE r.a < 10;

SELECT 'the table executes it as its own PREWHERE';
SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM v_prewhere_columns PREWHERE x >= 50 SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT total FROM v_prewhere_of_view PREWHERE y >= 50 SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Prewhere filter column%';
SELECT count() > 1 FROM (EXPLAIN actions = 1 SELECT b FROM v_prewhere_merge PREWHERE a < 10 SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Prewhere filter column%';
-- The primary key is used.
SELECT count() FROM v_prewhere_all PREWHERE a < 10 SETTINGS force_primary_key = 1;

CREATE TABLE t_prewhere_view_subcolumns (k UInt64, x Tuple(a UInt64, b String), n Nullable(UInt64), j JSON)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_prewhere_view_subcolumns
SELECT number, (number, toString(number)), if(number % 2, number, NULL), toJSONString(map('p', map('q', number))) FROM numbers(100);
CREATE VIEW v_prewhere_subcolumns AS SELECT k, x, x AS y, n, j FROM t_prewhere_view_subcolumns;
SELECT 'subcolumns';
SELECT count() FROM v_prewhere_subcolumns PREWHERE x.a < 10;
SELECT count() FROM v_prewhere_subcolumns PREWHERE y.a < 10 AND tupleElement(x, 'b') != '5';
SELECT count() FROM v_prewhere_subcolumns PREWHERE n.null;
SELECT count() FROM v_prewhere_subcolumns PREWHERE j.p.q = 5;

-- The condition must reach the read of the table as a whole.
CREATE VIEW v_prewhere_aggregate AS SELECT s, count() AS c FROM t_prewhere_view GROUP BY s;
CREATE VIEW v_prewhere_final AS SELECT * FROM t_prewhere_view FINAL;
CREATE VIEW v_prewhere_array_join AS SELECT a, arrayJoin(arr) AS e FROM t_prewhere_view;
CREATE VIEW v_prewhere_own_prewhere AS SELECT a, b FROM t_prewhere_view PREWHERE b > 50;
CREATE VIEW v_prewhere_limit AS SELECT a, b FROM t_prewhere_view ORDER BY a LIMIT 20;
CREATE VIEW v_prewhere_numbers AS SELECT number AS a FROM numbers(100);
SELECT count() FROM v_prewhere_aggregate PREWHERE c = 1; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_aggregate PREWHERE s = '5'; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_final PREWHERE b < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_array_join PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_own_prewhere PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_limit PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_numbers PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }

-- A function that depends on other rows would see the rows that are left after the condition.
CREATE VIEW v_prewhere_stateful AS SELECT a, rowNumberInAllBlocks() AS n FROM t_prewhere_view;
SELECT count(), sum(n) FROM v_prewhere_stateful PREWHERE a >= 50; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_all PREWHERE a = rowNumberInAllBlocks(); -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_all PREWHERE blockSize() > 5; -- { serverError ILLEGAL_PREWHERE }

-- A view with `SQL SECURITY NONE` hides its query from the query that reads it.
CREATE VIEW v_prewhere_none SQL SECURITY NONE AS SELECT * FROM t_prewhere_view WHERE b > 10;
SELECT count() FROM v_prewhere_none PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }

-- The condition must not see the rows that a row policy on the view hides.
DROP ROW POLICY IF EXISTS p_prewhere_view ON v_prewhere_all;
CREATE ROW POLICY p_prewhere_view ON v_prewhere_all USING a < 50 TO ALL;
SELECT count() FROM v_prewhere_all PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY p_prewhere_view ON v_prewhere_all;

DROP VIEW v_prewhere_all;
DROP VIEW v_prewhere_columns;
DROP VIEW v_prewhere_definer;
DROP VIEW v_prewhere_of_view;
DROP VIEW v_prewhere_merge;
DROP VIEW v_prewhere_subcolumns;
DROP VIEW v_prewhere_none;
DROP VIEW v_prewhere_stateful;
DROP VIEW v_prewhere_aggregate;
DROP VIEW v_prewhere_final;
DROP VIEW v_prewhere_array_join;
DROP VIEW v_prewhere_own_prewhere;
DROP VIEW v_prewhere_limit;
DROP VIEW v_prewhere_numbers;
DROP VIEW v_prewhere_empty;
DROP VIEW v_prewhere_parameterized;
DROP TABLE t_prewhere_view_empty;
DROP TABLE m_prewhere_view;
DROP TABLE t_prewhere_view;
DROP TABLE t_prewhere_view_other;
DROP TABLE t_prewhere_view_subcolumns;
