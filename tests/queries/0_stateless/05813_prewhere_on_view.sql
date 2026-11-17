-- A `PREWHERE` on a view that selects stored columns of a single table is moved into the query of the view.

DROP TABLE IF EXISTS t_prewhere_view;
DROP VIEW IF EXISTS v_prewhere_all;
DROP VIEW IF EXISTS v_prewhere_columns;
DROP VIEW IF EXISTS v_prewhere_definer;
DROP VIEW IF EXISTS v_prewhere_qualified;
DROP TABLE IF EXISTS t_prewhere_view_other;
DROP VIEW IF EXISTS v_prewhere_swapped;
DROP VIEW IF EXISTS v_prewhere_alias_of_alias;
DROP VIEW IF EXISTS v_prewhere_stateful;
DROP VIEW IF EXISTS v_prewhere_aggregate;
DROP VIEW IF EXISTS v_prewhere_final;
DROP VIEW IF EXISTS v_prewhere_none;
DROP VIEW IF EXISTS v_prewhere_of_view;
DROP VIEW IF EXISTS v_prewhere_array_join;
DROP VIEW IF EXISTS v_prewhere_stateful_alias;
DROP TABLE IF EXISTS t_prewhere_view_stateful_alias;
DROP VIEW IF EXISTS v_prewhere_stateful_default;
DROP TABLE IF EXISTS t_prewhere_view_stateful_default;
DROP VIEW IF EXISTS v_prewhere_subcolumns;
DROP VIEW IF EXISTS v_prewhere_subcolumn_of_two;
DROP TABLE IF EXISTS t_prewhere_view_subcolumns;
DROP USER IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP ROW POLICY IF EXISTS p_prewhere_view ON v_prewhere_all;

CREATE TABLE t_prewhere_view (a UInt64, b UInt64, s String, arr Array(UInt64), flag UInt8 MATERIALIZED a % 2, d UInt64 ALIAS a * 2)
ENGINE = ReplacingMergeTree ORDER BY a;
INSERT INTO t_prewhere_view SELECT number, 100 - number, toString(number), [number, 5] FROM numbers(100);

CREATE VIEW v_prewhere_all AS SELECT * FROM t_prewhere_view;
CREATE VIEW v_prewhere_columns AS SELECT b AS x, s, flag, d, a + b AS total FROM t_prewhere_view WHERE a >= 10;
CREATE VIEW v_prewhere_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM t_prewhere_view WHERE b > 10;
CREATE VIEW v_prewhere_qualified AS SELECT t.a, t.b AS y, arr FROM t_prewhere_view AS t;
CREATE TABLE t_prewhere_view_other (a UInt64) ENGINE = Memory;
INSERT INTO t_prewhere_view_other VALUES (200), (7);

SELECT 'the same rows as with WHERE';
SELECT count(), sum(a) FROM v_prewhere_all PREWHERE a < 10;
SELECT count(), sum(a) FROM v_prewhere_all WHERE a < 10;
SELECT count(), sum(x) FROM v_prewhere_columns PREWHERE x >= 50 AND s != '40';
SELECT count(), sum(x) FROM v_prewhere_columns WHERE x >= 50 AND s != '40';
SELECT count(), sum(total) FROM v_prewhere_columns PREWHERE flag WHERE total = 100;
SELECT count(), sum(total) FROM v_prewhere_columns WHERE flag AND total = 100;
SELECT count() FROM v_prewhere_all PREWHERE arrayExists(e -> e = 4, arr) AND a < 50;
SELECT count() FROM v_prewhere_all WHERE arrayExists(e -> e = 4, arr) AND a < 50;
SELECT count() FROM v_prewhere_qualified PREWHERE a < 10 AND y > 95;
-- A condition without a column of the view.
SELECT count() FROM v_prewhere_all PREWHERE 1;
-- The definer reads the view as its own query.
SELECT count() FROM v_prewhere_definer PREWHERE a < 10;
-- It applies to the view before a join.
SELECT count() FROM v_prewhere_all AS l LEFT JOIN v_prewhere_all AS r ON l.a = r.a + 1 PREWHERE l.a < 10;
SELECT count() FROM v_prewhere_all AS l LEFT JOIN v_prewhere_all AS r ON l.a = r.a + 1 PREWHERE r.a < 10;

SELECT 'the table executes it as its own PREWHERE';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT s FROM v_prewhere_columns PREWHERE x >= 50 SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Prewhere filter column%';
-- The set of the condition is built once, by the query of the view.
SELECT count() FROM v_prewhere_all PREWHERE a IN t_prewhere_view_other;
SELECT count() FROM (EXPLAIN SELECT count() FROM v_prewhere_all PREWHERE a IN t_prewhere_view_other) WHERE explain LIKE '%CreatingSets%';

-- A subcolumn is read from its column, also when the view selects the column under another name.
CREATE TABLE t_prewhere_view_subcolumns (k UInt64, x Tuple(a UInt64, b String), n Nullable(UInt64), j JSON, w Tuple(a Tuple(b UInt64)), z Tuple(b UInt64))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_prewhere_view_subcolumns
SELECT number, (number, toString(number)), if(number % 2, number, NULL), toJSONString(map('p', map('q', number))), tuple(tuple(number)), tuple(1000) FROM numbers(100);
CREATE VIEW v_prewhere_subcolumns AS SELECT k, x, x AS y, n, j FROM t_prewhere_view_subcolumns;
SELECT 'subcolumns';
SELECT count() FROM v_prewhere_subcolumns PREWHERE x.a < 10;
SELECT count() FROM v_prewhere_subcolumns PREWHERE y.a < 10 AND tupleElement(x, 'b') != '5';
SELECT count() FROM v_prewhere_subcolumns PREWHERE n.null;
SELECT count() FROM v_prewhere_subcolumns PREWHERE j.p.q = 5;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k FROM v_prewhere_subcolumns PREWHERE y.a < 10 SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Prewhere filter column%';
-- The name `w.a.b` could be the subcolumn `a.b` of the column `w` or the subcolumn `b` of the column `w.a`.
CREATE VIEW v_prewhere_subcolumn_of_two AS SELECT z AS `w.a`, w FROM t_prewhere_view_subcolumns;
SELECT count() FROM v_prewhere_subcolumn_of_two PREWHERE w.a.b < 10; -- { serverError ILLEGAL_PREWHERE }

-- A column that is not a stored column of the table.
SELECT count() FROM v_prewhere_columns PREWHERE total = 100; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_columns PREWHERE d = 100; -- { serverError ILLEGAL_PREWHERE }
-- A condition that would be read differently in the query of the view. Inside a lambda the name of a column could mean its argument.
SELECT count() FROM v_prewhere_all PREWHERE arrayExists(e -> e = a + 1, arr); -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_all PREWHERE arrayExists(a -> a = v_prewhere_all.a + 1, arr); -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_all PREWHERE a IN (SELECT a FROM t_prewhere_view WHERE a < 10); -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_all PREWHERE a < 10 SETTINGS prefer_column_name_to_alias = 1; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_all FINAL PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
-- A column of another table inside a lambda is not a column of the view.
SELECT l.a, r.a FROM v_prewhere_qualified AS l CROSS JOIN t_prewhere_view_other AS r PREWHERE arrayExists(e -> e = r.a, l.arr); -- { serverError ILLEGAL_PREWHERE }
-- A stateful function would see the rows of the table instead of the rows of the view.
SELECT count() FROM v_prewhere_all PREWHERE a = rowNumberInAllBlocks(); -- { serverError ILLEGAL_PREWHERE }

-- A view whose columns are not the columns of the table, or that changes or hides the rows.
CREATE VIEW v_prewhere_swapped AS SELECT b AS a, a AS b FROM t_prewhere_view;
CREATE VIEW v_prewhere_alias_of_alias AS SELECT rand64() AS b, b AS c FROM t_prewhere_view;
CREATE VIEW v_prewhere_stateful AS SELECT a, rowNumberInAllBlocks() AS n FROM t_prewhere_view;
CREATE VIEW v_prewhere_aggregate AS SELECT s, count() AS c FROM t_prewhere_view GROUP BY s;
CREATE VIEW v_prewhere_final AS SELECT * FROM t_prewhere_view FINAL;
CREATE VIEW v_prewhere_none SQL SECURITY NONE AS SELECT * FROM t_prewhere_view WHERE b > 10;
CREATE VIEW v_prewhere_of_view AS SELECT * FROM v_prewhere_all;
CREATE VIEW v_prewhere_array_join AS SELECT a, arrayJoin(arr) AS e FROM t_prewhere_view;
CREATE TABLE t_prewhere_view_stateful_alias (a UInt64, n UInt64 ALIAS rowNumberInAllBlocks()) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_prewhere_stateful_alias AS SELECT a, n FROM t_prewhere_view_stateful_alias;
-- The default expression is calculated at the read of the part that was written before the column.
CREATE TABLE t_prewhere_view_stateful_default (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_prewhere_view_stateful_default SELECT number FROM numbers(10);
ALTER TABLE t_prewhere_view_stateful_default ADD COLUMN n UInt64 DEFAULT rowNumberInAllBlocks();
CREATE VIEW v_prewhere_stateful_default AS SELECT a, n FROM t_prewhere_view_stateful_default;
SELECT count() FROM v_prewhere_swapped PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_alias_of_alias PREWHERE c % 2 = 0; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_stateful PREWHERE a >= 50; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_aggregate PREWHERE c = 1; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_final PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_none PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_of_view PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_array_join PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_stateful_alias PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM v_prewhere_stateful_default PREWHERE a >= 5; -- { serverError ILLEGAL_PREWHERE }

-- The condition would be analyzed with the rights of the definer, so only the definer moves it into such a view.
CREATE USER {CLICKHOUSE_DATABASE:Identifier};
GRANT SELECT ON v_prewhere_definer TO {CLICKHOUSE_DATABASE:Identifier};
SELECT count() FROM remote('127.0.0.2', currentDatabase(), v_prewhere_definer, {CLICKHOUSE_DATABASE:String}) WHERE a < 10;
SELECT count() FROM remote('127.0.0.2', currentDatabase(), v_prewhere_definer, {CLICKHOUSE_DATABASE:String}) PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }

-- The condition must not see the rows that a row policy on the view hides.
CREATE ROW POLICY p_prewhere_view ON v_prewhere_all USING a < 50 TO ALL;
SELECT count() FROM v_prewhere_all PREWHERE a < 10; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY p_prewhere_view ON v_prewhere_all;

DROP VIEW v_prewhere_all;
DROP VIEW v_prewhere_columns;
DROP VIEW v_prewhere_definer;
DROP VIEW v_prewhere_qualified;
DROP TABLE t_prewhere_view_other;
DROP VIEW v_prewhere_swapped;
DROP VIEW v_prewhere_alias_of_alias;
DROP VIEW v_prewhere_stateful;
DROP VIEW v_prewhere_aggregate;
DROP VIEW v_prewhere_final;
DROP VIEW v_prewhere_none;
DROP VIEW v_prewhere_of_view;
DROP VIEW v_prewhere_array_join;
DROP VIEW v_prewhere_stateful_alias;
DROP TABLE t_prewhere_view_stateful_alias;
DROP VIEW v_prewhere_stateful_default;
DROP TABLE t_prewhere_view_stateful_default;
DROP VIEW v_prewhere_subcolumns;
DROP VIEW v_prewhere_subcolumn_of_two;
DROP TABLE t_prewhere_view_subcolumns;
DROP USER {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE t_prewhere_view;
