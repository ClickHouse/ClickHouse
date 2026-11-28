-- An `additional_table_filters` entry keyed on a view must be applied when `analyzer_inline_views = 1`.

SET analyzer_inline_views = 1;

DROP VIEW IF EXISTS vv;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(10);
CREATE VIEW v AS SELECT x FROM t;
CREATE VIEW vv AS SELECT x FROM v;

SELECT count() FROM v SETTINGS additional_table_filters = {'v': 'x = 1'};
SELECT count() FROM v AS a SETTINGS additional_table_filters = {'a': 'x = 1'};
SELECT count() FROM vv SETTINGS additional_table_filters = {'v': 'x = 1'};
SELECT count() FROM v AS a INNER JOIN t AS b ON a.x = b.x SETTINGS additional_table_filters = {'a': 'x < 3'};

-- A key on another table keeps the view inlined.
SELECT count() FROM v SETTINGS additional_table_filters = {'t': 'x = 1'};
SELECT countIf(explain LIKE concat('%table_name: ', currentDatabase(), '.v')) FROM (EXPLAIN QUERY TREE SELECT count() FROM v SETTINGS additional_table_filters = {'t': 'x = 1'});
SELECT countIf(explain LIKE concat('%table_name: ', currentDatabase(), '.v')) FROM (EXPLAIN QUERY TREE SELECT count() FROM v SETTINGS additional_table_filters = {'v': 'x = 1'});

DROP VIEW vv;
DROP VIEW v;
DROP TABLE t;
