-- https://github.com/ClickHouse/ClickHouse/issues/132717

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (c0 Int32) ENGINE = Memory;
INSERT INTO t1 VALUES (1);
CREATE VIEW v1 AS SELECT q.k AS k, q.__ch_value2 AS __ch_value2
FROM (SELECT t.c0 AS k, toInt32(0) AS __ch_value2 FROM t1 AS t) AS q ORDER BY k;

SELECT hex(formatRowNoNewline('RowBinary', CAST(result.k AS Int32)))
FROM (SELECT l.k AS k FROM (SELECT toInt32(0) AS k) AS l
      WHERE NOT EXISTS (SELECT 1 FROM v1 AS r WHERE ((l.k = r.__ch_value2) OR true))) AS result;

SELECT arraySort(groupArray(k)) FROM (SELECT l.k AS k FROM (SELECT toInt32(number) AS k FROM numbers(3)) AS l
    WHERE NOT EXISTS (SELECT 1 FROM v1 AS r WHERE l.k = r.__ch_value2 OR true));
SELECT arraySort(groupArray(k)) FROM (SELECT l.k AS k FROM (SELECT toInt32(number) AS k FROM numbers(3)) AS l
    WHERE EXISTS (SELECT 1 FROM v1 AS r WHERE l.k = r.__ch_value2 OR true));
SELECT arraySort(groupArray(k)) FROM (SELECT l.k AS k FROM (SELECT toInt32(number) AS k FROM numbers(3)) AS l
    WHERE EXISTS (SELECT l.k FROM v1));
SELECT arraySort(groupArray(k)) FROM (SELECT l.k AS k FROM (SELECT toInt32(number) AS k FROM numbers(3)) AS l
    WHERE NOT EXISTS (SELECT 1 FROM v1 AS r WHERE l.k = r.__ch_value2 OR true))
SETTINGS correlated_subqueries_default_join_kind = 'left';

TRUNCATE TABLE t1;
SELECT arraySort(groupArray(k)) FROM (SELECT l.k AS k FROM (SELECT toInt32(number) AS k FROM numbers(3)) AS l
    WHERE NOT EXISTS (SELECT 1 FROM v1 AS r WHERE l.k = r.__ch_value2 OR true));

DROP VIEW v1;
DROP TABLE t1;
