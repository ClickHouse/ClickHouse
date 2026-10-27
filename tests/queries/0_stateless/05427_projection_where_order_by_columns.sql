-- A normal projection with `WHERE` stores the columns its `ORDER BY` reads, not the `ORDER BY` expression.
-- https://github.com/ClickHouse/ClickHouse/issues/126679
-- https://github.com/ClickHouse/ClickHouse/issues/126800

SET optimize_use_projections = 1;

DROP TABLE IF EXISTS t_offsets;
DROP TABLE IF EXISTS t_select_k;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_materialize;
DROP TABLE IF EXISTS t_all;
DROP TABLE IF EXISTS t_intdiv;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_replace;
DROP TABLE IF EXISTS t_with;
DROP TABLE IF EXISTS t_alias_where;
DROP TABLE IF EXISTS t_alias_where_agg;

-- The `ORDER BY` columns are not selected: every `INSERT` used to fail with `UNKNOWN_IDENTIFIER`.
CREATE TABLE t_offsets (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_offsets ADD PROJECTION p (SELECT _part_offset WHERE s = 'a' ORDER BY (v, k));
INSERT INTO t_offsets SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);

CREATE TABLE t_select_k (k UInt64, v UInt64, s String, PROJECTION p (SELECT k WHERE s = 'a' ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_select_k SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);

CREATE TABLE t_tuple (k UInt64, v UInt64, s String, PROJECTION p (SELECT _part_offset WHERE s = 'a' ORDER BY tuple(v, k)))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_tuple SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);

-- The projection is built for parts which existed before it.
CREATE TABLE t_materialize (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_materialize SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
ALTER TABLE t_materialize ADD PROJECTION p (SELECT _part_offset WHERE s = 'a' ORDER BY (v, k));
ALTER TABLE t_materialize MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;

SELECT 'rows', count() FROM t_offsets;
SELECT 'rows', count() FROM t_select_k;
SELECT 'rows', count() FROM t_tuple;
SELECT 'rows', count() FROM t_materialize;

SELECT 'projection rows', table, sum(rows)
FROM system.projection_parts
WHERE database = currentDatabase() AND active AND table IN ('t_offsets', 't_select_k', 't_tuple', 't_materialize')
GROUP BY table ORDER BY table;

SELECT 'projection columns', table, arrayStringConcat(arraySort(groupUniqArray(column)), ', ')
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND active AND table IN ('t_offsets', 't_select_k', 't_tuple', 't_materialize')
GROUP BY table ORDER BY table;

-- Every projection row points to a parent row with the same `v` and `k`, and that row matches `WHERE`.
SELECT 'parent rows', count()
FROM mergeTreeProjection(currentDatabase(), t_offsets, p) AS pp
INNER JOIN (SELECT _part AS part, _part_offset AS offset, k, v, s FROM t_offsets) AS tt
    ON pp._part = tt.part AND pp._parent_part_offset = tt.offset
WHERE pp.v = tt.v AND pp.k = tt.k AND tt.s = 'a';

SELECT 'parent rows', count()
FROM mergeTreeProjection(currentDatabase(), t_tuple, p) AS pp
INNER JOIN (SELECT _part AS part, _part_offset AS offset, k, v, s FROM t_tuple) AS tt
    ON pp._part = tt.part AND pp._parent_part_offset = tt.offset
WHERE pp.v = tt.v AND pp.k = tt.k AND tt.s = 'a';

SELECT 'parent rows', count()
FROM mergeTreeProjection(currentDatabase(), t_materialize, p) AS pp
INNER JOIN (SELECT _part AS part, _part_offset AS offset, k, v, s FROM t_materialize) AS tt
    ON pp._part = tt.part AND pp._parent_part_offset = tt.offset
WHERE pp.v = tt.v AND pp.k = tt.k AND tt.s = 'a';

-- Each projection part is sorted by `(v, k)`.
SELECT 'unsorted parts', countIf(a != arraySort(a))
FROM
(
    SELECT arrayMap(x -> (x.2, x.3), arraySort(x -> x.1, groupArray((_part_offset, v, k)))) AS a
    FROM mergeTreeProjection(currentDatabase(), t_select_k, p)
    GROUP BY _part
);

-- The `ORDER BY` columns are selected: the part used to also store the `ORDER BY` expression as a column.
CREATE TABLE t_all (k UInt64, v UInt64, s String, PROJECTION p (SELECT * WHERE s = 'a' ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_intdiv (k UInt64, v UInt64, s String, PROJECTION p (SELECT * WHERE s = 'a' ORDER BY intDiv(v, 10)))
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_all SELECT number, intHash64(number) % 1000, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
INSERT INTO t_intdiv SELECT number, intHash64(number) % 1000, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);

SELECT 'projection columns', table, arrayStringConcat(arraySort(groupUniqArray(column)), ', ')
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND active AND table IN ('t_all', 't_intdiv')
GROUP BY table ORDER BY table;

INSERT INTO t_all SELECT number + 1000, intHash64(number + 1000) % 1000, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
INSERT INTO t_intdiv SELECT number + 1000, intHash64(number + 1000) % 1000, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
OPTIMIZE TABLE t_all FINAL;
OPTIMIZE TABLE t_intdiv FINAL;

SELECT 'merged projection columns', table, arrayStringConcat(arraySort(groupUniqArray(column)), ', ')
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND active AND table IN ('t_all', 't_intdiv')
GROUP BY table ORDER BY table;

SELECT 'projection', count(), sum(k), sum(v) FROM t_all WHERE s = 'a' SETTINGS force_optimize_projection = 1;
SELECT 'no projection', count(), sum(k), sum(v) FROM t_all WHERE s = 'a' SETTINGS optimize_use_projections = 0;
SELECT 'projection', count(), sum(k), sum(v) FROM t_intdiv WHERE s = 'a' SETTINGS force_optimize_projection = 1;
SELECT 'no projection', count(), sum(k), sum(v) FROM t_intdiv WHERE s = 'a' SETTINGS optimize_use_projections = 0;

-- An alias named like a column the `ORDER BY` reads does not replace the column the projection stores.
CREATE TABLE t_alias (k UInt64, v UInt64, s String, PROJECTION p (SELECT _part_offset, 100 - v AS v WHERE s = 'a' ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_replace (k UInt64, v UInt64, s String, PROJECTION p (SELECT * REPLACE (100 - v AS v) WHERE s = 'a' ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_with (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_alias SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
INSERT INTO t_replace SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
INSERT INTO t_with SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);
ALTER TABLE t_with ADD PROJECTION p (WITH 100 - v AS v SELECT _part_offset WHERE s = 'a' ORDER BY (v, k));
ALTER TABLE t_with MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;

SELECT 'projection columns', table, arrayStringConcat(arraySort(groupUniqArray(column)), ', ')
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND active AND table IN ('t_alias', 't_replace', 't_with')
GROUP BY table ORDER BY table;

-- Each projection part is sorted by `(100 - v, k)`.
SELECT 'unsorted parts', table, countIf(a != arraySort(a))
FROM
(
    SELECT 't_alias' AS table, arrayMap(x -> (100 - x.2, x.3), arraySort(x -> x.1, groupArray((_part_offset, v, k)))) AS a
    FROM mergeTreeProjection(currentDatabase(), t_alias, p) GROUP BY _part
    UNION ALL
    SELECT 't_replace', arrayMap(x -> (100 - x.2, x.3), arraySort(x -> x.1, groupArray((_part_offset, v, k))))
    FROM mergeTreeProjection(currentDatabase(), t_replace, p) GROUP BY _part
    UNION ALL
    SELECT 't_with', arrayMap(x -> (100 - x.2, x.3), arraySort(x -> x.1, groupArray((_part_offset, v, k))))
    FROM mergeTreeProjection(currentDatabase(), t_with, p) GROUP BY _part
)
GROUP BY table ORDER BY table;

-- A `WHERE` that reads such an alias filters by `100 - v`, so it does not cover a query filter on the column `v`.
CREATE TABLE t_alias_where (k UInt64, v UInt64, PROJECTION p (SELECT k, 100 - v AS v WHERE v = 90 ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_alias_where_agg (k UInt64, v UInt64, PROJECTION p (SELECT 100 - v AS v, count() WHERE v = 90 GROUP BY v))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_alias_where VALUES (10, 10), (11, 10), (90, 90);
INSERT INTO t_alias_where_agg VALUES (10, 10), (11, 10), (90, 90);
OPTIMIZE TABLE t_alias_where FINAL;

SELECT 'alias in where', count(), sum(k) FROM t_alias_where WHERE v = 90;
SELECT 'alias in where, no projection', count(), sum(k) FROM t_alias_where WHERE v = 90 SETTINGS optimize_use_projections = 0;
SELECT 'alias in where, aggregate', count() FROM t_alias_where_agg WHERE v = 90;
SELECT 'alias in where, aggregate, no projection', count() FROM t_alias_where_agg WHERE v = 90 SETTINGS optimize_use_projections = 0;

DROP TABLE t_offsets;
DROP TABLE t_select_k;
DROP TABLE t_tuple;
DROP TABLE t_materialize;
DROP TABLE t_all;
DROP TABLE t_intdiv;
DROP TABLE t_alias;
DROP TABLE t_replace;
DROP TABLE t_with;
DROP TABLE t_alias_where;
DROP TABLE t_alias_where_agg;
