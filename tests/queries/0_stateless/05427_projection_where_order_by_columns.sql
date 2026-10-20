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

-- An alias named like a column the `ORDER BY` reads would replace that column.
CREATE TABLE t_alias (k UInt64, v UInt64, s String, PROJECTION p (SELECT _part_offset, v + 1 AS v WHERE s = 'a' ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k; -- { serverError ILLEGAL_PROJECTION }
CREATE TABLE t_alias (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_alias ADD PROJECTION p (SELECT k WHERE (v + 1 AS v) > 3 ORDER BY v); -- { serverError ILLEGAL_PROJECTION }
ALTER TABLE t_alias ADD PROJECTION p (WITH v + 1 AS v SELECT _part_offset WHERE s = 'a' ORDER BY (v, k)); -- { serverError ILLEGAL_PROJECTION }
-- An alias inside a nested query does not replace a column. With parallel replicas the definition would replace
-- this subquery with a temporary table name in the stored projection query.
ALTER TABLE t_alias ADD PROJECTION p (SELECT _part_offset, v + 1 AS w WHERE s IN (SELECT 'a' AS v) ORDER BY (w, k))
SETTINGS enable_parallel_replicas = 0;
INSERT INTO t_alias SELECT number, number % 10, if(number % 2 = 0, 'a', 'b') FROM numbers(1000);

SELECT 'projection rows', sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND active AND table = 't_alias';
SELECT 'projection columns', arrayStringConcat(arraySort(groupUniqArray(column)), ', ')
FROM system.projection_parts_columns WHERE database = currentDatabase() AND active AND table = 't_alias';

DROP TABLE t_offsets;
DROP TABLE t_select_k;
DROP TABLE t_tuple;
DROP TABLE t_materialize;
DROP TABLE t_all;
DROP TABLE t_intdiv;
DROP TABLE t_alias;
