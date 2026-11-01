-- A filter on a constant column of UNION ALL branches reaches each branch as a predicate of constants behind
-- `materialize`, such as `materialize('a') = 'a'`. The row estimate of the branch it selects must not drop.
-- https://github.com/ClickHouse/ClickHouse/issues/128482
-- Random settings limits: query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, 1); materialize_statistics_on_insert=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
SET enable_parallel_replicas = 0;
SET use_hash_table_stats_for_join_reordering = 0;

DROP VIEW IF EXISTS branches;
DROP VIEW IF EXISTS constants;
DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS dim;
CREATE TABLE fact (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
CREATE TABLE dim (k UInt32, y UInt32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic';
INSERT INTO fact SELECT number % 10000, number FROM numbers(100000);
INSERT INTO dim SELECT number, number % 100 FROM numbers(10000);

CREATE VIEW branches AS
    SELECT f.v AS v, d.y AS y, 'a' AS tag, 1 AS b, NULL AS n FROM fact AS f JOIN dim AS d ON f.k = d.k
    UNION ALL
    SELECT f.v AS v, d.y AS y, 'b' AS tag, 1000 AS b, 1 AS n FROM fact AS f JOIN dim AS d ON f.k = d.k;
CREATE VIEW constants AS
    SELECT f.v AS v, d.y AS y, materialize(CAST('b', 'Enum8(''b'' = 1, ''a'' = 2)')) AS e, materialize(toInt8(-1)) AS i,
        materialize(toDecimal32(1.5, 2)) AS dec, materialize(toDate('2026-01-02')) AS dt, materialize(toFixedString('ab', 2)) AS fs
    FROM fact AS f JOIN dim AS d ON f.k = d.k;

SELECT '-- the branch selected by a constant column is estimated as without that filter, the other one as empty';
WITH
    (SELECT extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(f.v) FROM fact AS f JOIN dim AS d ON f.k = d.k WHERE d.y = 1) WHERE explain LIKE '%⋈%') AS alone,
    (SELECT arraySort(groupArray(extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]'))) FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(v) FROM branches WHERE tag = 'a' AND y = 1) WHERE explain LIKE '%⋈%') AS by_string,
    (SELECT arraySort(groupArray(extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]'))) FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(v) FROM branches WHERE b = 1 AND y = 1) WHERE explain LIKE '%⋈%') AS by_cast_integer,
    (SELECT arraySort(groupArray(extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]'))) FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(v) FROM branches WHERE n IS NOT NULL AND y = 1) WHERE explain LIKE '%⋈%') AS by_null,
    (SELECT arraySort(groupArray(extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]'))) FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(v) FROM branches WHERE n IS NULL AND y = 1) WHERE explain LIKE '%⋈%') AS by_is_null
SELECT alone NOT IN ('', '0'), by_string = arraySort(['0', alone]), by_cast_integer = arraySort(['0', alone]), by_null = arraySort(['0', alone]), by_is_null = arraySort(['0', alone]);

SELECT '-- comparisons of materialized constants are decided as they are executed';
WITH
    (SELECT extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(f.v) FROM fact AS f JOIN dim AS d ON f.k = d.k WHERE d.y = 1) WHERE explain LIKE '%⋈%') AS alone,
    (SELECT extract(explain, '\\bd\\[(?:no_stats~)?(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT sum(v) FROM constants WHERE y = 1 AND e < CAST('a', 'Enum8(''b'' = 1, ''a'' = 2)') AND e <= CAST('b', 'Enum8(''b'' = 1, ''a'' = 2)')
            AND i < toUInt64(1) AND NOT (i > toUInt64(1))
            AND dec >= toDecimal32(1.5, 2) AND dt > toDate('2026-01-01') AND fs != toFixedString('ac', 2)) WHERE explain LIKE '%⋈%') AS all_true
SELECT all_true = alone;
SELECT
    (SELECT count() FROM constants WHERE y = 1 AND e < CAST('a', 'Enum8(''b'' = 1, ''a'' = 2)') AND e <= CAST('b', 'Enum8(''b'' = 1, ''a'' = 2)')
        AND i < toUInt64(1) AND NOT (i > toUInt64(1))
        AND dec >= toDecimal32(1.5, 2) AND dt > toDate('2026-01-01') AND fs != toFixedString('ac', 2))
        = (SELECT count() FROM fact AS f JOIN dim AS d ON f.k = d.k WHERE d.y = 1);

DROP VIEW branches;
DROP VIEW constants;
DROP TABLE fact;
DROP TABLE dim;
