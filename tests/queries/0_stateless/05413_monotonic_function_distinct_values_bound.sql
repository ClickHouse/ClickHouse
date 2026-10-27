-- A function that is monotonic on the value range of its argument and returns integers has at most as many
-- values as there are integers between its values at the ends of the range. The dates span 1000 days of the
-- years 2020 to 2022.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 2;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_monotonic_ndv;
CREATE TABLE t_monotonic_ndv (d Date, nd Nullable(Date)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv SELECT toDate('2020-01-01') + number % 1000, if(number % 7 = 0, NULL, toDate('2020-01-01') + number % 1000)
FROM numbers(10000);

SELECT '-- toYear: 3 years';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYear(d) AS y, count() FROM t_monotonic_ndv GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- a decreasing chain: 3 years';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT -toYear(d) AS y, count() FROM t_monotonic_ndv GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- a Nullable argument: 3 years';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYear(nd) AS y, count() FROM t_monotonic_ndv GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- toYYYYMM: the 209 integers from 202001 to 202209 bound the 33 months';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYYYYMM(d) AS m, count() FROM t_monotonic_ndv GROUP BY m)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- toDayOfWeek is not monotonic over the range: the NDV of the dates';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toDayOfWeek(d) AS w, count() FROM t_monotonic_ndv GROUP BY w)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;

SELECT '-- materialize and toNullable keep the values, LowCardinality is looked through: 3 years';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYear(toNullable(materialize(d))) AS y, count() FROM t_monotonic_ndv GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYear(toLowCardinality(d)) AS y, count() FROM t_monotonic_ndv GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;

-- The range of the statistics must cover every value the function gets, otherwise the bound would be too low.
-- The join estimates are not randomized in the queries with joins.
SELECT '-- assumeNotNull exposes the values stored under NULL, which the range does not cover: the NDV of the dates and one more';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYear(assumeNotNull(nd)) AS y, count() FROM t_monotonic_ndv GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- the range of a float column leaves out NaN: the 3 values of x, not the 1 sign of -2 and -1';
DROP TABLE IF EXISTS t_monotonic_ndv_float;
CREATE TABLE t_monotonic_ndv_float (x Float64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv_float SELECT [-2, -1, nan][number % 3 + 1] FROM numbers(30);
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT sign(x) AS s, count() FROM t_monotonic_ndv_float GROUP BY s)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
DROP TABLE t_monotonic_ndv_float;

SELECT '-- a union covers the ranges of both inputs: 2020 and 2021, not the 1 year of the first input';
DROP TABLE IF EXISTS t_monotonic_ndv_2020;
DROP TABLE IF EXISTS t_monotonic_ndv_2021;
CREATE TABLE t_monotonic_ndv_2020 (k UInt64, d Date) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_monotonic_ndv_2021 (k UInt64, d Date) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv_2020 SELECT number, toDate('2020-01-01') + number % 100 FROM numbers(1000);
INSERT INTO t_monotonic_ndv_2021 SELECT number, toDate('2021-01-01') + number % 100 FROM numbers(100);
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(d) AS y, count() FROM (SELECT d FROM t_monotonic_ndv_2020 UNION ALL SELECT d FROM t_monotonic_ndv_2021) GROUP BY y
    SETTINGS query_plan_lift_up_union = 0)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;

SELECT '-- an outer join pads with the default date of 1970, outside the range: the 100 dates, not 1 year';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(r.d) AS y, count() FROM t_monotonic_ndv_2020 AS l LEFT JOIN t_monotonic_ndv_2021 AS r ON l.k = r.k
    GROUP BY y SETTINGS join_use_nulls = 0, query_plan_optimize_join_order_randomize = 0)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- GROUPING SETS fill the key with the default date of 1970, outside the range: the 100 dates, not 1 year';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(d) AS y, count() FROM (SELECT d, count() FROM t_monotonic_ndv_2020 GROUP BY GROUPING SETS ((d), ())) GROUP BY y
    SETTINGS group_by_use_nulls = 0)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
SELECT '-- an anti join fills the columns of the other side with the default date of 1970, outside the range: with the 2021 dates of a union, the NDV of the dates, not 1 year';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(d) AS y, count() FROM
    (
        SELECT r.d AS d FROM t_monotonic_ndv_2020 AS l LEFT ANTI JOIN t_monotonic_ndv_2021 AS r ON l.k = r.k
        UNION ALL
        SELECT d FROM t_monotonic_ndv_2021
    )
    GROUP BY y
    SETTINGS anti_join_include_columns_from_both_sides = 1, join_use_nulls = 0, query_plan_lift_up_union = 0,
        query_plan_optimize_join_order_randomize = 0)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
DROP TABLE t_monotonic_ndv_2020;
DROP TABLE t_monotonic_ndv_2021;

SELECT '-- JOIN USING casts the key to DateTime64(3), whose values the range of DateTime64(0) does not describe: the 2 times, not 1 year';
DROP TABLE IF EXISTS t_monotonic_ndv_s0;
DROP TABLE IF EXISTS t_monotonic_ndv_s3;
CREATE TABLE t_monotonic_ndv_s0 (k DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_monotonic_ndv_s3 (k DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv_s0 VALUES ('2020-01-01 00:00:00'), ('2021-01-01 00:00:00');
INSERT INTO t_monotonic_ndv_s3 VALUES ('2020-01-01 00:00:00'), ('2021-01-01 00:00:00');
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(k) AS y, count() FROM t_monotonic_ndv_s0 INNER JOIN t_monotonic_ndv_s3 USING (k) GROUP BY y
    SETTINGS query_plan_optimize_join_order_randomize = 0)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
DROP TABLE t_monotonic_ndv_s0;
DROP TABLE t_monotonic_ndv_s3;

SELECT '-- FINAL of SummingMergeTree returns sums, which the range of the stored values does not cover: the 2 values, not 1 year';
DROP TABLE IF EXISTS t_monotonic_ndv_sum;
CREATE TABLE t_monotonic_ndv_sum (k UInt64, x UInt64) ENGINE = SummingMergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv_sum SELECT 1, 100 FROM numbers(4) SETTINGS optimize_on_insert = 0;
INSERT INTO t_monotonic_ndv_sum VALUES (2, 101);
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(toDate(x)) AS y, count() FROM t_monotonic_ndv_sum FINAL GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
DROP TABLE t_monotonic_ndv_sum;

SELECT '-- an update that the read applies on the fly changes values the range does not cover: the 2 dates, not 1 year';
DROP TABLE IF EXISTS t_monotonic_ndv_update;
CREATE TABLE t_monotonic_ndv_update (k UInt64, d Date) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv_update VALUES (1, '2020-01-01'), (2, '2020-01-02');
SYSTEM STOP MERGES t_monotonic_ndv_update;
ALTER TABLE t_monotonic_ndv_update UPDATE d = toDate('2021-01-01') WHERE k = 1 SETTINGS mutations_sync = 0;
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1
    SELECT toYear(d) AS y, count() FROM t_monotonic_ndv_update GROUP BY y SETTINGS apply_mutations_on_fly = 1)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
DROP TABLE t_monotonic_ndv_update;

SELECT '-- a pending rename gives x the values of the old y, which the statistics of the old x do not describe: the 2 dates, not 1 year';
DROP TABLE IF EXISTS t_monotonic_ndv_rename;
CREATE TABLE t_monotonic_ndv_rename (k UInt64, x Date, y Date) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_monotonic_ndv_rename VALUES (1, '2020-01-01', '2020-01-01'), (2, '2020-01-02', '2021-01-01');
SYSTEM STOP MERGES t_monotonic_ndv_rename;
ALTER TABLE t_monotonic_ndv_rename RENAME COLUMN x TO z, RENAME COLUMN y TO x SETTINGS alter_sync = 0;
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT toYear(x) AS y, count() FROM t_monotonic_ndv_rename GROUP BY y)
WHERE trimLeft(explain) LIKE 'Aggregating%' LIMIT 1;
DROP TABLE t_monotonic_ndv_rename;

DROP TABLE t_monotonic_ndv;
