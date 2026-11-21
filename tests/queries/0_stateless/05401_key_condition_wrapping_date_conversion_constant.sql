-- A key such as `toDate(ts)` over `DateTime64` or `Date32` stores a wrapped value for an argument outside the
-- `Date` range (`toDate('1969-12-31 12:00:00')` is `2149-06-06`), and `toDateTime` does the same outside the
-- `DateTime` range, so these keys are not ordered like the argument: a range constant must not be pushed
-- through them. `count()` goes through key analysis, `countIf` reads every row; the two must agree.

SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET explain_query_plan_default = 'legacy';  -- for the Granules assertion
SET parallel_replicas_local_plan = 1;       -- for explain with indexes

DROP TABLE IF EXISTS t_order_date, t_partition_date, t_date32, t_order_datetime, t_cast_date, t_equality, t_datetime;

CREATE TABLE t_order_date (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY toDate(ts) SETTINGS index_granularity = 1;
CREATE TABLE t_partition_date (ts DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY tuple();
CREATE TABLE t_date32 (d Date32) ENGINE = MergeTree ORDER BY toDate(d) SETTINGS index_granularity = 1;
CREATE TABLE t_order_datetime (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY toDateTime(ts) SETTINGS index_granularity = 1;
CREATE TABLE t_cast_date (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY CAST(ts AS Date) SETTINGS index_granularity = 1;

-- In `t_date32` and `t_order_datetime` the wrapped key of the `2200` row sorts between the other two.
INSERT INTO t_order_date VALUES ('1969-12-31 12:00:00'), ('1970-01-01 12:00:00');
INSERT INTO t_partition_date VALUES ('1969-12-31 12:00:00'), ('1970-01-01 12:00:00');
INSERT INTO t_date32 VALUES ('2000-01-01'), ('2200-01-01'), ('2030-01-01');
INSERT INTO t_order_datetime VALUES ('2000-01-01 00:00:00'), ('2200-01-01 12:00:00'), ('2080-01-01 00:00:00');
INSERT INTO t_cast_date VALUES ('1969-12-31 12:00:00'), ('1970-01-01 12:00:00');

SELECT count() FROM t_order_date WHERE ts <= '1970-01-02 00:00:00';
SELECT countIf(ts <= '1970-01-02 00:00:00') FROM t_order_date;
SELECT count() FROM t_partition_date WHERE ts <= '1970-01-02 00:00:00';
SELECT countIf(ts <= '1970-01-02 00:00:00') FROM t_partition_date;
SELECT count() FROM t_date32 WHERE d >= '2100-01-01';
SELECT countIf(d >= '2100-01-01') FROM t_date32;
SELECT count() FROM t_order_datetime WHERE ts >= '2100-01-01 00:00:00';
SELECT countIf(ts >= '2100-01-01 00:00:00') FROM t_order_datetime;
SELECT count() FROM t_cast_date WHERE ts <= '1970-01-02 00:00:00';
SELECT countIf(ts <= '1970-01-02 00:00:00') FROM t_cast_date;

-- Equality still prunes: the constant goes through the same wrapping conversion as the stored keys.
CREATE TABLE t_equality (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY toDate(ts) SETTINGS index_granularity = 1;
INSERT INTO t_equality SELECT toDateTime64('2026-01-01 12:00:00', 3, 'UTC') + INTERVAL number DAY FROM numbers(10);
SELECT count() FROM t_equality WHERE ts = '2026-01-05 12:00:00';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_equality WHERE ts = '2026-01-05 12:00:00')
WHERE explain ILIKE '%Granules: %/10' AND explain NOT ILIKE '%Granules: 10/10';

-- A `DateTime` argument cannot wrap, so a range constant is still pushed through `toDate`.
CREATE TABLE t_datetime (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY toDate(dt) SETTINGS index_granularity = 1;
INSERT INTO t_datetime SELECT toDateTime('2026-01-01 12:00:00', 'UTC') + INTERVAL number DAY FROM numbers(10);
SELECT count() FROM t_datetime WHERE dt >= '2026-01-08 00:00:00' SETTINGS force_primary_key = 1;

DROP TABLE t_order_date, t_partition_date, t_date32, t_order_datetime, t_cast_date, t_equality, t_datetime;
