-- Reading in order when the first sorting key column is in a set of constants (issue #61213).

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, read_in_order_in_set_max_streams = 100;

DROP TABLE IF EXISTS t_in_set_order;

-- Tiny granules: the granules at the borders of the values hold several values of `k`.
CREATE TABLE t_in_set_order (k LowCardinality(String), ts DateTime64(9), v UInt64)
ENGINE = MergeTree ORDER BY (k, ts) SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_in_set_order;
INSERT INTO t_in_set_order SELECT toString(number % 7), toDateTime64('2026-01-01 00:00:00', 9) + toIntervalNanosecond((number * 7919) % 1000003 * 1000), number FROM numbers(30000);
INSERT INTO t_in_set_order SELECT toString(number % 3), toDateTime64('2026-01-01 00:00:00', 9) + toIntervalNanosecond((number * 7919) % 1000003 * 1000 + 1), number + 100000 FROM numbers(10000);
INSERT INTO t_in_set_order SELECT if(number < 5, '1', '2'), toDateTime64('2026-01-01 00:00:00', 9) + toIntervalNanosecond(number * 1000 + 2), number + 300000 FROM numbers(10);
-- Equal timestamps in two values.
INSERT INTO t_in_set_order SELECT if(number % 2, 'x', 'y'), toDateTime64('2025-12-31 00:00:00', 9) + toIntervalSecond(intDiv(number, 2)), number + 400000 FROM numbers(20);

SELECT 'plan';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC LIMIT 10)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts LIMIT 5, 20)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC LIMIT 10 SETTINGS read_in_order_in_set_max_streams = 0)
WHERE explain ILIKE '%Read in order by values%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC)
WHERE explain ILIKE '%Read in order by values%';
-- A fixed ORDER BY column before the first key column.
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE v = 7 AND k IN ('1', '3', '5') ORDER BY v, k, ts LIMIT 10)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%' OR explain ILIKE '%Prefix sort description%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE v = 7 AND k IN ('1', '3', '5') ORDER BY v, cityHash64(ts) LIMIT 10)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%' OR explain ILIKE '%Prefix sort description%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE v = 7 AND k IN ('1', '3', '5') ORDER BY v, ts LIMIT 10)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%' OR explain ILIKE '%Prefix sort description%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE v = 7 AND k IN ('1', '3', '5') ORDER BY v, k, ts LIMIT 10 SETTINGS read_in_order_in_set_max_streams = 0)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%' OR explain ILIKE '%Prefix sort description%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, ts FROM t_in_set_order WHERE v = 7 AND k IN ('1', '3', '5') ORDER BY v, cityHash64(ts) LIMIT 10 SETTINGS read_in_order_in_set_max_streams = 0)
WHERE explain ILIKE '%Read in order by values%' OR explain ILIKE '%ReadType%' OR explain ILIKE '%Prefix sort description%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts, v FROM t_in_set_order WHERE k IN ('0', '1', '2', '3', '4', '5', '6', 'none') ORDER BY ts LIMIT 500)
WHERE explain ILIKE '%Read in order by values%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '2') AND v % 3 = 0 ORDER BY ts DESC LIMIT 40)
WHERE explain ILIKE '%Read in order by values%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT ts, v FROM t_in_set_order WHERE k IN ('2', '4') ORDER BY ts LIMIT 30 SETTINGS max_block_size = 7)
WHERE explain ILIKE '%Read in order by values%';
-- WITH TIES keeps the usual plan.
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', 'x', 'y') ORDER BY ts LIMIT 5 WITH TIES)
WHERE explain ILIKE '%Read in order by values%';

SELECT 'results';
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC LIMIT 25))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC LIMIT 25) SETTINGS optimize_read_in_order = 0);
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('0', '1', '2', '3', '4', '5', '6', 'none') ORDER BY ts LIMIT 500))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('0', '1', '2', '3', '4', '5', '6', 'none') ORDER BY ts LIMIT 500) SETTINGS optimize_read_in_order = 0);
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '2') AND v % 3 = 0 ORDER BY ts DESC LIMIT 40))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '2') AND v % 3 = 0 ORDER BY ts DESC LIMIT 40) SETTINGS optimize_read_in_order = 0);
SELECT
    (SELECT groupArray((ts, v)) FROM (SELECT ts, v FROM t_in_set_order WHERE k IN ('2', '4') ORDER BY ts LIMIT 30) SETTINGS max_block_size = 7)
  = (SELECT groupArray((ts, v)) FROM (SELECT ts, v FROM t_in_set_order WHERE k IN ('2', '4') ORDER BY ts LIMIT 30) SETTINGS optimize_read_in_order = 0, max_block_size = 7);
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts LIMIT 5, 20))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts LIMIT 5, 20) SETTINGS optimize_read_in_order = 0);
SELECT
    (SELECT arraySort(groupArray((k, ts, v))) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', 'x', 'y') ORDER BY ts LIMIT 5 WITH TIES))
  = (SELECT arraySort(groupArray((k, ts, v))) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', 'x', 'y') ORDER BY ts LIMIT 5 WITH TIES) SETTINGS optimize_read_in_order = 0);

SELECT 'filter column name';
-- A column of the read with the name of the per-stream filter column.
DROP TABLE IF EXISTS t_in_set_order_name;
CREATE TABLE t_in_set_order_name (k String, ts UInt64, __read_in_order_key_value_filter String)
ENGINE = MergeTree ORDER BY (k, ts) SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_in_set_order_name SELECT toString(number % 3), number, toString(number) FROM numbers(3000);

SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts, __read_in_order_key_value_filter FROM t_in_set_order_name WHERE k IN ('1', '2') ORDER BY ts DESC LIMIT 20)
WHERE explain ILIKE '%Read in order by values%';
SELECT
    (SELECT groupArray((k, ts, __read_in_order_key_value_filter)) FROM (SELECT k, ts, __read_in_order_key_value_filter FROM t_in_set_order_name WHERE k IN ('1', '2') ORDER BY ts DESC LIMIT 20))
  = (SELECT groupArray((k, ts, __read_in_order_key_value_filter)) FROM (SELECT k, ts, __read_in_order_key_value_filter FROM t_in_set_order_name WHERE k IN ('1', '2') ORDER BY ts DESC LIMIT 20) SETTINGS optimize_read_in_order = 0);
SELECT
    (SELECT groupArray((k, ts, __read_in_order_key_value_filter)) FROM (SELECT k, ts, __read_in_order_key_value_filter FROM t_in_set_order_name WHERE k IN ('1', '2') AND __read_in_order_key_value_filter != 'z' ORDER BY ts DESC LIMIT 20))
  = (SELECT groupArray((k, ts, __read_in_order_key_value_filter)) FROM (SELECT k, ts, __read_in_order_key_value_filter FROM t_in_set_order_name WHERE k IN ('1', '2') AND __read_in_order_key_value_filter != 'z' ORDER BY ts DESC LIMIT 20) SETTINGS optimize_read_in_order = 0);

-- The first key column with the name of the per-stream filter column.
DROP TABLE IF EXISTS t_in_set_order_key_name;
CREATE TABLE t_in_set_order_key_name (__read_in_order_key_value_filter String, ts UInt64)
ENGINE = MergeTree ORDER BY (__read_in_order_key_value_filter, ts) SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_in_set_order_key_name SELECT toString(number % 3), number FROM numbers(3000);

SELECT count() FROM (EXPLAIN actions = 1 SELECT __read_in_order_key_value_filter, ts FROM t_in_set_order_key_name WHERE __read_in_order_key_value_filter IN ('1', '2') ORDER BY ts LIMIT 20)
WHERE explain ILIKE '%Read in order by values%';
SELECT
    (SELECT groupArray((__read_in_order_key_value_filter, ts)) FROM (SELECT __read_in_order_key_value_filter, ts FROM t_in_set_order_key_name WHERE __read_in_order_key_value_filter IN ('1', '2') ORDER BY ts LIMIT 20))
  = (SELECT groupArray((__read_in_order_key_value_filter, ts)) FROM (SELECT __read_in_order_key_value_filter, ts FROM t_in_set_order_key_name WHERE __read_in_order_key_value_filter IN ('1', '2') ORDER BY ts LIMIT 20) SETTINGS optimize_read_in_order = 0);

SELECT 'projection';
DROP TABLE IF EXISTS t_in_set_order_proj;
CREATE TABLE t_in_set_order_proj (k String, ts UInt64, v UInt64, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY (k, ts) SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_in_set_order_proj SELECT toString(number % 3), number, intDiv(number, 100) FROM numbers(30000);
-- A projection reading fewer granules is chosen as without the split; without it the split is taken.
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts, v FROM t_in_set_order_proj WHERE k IN ('1', '2') AND v = 1 ORDER BY ts LIMIT 2 SETTINGS optimize_use_projections = 1)
WHERE explain ILIKE '%Read in order by values%';
SELECT k, ts, v FROM t_in_set_order_proj WHERE k IN ('1', '2') AND v = 1 ORDER BY ts LIMIT 2 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, ts, v FROM t_in_set_order_proj WHERE k IN ('1', '2') ORDER BY ts LIMIT 2 SETTINGS optimize_use_projections = 1)
WHERE explain ILIKE '%Read in order by values%';

DROP TABLE t_in_set_order_name;
DROP TABLE t_in_set_order_key_name;
DROP TABLE t_in_set_order_proj;
DROP TABLE t_in_set_order;
