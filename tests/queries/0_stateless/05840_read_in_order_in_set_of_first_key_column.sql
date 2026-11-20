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

SELECT 'results';
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC LIMIT 25))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts DESC LIMIT 25 SETTINGS optimize_read_in_order = 0));
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('0', '1', '2', '3', '4', '5', '6', 'none') ORDER BY ts LIMIT 500))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('0', '1', '2', '3', '4', '5', '6', 'none') ORDER BY ts LIMIT 500 SETTINGS optimize_read_in_order = 0));
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '2') AND v % 3 = 0 ORDER BY ts DESC LIMIT 40))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '2') AND v % 3 = 0 ORDER BY ts DESC LIMIT 40 SETTINGS optimize_read_in_order = 0));
SELECT
    (SELECT groupArray((ts, v)) FROM (SELECT ts, v FROM t_in_set_order WHERE k IN ('2', '4') ORDER BY ts LIMIT 30 SETTINGS max_block_size = 7))
  = (SELECT groupArray((ts, v)) FROM (SELECT ts, v FROM t_in_set_order WHERE k IN ('2', '4') ORDER BY ts LIMIT 30 SETTINGS optimize_read_in_order = 0));
SELECT
    (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts LIMIT 5, 20))
  = (SELECT groupArray((k, ts, v)) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', '3', '5') ORDER BY ts LIMIT 5, 20 SETTINGS optimize_read_in_order = 0));
SELECT
    (SELECT arraySort(groupArray((k, ts, v))) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', 'x', 'y') ORDER BY ts LIMIT 5 WITH TIES))
  = (SELECT arraySort(groupArray((k, ts, v))) FROM (SELECT k, ts, v FROM t_in_set_order WHERE k IN ('1', 'x', 'y') ORDER BY ts LIMIT 5 WITH TIES SETTINGS optimize_read_in_order = 0));

DROP TABLE t_in_set_order;
