-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the pipeline checks below expect a single local read.

-- An aggregation in order of the primary key is split into one primary key range per thread. Each range is
-- aggregated on its own thread and the streams are not merged, because the ranges hold disjoint keys.

SET max_threads = 4;
-- The tables are small, so a lower threshold per stream keeps four reading streams.
SET merge_tree_min_rows_for_concurrent_read = 1024;
SET merge_tree_min_bytes_for_concurrent_read = 1024;
SET optimize_aggregation_in_order = 1;
SET aggregation_in_order_split_by_primary_key_ranges = 1;
-- The queries below group by up to 100000 distinct keys; a small block budget would make one block per key.
SET aggregation_in_order_max_block_bytes = 50000000;

DROP TABLE IF EXISTS t_agg_ranges;
CREATE TABLE t_agg_ranges (k UInt64, ts UInt64, v Float64)
ENGINE = MergeTree ORDER BY (k, ts)
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_agg_ranges;

INSERT INTO t_agg_ranges SELECT number % 1000, number, number FROM numbers(100000);

SELECT '-- one part: the read is split by ranges of k and the streams are not merged';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') AS merging
FROM (EXPLAIN PIPELINE SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k);

SELECT countIf(explain LIKE '%Skip merging: 1%') > 0 AS skip_merging
FROM (EXPLAIN actions = 1 SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k);

SELECT '-- the setting switches the split off';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') > 0 AS merging
FROM (EXPLAIN PIPELINE SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);

SELECT '-- a key wrapped into a function is not split: one function value can span two ranges';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') > 0 AS merging
FROM (EXPLAIN PIPELINE SELECT intDiv(k, 10), sum(v), count() FROM t_agg_ranges GROUP BY intDiv(k, 10));

SELECT '-- an alias of the key, PREWHERE and WHERE keep the split';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges
FROM (EXPLAIN PIPELINE SELECT k AS k2, sum(v), count() FROM t_agg_ranges GROUP BY k2);
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges
FROM (EXPLAIN PIPELINE SELECT k, sum(v), count() FROM t_agg_ranges PREWHERE v > 100 WHERE ts > 10 GROUP BY k);

SELECT '-- one thread: no split';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges
FROM (EXPLAIN PIPELINE SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k SETTINGS max_threads = 1);

SELECT '-- one part: results';
SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k ORDER BY k LIMIT 3;
SELECT count(), sum(c), sum(s) FROM (SELECT k, count() AS c, sum(v) AS s FROM t_agg_ranges GROUP BY k);
SELECT count(), sum(c), sum(s) FROM (SELECT k, ts, count() AS c, sum(v) AS s FROM t_agg_ranges GROUP BY k, ts);
SELECT count(), sum(c), sum(s) FROM (SELECT intDiv(k, 10) AS d, count() AS c, sum(v) AS s FROM t_agg_ranges GROUP BY d);

SELECT '-- one part: the results do not depend on the split';
SELECT (SELECT groupArray((k, s, c)) FROM (SELECT k, sum(v) AS s, count() AS c FROM t_agg_ranges GROUP BY k ORDER BY k))
     = (SELECT groupArray((k, s, c)) FROM (SELECT k, sum(v) AS s, count() AS c FROM t_agg_ranges GROUP BY k ORDER BY k) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT sum(cityHash64(k, ts, s)) FROM (SELECT k, ts, sum(v) AS s FROM t_agg_ranges GROUP BY k, ts))
     = (SELECT sum(cityHash64(k, ts, s)) FROM (SELECT k, ts, sum(v) AS s FROM t_agg_ranges GROUP BY k, ts) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT groupArray((d, s)) FROM (SELECT intDiv(k, 10) AS d, sum(v) AS s FROM t_agg_ranges GROUP BY d ORDER BY d))
     = (SELECT groupArray((d, s)) FROM (SELECT intDiv(k, 10) AS d, sum(v) AS s FROM t_agg_ranges GROUP BY d ORDER BY d) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT groupArray((ts, s)) FROM (SELECT ts, sum(v) AS s FROM t_agg_ranges WHERE k = 7 GROUP BY ts ORDER BY ts))
     = (SELECT groupArray((ts, s)) FROM (SELECT ts, sum(v) AS s FROM t_agg_ranges WHERE k = 7 GROUP BY ts ORDER BY ts) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges PREWHERE v > 100 WHERE ts > 10 GROUP BY k ORDER BY k))
     = (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges PREWHERE v > 100 WHERE ts > 10 GROUP BY k ORDER BY k) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k HAVING s > 5000000 ORDER BY k))
     = (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k HAVING s > 5000000 ORDER BY k) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT sum(cityHash64(k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k WITH TOTALS))
     = (SELECT sum(cityHash64(k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k WITH TOTALS) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT sum(cityHash64(k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k WITH ROLLUP))
     = (SELECT sum(cityHash64(k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k WITH ROLLUP) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);

-- More parts with overlapping key ranges, so every range of k is spread over all parts.
INSERT INTO t_agg_ranges SELECT number % 1000, number + 100000, number FROM numbers(100000);
INSERT INTO t_agg_ranges SELECT number % 500, number + 200000, number FROM numbers(50000);
INSERT INTO t_agg_ranges SELECT 500 + number % 500, number + 300000, number FROM numbers(50000);

SELECT '-- several parts: the read is split by ranges of k and the streams are not merged';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') AS merging
FROM (EXPLAIN PIPELINE SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k);

SELECT '-- several parts: results';
SELECT k, sum(v), count() FROM t_agg_ranges GROUP BY k ORDER BY k LIMIT 3;
SELECT count(), sum(c), sum(s) FROM (SELECT k, count() AS c, sum(v) AS s FROM t_agg_ranges GROUP BY k);
SELECT count(), sum(c), sum(s) FROM (SELECT k, ts, count() AS c, sum(v) AS s FROM t_agg_ranges GROUP BY k, ts);
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_agg_ranges WHERE k BETWEEN 200 AND 700 GROUP BY k);
SELECT count() FROM (SELECT k, count() AS c FROM t_agg_ranges WHERE k > 1000 GROUP BY k);

SELECT '-- several parts: the results do not depend on the split';
SELECT (SELECT groupArray((k, s, c)) FROM (SELECT k, sum(v) AS s, count() AS c FROM t_agg_ranges GROUP BY k ORDER BY k))
     = (SELECT groupArray((k, s, c)) FROM (SELECT k, sum(v) AS s, count() AS c FROM t_agg_ranges GROUP BY k ORDER BY k) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT sum(cityHash64(k, ts, s)) FROM (SELECT k, ts, sum(v) AS s FROM t_agg_ranges GROUP BY k, ts))
     = (SELECT sum(cityHash64(k, ts, s)) FROM (SELECT k, ts, sum(v) AS s FROM t_agg_ranges GROUP BY k, ts) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges WHERE k BETWEEN 200 AND 700 GROUP BY k ORDER BY k))
     = (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges WHERE k BETWEEN 200 AND 700 GROUP BY k ORDER BY k) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);
SELECT (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k ORDER BY k LIMIT 10))
     = (SELECT groupArray((k, s)) FROM (SELECT k, sum(v) AS s FROM t_agg_ranges GROUP BY k ORDER BY k LIMIT 10) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);

DROP TABLE t_agg_ranges;

-- A UUID key, as in the samples table of a TimeSeries table.
DROP TABLE IF EXISTS t_agg_ranges_uuid;
CREATE TABLE t_agg_ranges_uuid (id UUID, ts UInt64, v Float64)
ENGINE = MergeTree ORDER BY (id, ts)
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_agg_ranges_uuid;

INSERT INTO t_agg_ranges_uuid SELECT toUUID(concat('00000000-0000-0000-0000-', leftPad(hex(number % 1000), 12, '0'))), number, number FROM numbers(100000);
INSERT INTO t_agg_ranges_uuid SELECT toUUID(concat('00000000-0000-0000-0000-', leftPad(hex(number % 1000), 12, '0'))), number + 100000, number FROM numbers(100000);

SELECT '-- UUID key: the read is split by ranges of id and the streams are not merged';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') AS merging
FROM (EXPLAIN PIPELINE SELECT id, sum(v), count() FROM t_agg_ranges_uuid GROUP BY id);

SELECT '-- UUID key: results';
SELECT id, sum(v), count() FROM t_agg_ranges_uuid GROUP BY id ORDER BY id LIMIT 3;
SELECT count(), sum(c), sum(s) FROM (SELECT id, count() AS c, sum(v) AS s FROM t_agg_ranges_uuid GROUP BY id);
SELECT (SELECT groupArray((id, s, c)) FROM (SELECT id, sum(v) AS s, count() AS c FROM t_agg_ranges_uuid GROUP BY id ORDER BY id))
     = (SELECT groupArray((id, s, c)) FROM (SELECT id, sum(v) AS s, count() AS c FROM t_agg_ranges_uuid GROUP BY id ORDER BY id) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);

DROP TABLE t_agg_ranges_uuid;

-- A floating point key cannot be split into ranges, the aggregation keeps the merge of the streams.
DROP TABLE IF EXISTS t_agg_ranges_float;
CREATE TABLE t_agg_ranges_float (f Float64, ts UInt64, v Float64)
ENGINE = MergeTree ORDER BY (f, ts)
SETTINGS index_granularity = 128;

INSERT INTO t_agg_ranges_float SELECT number % 1000, number, number FROM numbers(100000);

SELECT '-- Float64 key: no split';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') > 0 AS merging
FROM (EXPLAIN PIPELINE SELECT f, sum(v), count() FROM t_agg_ranges_float GROUP BY f);
SELECT count(), sum(c), sum(s) FROM (SELECT f, count() AS c, sum(v) AS s FROM t_agg_ranges_float GROUP BY f);

DROP TABLE t_agg_ranges_float;
