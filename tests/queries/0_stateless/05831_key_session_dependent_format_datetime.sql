-- Tags: shard

-- Stored key, index, partition and sharding values are computed with the server settings. A session that changes a
-- setting `formatDateTime` / `parseDateTime` depend on computes other values from the same expression, so no
-- optimization may substitute the stored values for it. With the default `formatdatetime_parsedatetime_m_is_month_name = 1`
-- `%M` is the month name, with 0 it is the minute.

SET formatdatetime_parsedatetime_m_is_month_name = 1;
SET enable_parallel_replicas = 0;
-- Join sharding by primary key ranges needs a plain hash join.
SET query_plan_join_shard_by_pk_ranges = 1, join_algorithm = 'hash', max_threads = 4, max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0, enable_join_runtime_filters = 0, query_plan_join_swap_table = 0,
    query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_sort;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_ja;
DROP TABLE IF EXISTS t_jb;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_dist_view;
DROP TABLE IF EXISTS t_dist_local;
DROP TABLE IF EXISTS t_parse;
DROP TABLE IF EXISTS t_lambda;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_unix;
DROP TABLE IF EXISTS t_final;
DROP TABLE IF EXISTS t_sample;
DROP TABLE IF EXISTS t_folded;
DROP TABLE IF EXISTS t_folded_sample;
DROP TABLE IF EXISTS t_modulo;

-- 12 months x 60 minutes.
CREATE TABLE t_sort (dt DateTime('UTC'), b UInt32) ENGINE = MergeTree ORDER BY (formatDateTime(dt, '%M'), b) SETTINGS index_granularity = 100;
INSERT INTO t_sort SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60, number % 5 FROM numbers(10000);

CREATE TABLE t_part (dt DateTime('UTC'), b UInt32) ENGINE = MergeTree PARTITION BY formatDateTime(dt, '%M') ORDER BY b SETTINGS index_granularity = 100;
INSERT INTO t_part SELECT * FROM t_sort;

CREATE TABLE t_idx
(
    dt DateTime('UTC'),
    b UInt32,
    INDEX i_set formatDateTime(dt, '%M') TYPE set(0) GRANULARITY 1,
    INDEX i_bf formatDateTime(dt, '%M') TYPE bloom_filter GRANULARITY 1,
    INDEX i_mm formatDateTime(dt, '%M') TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_idx SELECT * FROM t_sort ORDER BY intDiv(toMinute(dt), 6), dt;

CREATE TABLE t_ja (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY formatDateTime(dt, '%M') SETTINGS index_granularity = 16;
CREATE TABLE t_jb (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY formatDateTime(dt, '%M') SETTINGS index_granularity = 16;
INSERT INTO t_ja SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60 FROM numbers(1200);
INSERT INTO t_jb SELECT * FROM t_ja;

-- Each shard reads only the rows the sharding key placed on it.
CREATE TABLE t_dist_local (dt DateTime('UTC'), shard UInt32) ENGINE = MergeTree ORDER BY dt;
CREATE VIEW t_dist_view AS SELECT dt FROM t_dist_local WHERE shard = shardNum();
CREATE TABLE t_dist (dt DateTime('UTC')) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_dist_view, cityHash64(formatDateTime(dt, '%M')));
INSERT INTO t_dist_local SELECT dt, cityHash64(formatDateTime(dt, '%M')) % 2 + 1 FROM t_sort;

CREATE TABLE t_parse (s String) ENGINE = MergeTree ORDER BY parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC');
INSERT INTO t_parse VALUES ('2024-May-01'), ('2024-June-02');

CREATE TABLE t_lambda (arr Array(DateTime('UTC'))) ENGINE = MergeTree ORDER BY arrayMap(x -> formatDateTime(x, '%M'), arr);
INSERT INTO t_lambda VALUES (['2024-05-01 00:05:00']), (['2024-06-01 00:06:00']);

CREATE TABLE t_nullable (dt Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY formatDateTime(dt, '%M') SETTINGS allow_nullable_key = 1, index_granularity = 100;
INSERT INTO t_nullable SELECT dt FROM t_sort;
CREATE TABLE t_unix (ts UInt32) ENGINE = MergeTree ORDER BY fromUnixTimestamp(ts, '%M', 'UTC') SETTINGS index_granularity = 100;
INSERT INTO t_unix SELECT toUInt32(dt) FROM t_sort;

CREATE TABLE t_final (dt DateTime('UTC'), b UInt32) ENGINE = ReplacingMergeTree ORDER BY (formatDateTime(dt, '%M'), b, dt) SETTINGS index_granularity = 100;
SYSTEM STOP MERGES t_final;
INSERT INTO t_final SELECT * FROM t_sort;
INSERT INTO t_final SELECT * FROM t_sort;

CREATE TABLE t_sample (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY cityHash64(formatDateTime(dt, '%M'))
    SAMPLE BY cityHash64(formatDateTime(dt, '%M')) SETTINGS index_granularity = 1;
INSERT INTO t_sample SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60 FROM numbers(720);

-- The constant `formatDateTime` call is folded when the key is built.
CREATE TABLE t_folded (s String) ENGINE = MergeTree ORDER BY concat(s, formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))
    SETTINGS index_granularity = 1;
INSERT INTO t_folded SELECT toString(number) FROM numbers(1000);
CREATE TABLE t_folded_sample (s String) ENGINE = MergeTree ORDER BY cityHash64(s, formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))
    SAMPLE BY cityHash64(s, formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M')) SETTINGS index_granularity = 1;
INSERT INTO t_folded_sample SELECT * FROM t_folded;

CREATE TABLE t_modulo (dt DateTime('UTC'), id UInt32) ENGINE = MergeTree PARTITION BY (formatDateTime(dt, '%M'), id % 2) ORDER BY id;

SELECT 'baseline session';
SELECT 'read in order', count() > 0 FROM (EXPLAIN actions = 1 SELECT formatDateTime(dt, '%M') AS k FROM t_sort ORDER BY k SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InOrder%';
SELECT 'primary key', count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May' SETTINGS force_primary_key = 1;
SELECT 'fromUnixTimestamp primary key', count() FROM t_unix WHERE fromUnixTimestamp(ts, '%M', 'UTC') = 'May' SETTINGS force_primary_key = 1;
SELECT 'skip indexes', count() FROM t_idx WHERE formatDateTime(dt, '%M') = 'May' SETTINGS force_data_skipping_indices = 'i_set,i_bf,i_mm';
SELECT 'join sharding', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM t_ja AS a INNER JOIN t_jb AS b ON formatDateTime(a.dt, '%M') = formatDateTime(b.dt, '%M')) WHERE explain ILIKE '%Sharding:%';

-- Which key is not used when one setting deviates: the formatDateTime key, the parseDateTime key.
SELECT 'no setting changed', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%');
SELECT 'formatdatetime_parsedatetime_m_is_month_name', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT 'formatdatetime_f_prints_single_zero', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT 'formatdatetime_f_prints_scale_number_of_digits', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;
SELECT 'formatdatetime_format_without_leading_zeros', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS formatdatetime_format_without_leading_zeros = 1;
SELECT 'formatdatetime_e_with_space_padding', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS formatdatetime_e_with_space_padding = 1;
SELECT 'parsedatetime_parse_without_leading_zeros', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS parsedatetime_parse_without_leading_zeros = 0;
SELECT 'parsedatetime_e_requires_space_padding', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May') WHERE explain ILIKE '%Condition: true%'), (SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime('2024-05-01', 'UTC')) WHERE explain ILIKE '%Condition: true%') SETTINGS parsedatetime_e_requires_space_padding = 1;

SET formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT 'deviating session';

SELECT 'read in order', count() FROM (EXPLAIN actions = 1 SELECT formatDateTime(dt, '%M') AS k FROM t_sort ORDER BY k SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InOrder%';
SELECT 'order by', groupArray(k) = arraySort(groupArray(k)) FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_sort ORDER BY k) SETTINGS optimize_read_in_order = 1, max_threads = 1;
SELECT 'aggregation in order', count() FROM (SELECT formatDateTime(dt, '%M') AS k, b, count() FROM t_sort GROUP BY k, b) SETTINGS optimize_aggregation_in_order = 1;

SELECT 'primary key', count() FROM t_sort WHERE formatDateTime(dt, '%M') = '05';
SELECT count() FROM t_sort WHERE formatDateTime(dt, '%M') = '05' SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT 'partition pruning', count() FROM t_part WHERE formatDateTime(dt, '%M') = '05' SETTINGS optimize_use_implicit_projections = 0;
SELECT 'constant folding', count() FROM t_part WHERE formatDateTime(dt, '%M') = '05' AND b < 3 SETTINGS use_constant_folding_in_index_analysis = 1, optimize_use_implicit_projections = 0;
SELECT 'set index', count() FROM t_idx WHERE formatDateTime(dt, '%M') = '05' SETTINGS ignore_data_skipping_indexes = 'i_bf,i_mm';
SELECT 'bloom filter index', count() FROM t_idx WHERE formatDateTime(dt, '%M') = '05' SETTINGS ignore_data_skipping_indexes = 'i_set,i_mm';
SELECT 'minmax index', count() FROM t_idx WHERE formatDateTime(dt, '%M') = '05' SETTINGS ignore_data_skipping_indexes = 'i_set,i_bf';
SELECT count() FROM t_idx WHERE formatDateTime(dt, '%M') = '05' SETTINGS force_data_skipping_indices = 'i_set'; -- { serverError INDEX_NOT_USED }

SELECT 'min max', min(formatDateTime(dt, '%M')), max(formatDateTime(dt, '%M')) FROM t_sort SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'partition values', count() FROM (SELECT formatDateTime(dt, '%M') AS k, count() FROM t_part GROUP BY k) SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

SELECT 'aggregation per partition', count() FROM (SELECT formatDateTime(dt, '%M') AS k, count() FROM t_part GROUP BY k)
SETTINGS optimize_use_implicit_projections = 0, allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, max_threads = 4;
SELECT 'limit by per partition', count() FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_part LIMIT 1 BY k)
SETTINGS allow_limit_by_partitions_independently = 1, max_threads = 4;
SELECT 'distinct per partition', count() FROM (SELECT DISTINCT formatDateTime(dt, '%M') FROM t_part)
SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_threads = 4;

SELECT 'join sharding', count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_ja AS a INNER JOIN t_jb AS b ON formatDateTime(a.dt, '%M') = formatDateTime(b.dt, '%M')) WHERE explain ILIKE '%Sharding:%';
SELECT 'join', count() FROM t_ja AS a INNER JOIN t_jb AS b ON formatDateTime(a.dt, '%M') = formatDateTime(b.dt, '%M');

SELECT 'shard pruning', count() FROM t_dist WHERE formatDateTime(dt, '%M') = '05' SETTINGS optimize_skip_unused_shards = 1;

-- '2024-May-01' does not parse when `%M` is the minute.
SELECT 'parseDateTime', count() FROM t_parse WHERE parseDateTimeOrZero(s, '%Y-%M-%d', 'UTC') = toDateTime(0, 'UTC');

SELECT 'lambda', count() FROM t_lambda WHERE arrayMap(x -> formatDateTime(x, '%M'), arr) = ['05'];
SELECT 'nullable', count() FROM t_nullable WHERE formatDateTime(dt, '%M') = '05';
SELECT 'fromUnixTimestamp', count() FROM t_unix WHERE fromUnixTimestamp(ts, '%M', 'UTC') = '05';

-- FINAL merges by the stored key, which is the table's own semantics.
SELECT 'final', count() FROM t_final FINAL WHERE formatDateTime(dt, '%M') = '05';
SELECT 'final without index', count() FROM t_final FINAL WHERE formatDateTime(dt, '%M') = '05' SETTINGS use_primary_key = 0;

-- SAMPLE bounds the primary key by the sampling key range even when the primary key is not analyzed.
SELECT 'sample', count() FROM t_sample SAMPLE 0.5;
SELECT 'sample without index', count() FROM t_sample SAMPLE 0.5 SETTINGS use_primary_key = 0;
SELECT 'sample expected', count() FROM t_sample WHERE cityHash64(formatDateTime(dt, '%M')) < 9223372036854775808;

SELECT 'folded min max', min(concat(s, formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))) FROM t_folded
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'folded sample', count() FROM t_folded_sample SAMPLE 0.5;
SELECT 'folded sample without index', count() FROM t_folded_sample SAMPLE 0.5 SETTINGS use_primary_key = 0;
SELECT 'folded sample expected', count() FROM t_folded_sample
WHERE cityHash64(s, formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M')) < 9223372036854775808;

-- A partition key with `modulo` is rebuilt for every INSERT: its values must not follow the inserting session.
INSERT INTO t_modulo VALUES ('2024-05-01 00:05:00', 1);
SELECT 'modulo partition', partition FROM system.parts WHERE database = currentDatabase() AND table = 't_modulo' AND active;
SELECT 'modulo partition pruning', count() FROM t_modulo WHERE formatDateTime(dt, '%M') = 'May' SETTINGS formatdatetime_parsedatetime_m_is_month_name = 1;

SELECT 'distinct in order', count() FROM (SELECT DISTINCT formatDateTime(dt, '%M'), b FROM t_sort) SETTINGS optimize_distinct_in_order = 1, max_threads = 1;
SELECT 'window', sum(c) FROM (SELECT count() OVER (PARTITION BY formatDateTime(dt, '%M')) AS c FROM t_sort)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, optimize_read_in_order = 1;

DROP TABLE t_sort;
DROP TABLE t_part;
DROP TABLE t_idx;
DROP TABLE t_ja;
DROP TABLE t_jb;
DROP TABLE t_dist;
DROP TABLE t_dist_view;
DROP TABLE t_dist_local;
DROP TABLE t_parse;
DROP TABLE t_lambda;
DROP TABLE t_nullable;
DROP TABLE t_unix;
DROP TABLE t_final;
DROP TABLE t_sample;
DROP TABLE t_folded;
DROP TABLE t_folded_sample;
DROP TABLE t_modulo;
