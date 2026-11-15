-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- no-parallel: the hit and miss counts below depend on the server-wide columns cache, which
-- `SYSTEM DROP COLUMNS CACHE` and the admission control of the cache share with other tests.

-- The columns cache holds the columns of compact parts, as it does for wide parts: a granule of
-- a column is served from the cache or read from the part independently of the other columns
-- and granules, and every result is the same as without the cache.

SET max_threads = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_cc_compact;

CREATE TABLE t_cc_compact
(
    id UInt64,
    v UInt64,
    s String,
    n Nullable(UInt32),
    arr Array(UInt16),
    t Tuple(a UInt8, b String),
    sparse UInt64,
    d Dynamic
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000, index_granularity = 8192, index_granularity_bytes = '1G',
    ratio_of_defaults_for_sparse_serialization = 0.9;

-- 12 granules of about 8192 rows make 2 stripes.
INSERT INTO t_cc_compact SELECT
    number, number % 10, toString(number), if(number % 3 = 0, NULL, number), range(number % 5), (number % 7, toString(number % 11)),
    if(number % 100 = 0, number, 0), if(number % 2 = 0, number::Dynamic, toString(number)::Dynamic)
FROM numbers(100000);

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_cc_compact' AND active;

SYSTEM DROP COLUMNS CACHE;

-- A full scan writes the entries of every column it reads: one entry per stripe per column.
SELECT sum(v), sum(length(s)) FROM t_cc_compact SETTINGS use_columns_cache = 1, log_comment = 'cc_compact_write';
SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_compact' GROUP BY column ORDER BY column;

-- The same scan is served from the cache.
SELECT sum(v), sum(length(s)) FROM t_cc_compact SETTINGS use_columns_cache = 1, log_comment = 'cc_compact_hit';

-- Some columns are cached, others are read from the part.
SELECT sum(v), sum(length(s)), sum(n), sum(length(arr)), sum(t.a), sum(length(t.b)), sum(sparse), count(d)
FROM t_cc_compact SETTINGS use_columns_cache = 1, log_comment = 'cc_compact_mixed';

-- Now everything is cached.
SELECT sum(v), sum(length(s)), sum(n), sum(length(arr)), sum(t.a), sum(length(t.b)), sum(sparse), count(d)
FROM t_cc_compact SETTINGS use_columns_cache = 1, log_comment = 'cc_compact_all_hit';

-- The same without the cache.
SELECT sum(v), sum(length(s)), sum(n), sum(length(arr)), sum(t.a), sum(length(t.b)), sum(sparse), count(d)
FROM t_cc_compact SETTINGS use_columns_cache = 0;

-- Subcolumns, filters and several mark ranges, with and without the cache.
SELECT sum(n.null), sum(arr.size0), sum(t.a), countIf(d.UInt64 IS NOT NULL), countIf(d.String IS NOT NULL)
FROM t_cc_compact SETTINGS use_columns_cache = 1;
SELECT sum(n.null), sum(arr.size0), sum(t.a), countIf(d.UInt64 IS NOT NULL), countIf(d.String IS NOT NULL)
FROM t_cc_compact SETTINGS use_columns_cache = 1;
SELECT sum(n.null), sum(arr.size0), sum(t.a), countIf(d.UInt64 IS NOT NULL), countIf(d.String IS NOT NULL)
FROM t_cc_compact SETTINGS use_columns_cache = 0;

SELECT count(), sum(length(s)), sum(sparse), groupBitXor(cityHash64(d)) FROM t_cc_compact WHERE v = 3 SETTINGS use_columns_cache = 1;
SELECT count(), sum(length(s)), sum(sparse), groupBitXor(cityHash64(d)) FROM t_cc_compact WHERE v = 3 SETTINGS use_columns_cache = 0;

SELECT count(), sum(v), sum(length(s)), sum(n) FROM t_cc_compact WHERE id < 1000 OR id BETWEEN 30000 AND 41000 OR id > 99000 SETTINGS use_columns_cache = 1;
SELECT count(), sum(v), sum(length(s)), sum(n) FROM t_cc_compact WHERE id < 1000 OR id BETWEEN 30000 AND 41000 OR id > 99000 SETTINGS use_columns_cache = 0;

SELECT id, v, s, n, arr, t, sparse, d FROM t_cc_compact WHERE id IN (0, 8191, 8192, 65535, 65536, 99999) ORDER BY id SETTINGS use_columns_cache = 1;

-- A column added after the part was written is not in the part and has no entries.
ALTER TABLE t_cc_compact ADD COLUMN added UInt64 DEFAULT v * 2;
SELECT sum(added), sum(v) FROM t_cc_compact SETTINGS use_columns_cache = 1;
SELECT sum(added), sum(v) FROM t_cc_compact SETTINGS use_columns_cache = 1;
SELECT countIf(column = 'added') FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_compact';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] AS hits,
    ProfileEvents['ColumnsCacheMisses'] AS misses
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'cc_compact_%'
ORDER BY event_time_microseconds;

DROP TABLE t_cc_compact;
