-- The String values a PREWHERE filter rejects at the start of a granule are not read when the column has a size stream:
-- `t_size` takes that path, `t_single` (sizes inline) reads them as before, and both must return the same rows.
-- `t_sparse` stores `flag` sparse, so the filter of `PREWHERE flag` is sparse.

SET use_columns_cache = 0, use_query_condition_cache = 0, use_uncompressed_cache = 0, max_threads = 1;

DROP TABLE IF EXISTS t_size;
DROP TABLE IF EXISTS t_single;
DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t_size (id UInt64, n UInt32, flag UInt8, short String, s String CODEC(LZ4), z String CODEC(ZSTD(1)), ns Nullable(String), arr Array(String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', ratio_of_defaults_for_sparse_serialization = 1,
    min_compress_block_size = 65536, max_compress_block_size = 65536, auto_statistics_types = '';

CREATE TABLE t_single (id UInt64, n UInt32, flag UInt8, short String, s String CODEC(LZ4), z String CODEC(ZSTD(1)), ns Nullable(String), arr Array(String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', serialization_info_version = 'with_types',
    string_serialization_version = 'single_stream', ratio_of_defaults_for_sparse_serialization = 1,
    min_compress_block_size = 65536, max_compress_block_size = 65536, auto_statistics_types = '';

CREATE TABLE t_sparse (id UInt64, n UInt32, flag UInt8, short String, s String CODEC(LZ4), z String CODEC(ZSTD(1)), ns Nullable(String), arr Array(String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', ratio_of_defaults_for_sparse_serialization = 0.9,
    min_compress_block_size = 65536, max_compress_block_size = 65536, auto_statistics_types = '';

-- About 260 bytes per value of `s` and `z`, so a 64 KiB compressed block holds about 250 values; in granule 3 the first
-- 1000 values of `s` are one byte each.
INSERT INTO t_size SELECT
    number,
    number % 7,
    number % 8192 IN (3000, 8100),
    toString(number),
    if(number >= 3 * 8192 AND number < 3 * 8192 + 1000, 'x', concat(toString(number), ':', repeat('x', 250))),
    concat(repeat('y', 250), ':', toString(number)),
    if(number % 5 = 0, NULL, toString(number)),
    [toString(number), 'a']
FROM numbers(4 * 8192);

INSERT INTO t_single SELECT * FROM t_size;
INSERT INTO t_sparse SELECT * FROM t_size;
OPTIMIZE TABLE t_size FINAL;
OPTIMIZE TABLE t_single FINAL;
OPTIMIZE TABLE t_sparse FINAL;

SELECT table, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND active AND column = 'flag' ORDER BY table;

SELECT 'one row in each granule';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id IN (5000, 12000, 20000, 30000) ORDER BY id SETTINGS log_comment = '05898_q1_size';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id IN (5000, 12000, 20000, 30000) ORDER BY id SETTINGS log_comment = '05898_q1_single';

SELECT 'the last row of a granule, then a row inside the next one';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id IN (8191, 13192) ORDER BY id SETTINGS log_comment = '05898_q2_size';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id IN (8191, 13192) ORDER BY id SETTINGS log_comment = '05898_q2_single';

SELECT 'the first row of a granule';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id IN (8192, 15192) ORDER BY id;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id IN (8192, 15192) ORDER BY id;

SELECT 'granules split across blocks';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id IN (5000, 12000, 20000, 30000) ORDER BY id SETTINGS max_block_size = 1000;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id IN (5000, 12000, 20000, 30000) ORDER BY id SETTINGS max_block_size = 1000;

SELECT 'several PREWHERE steps';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id % 4096 = 1500 AND n >= 0 ORDER BY id SETTINGS enable_multiple_prewhere_read_steps = 1, move_all_conditions_to_prewhere = 1;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id % 4096 = 1500 AND n >= 0 ORDER BY id SETTINGS enable_multiple_prewhere_read_steps = 1, move_all_conditions_to_prewhere = 1;

SELECT 'lazy materialization';
SELECT id, length(s), cityHash64(s), cityHash64(z) FROM t_size ORDER BY n DESC, id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, log_comment = '05898_q6_size';
SELECT id, length(s), cityHash64(s), cityHash64(z) FROM t_single ORDER BY n DESC, id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, log_comment = '05898_q6_single';

SELECT 'uncompressed cache';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id IN (5000, 12000, 20000, 30000) ORDER BY id SETTINGS use_uncompressed_cache = 1;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id IN (5000, 12000, 20000, 30000) ORDER BY id SETTINGS use_uncompressed_cache = 1;

SELECT 'filter of a sparse column';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_sparse PREWHERE flag ORDER BY id;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE flag ORDER BY id;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE flag ORDER BY id;

SELECT 'with the size subcolumn';
SELECT id, left(s, 8), cityHash64(s), length(s) FROM t_size PREWHERE id IN (5000, 30000) ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id, left(s, 8), cityHash64(s), length(s) FROM t_single PREWHERE id IN (5000, 30000) ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'filter that keeps most rows';
SELECT count(), sum(length(s)), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_size PREWHERE id % 8192 >= 3000;
SELECT count(), sum(length(s)), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_single PREWHERE id % 8192 >= 3000;

SELECT 'short rejected values';
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_size PREWHERE id = 3 * 8192 + 1000;
SELECT id, length(s), cityHash64(s), cityHash64(z), left(s, 8), right(z, 8), ns, arr, short FROM t_single PREWHERE id = 3 * 8192 + 1000;

-- The skipped values are counted as read.
SYSTEM FLUSH LOGS query_log;
SELECT splitByChar('_', log_comment)[2] AS query,
    abs(toInt64(anyIf(read_bytes, endsWith(log_comment, '_size'))) - toInt64(anyIf(read_bytes, endsWith(log_comment, '_single'))))
        <= anyIf(read_bytes, endsWith(log_comment, '_single')) / 100
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05898\_q%'
GROUP BY query ORDER BY query;

DROP TABLE t_size;
DROP TABLE t_single;
DROP TABLE t_sparse;
