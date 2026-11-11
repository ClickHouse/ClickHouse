-- A sparse column of a Wide part with only default values in the part is produced without reading its files.
DROP TABLE IF EXISTS t_sparse_only_defaults;
CREATE TABLE t_sparse_only_defaults (k UInt64, u UInt32, s String, n Nullable(String), d UInt32 DEFAULT 5)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse', index_granularity = 100;
INSERT INTO t_sparse_only_defaults (k, d) SELECT number, 0 FROM numbers(1050);
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_only_defaults' AND active AND column IN ('k', 'u', 's', 'n', 'd')
ORDER BY column;
SELECT sum(u) FROM t_sparse_only_defaults SETTINGS log_comment = '05616_sparse_column_only_defaults_wide_files', enable_parallel_replicas = 0,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['FileOpen'], ProfileEvents['ReadCompressedBytes'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05616_sparse_column_only_defaults_wide_files'
    AND type = 'QueryFinish' AND is_initial_query;
SELECT count(), sum(u), countIf(s != ''), countIf(n IS NOT NULL), sum(d) FROM t_sparse_only_defaults;
SELECT count(), sum(d) FROM (SELECT d FROM t_sparse_only_defaults LIMIT 7 OFFSET 1040);
SELECT k, u, s, n, n.null, d FROM t_sparse_only_defaults WHERE k IN (0, 99, 100, 555, 1049) ORDER BY k;
SELECT count() FROM t_sparse_only_defaults PREWHERE u = 0 WHERE d = 0 SETTINGS max_block_size = 33;
INSERT INTO t_sparse_only_defaults (k, u, s, n, d)
SELECT number + 2000, if(number = 500, 7, 0), if(number = 500, 'a', ''), if(number = 500, '', NULL), 0 FROM numbers(1000);
SELECT k, n FROM t_sparse_only_defaults WHERE k = 2500;
SELECT count(), sum(u), countIf(s != ''), countIf(n IS NOT NULL), sum(d) FROM t_sparse_only_defaults;
DROP TABLE t_sparse_only_defaults;
