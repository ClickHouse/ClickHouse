-- -0.0 and 0.0 (or two NaN payloads) are one key for FINAL, but a skip index tells them apart: it must not skip the exact-mode recovery
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1, use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_skip_final_float_pk;
CREATE TABLE t_skip_final_float_pk (id UInt64, f Float64, v UInt64,
    INDEX ix_id id TYPE minmax GRANULARITY 1, INDEX ix_f f TYPE set(0) GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);
SYSTEM STOP MERGES t_skip_final_float_pk;
INSERT INTO t_skip_final_float_pk VALUES (1, 0.0, 1);
INSERT INTO t_skip_final_float_pk VALUES (1, -0.0, 2);

SELECT 'signed zeros';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_skip_final_float_pk FINAL WHERE toString(f) = '0') WHERE trimLeft(explain) = 'Granules: 1/2';
SELECT id, f, v FROM t_skip_final_float_pk FINAL WHERE toString(f) = '0' SETTINGS force_data_skipping_indices = 'ix_f';
SELECT id, f, v FROM t_skip_final_float_pk FINAL WHERE toString(f) = '-0';
SELECT 'PrimaryKeyExpand for the float column, not for the integer one';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_skip_final_float_pk FINAL WHERE toString(f) = '0') WHERE explain ILIKE '%PrimaryKeyExpand%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_skip_final_float_pk FINAL WHERE id = 1 SETTINGS force_data_skipping_indices = 'ix_id') WHERE explain ILIKE '%PrimaryKeyExpand%';
DROP TABLE t_skip_final_float_pk;

SELECT 'NaN payloads';
DROP TABLE IF EXISTS t_skip_final_nan_pk;
CREATE TABLE t_skip_final_nan_pk (id UInt64, f Float64, v UInt64, INDEX ix_f f TYPE set(0) GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);
SYSTEM STOP MERGES t_skip_final_nan_pk;
INSERT INTO t_skip_final_nan_pk VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090560)), 1);
INSERT INTO t_skip_final_nan_pk VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090561)), 2);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_skip_final_nan_pk FINAL WHERE reinterpretAsUInt64(f) = 9221120237041090560) WHERE trimLeft(explain) = 'Granules: 1/2';
SELECT id, reinterpretAsUInt64(f), v FROM t_skip_final_nan_pk FINAL WHERE reinterpretAsUInt64(f) = 9221120237041090560 SETTINGS force_data_skipping_indices = 'ix_f';
DROP TABLE t_skip_final_nan_pk;

SELECT 'primary key ranges are kept';
DROP TABLE IF EXISTS t_skip_final_float_pk_ranges;
CREATE TABLE t_skip_final_float_pk_ranges (id UInt64, f Float64, v UInt64, INDEX ix_f f TYPE set(0) GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f) SETTINGS index_granularity = 128, index_granularity_bytes = 10485760;
SYSTEM STOP MERGES t_skip_final_float_pk_ranges;
INSERT INTO t_skip_final_float_pk_ranges SELECT number, 0.0, 1 FROM numbers(10000) SETTINGS max_block_size = 65536, max_insert_block_size = 1048576, min_insert_block_size_rows = 1048576, min_insert_block_size_bytes = 0;
INSERT INTO t_skip_final_float_pk_ranges SELECT number, -0.0, 2 FROM numbers(10000) SETTINGS max_block_size = 65536, max_insert_block_size = 1048576, min_insert_block_size_rows = 1048576, min_insert_block_size_bytes = 0;
SELECT id, f, v FROM t_skip_final_float_pk_ranges FINAL WHERE id = 5000 AND toString(f) = '0' SETTINGS log_comment = '05401_pk_ranges_idx', force_data_skipping_indices = 'ix_f';
SELECT id, f, v FROM t_skip_final_float_pk_ranges FINAL WHERE id = 5000 AND toString(f) = '0' SETTINGS log_comment = '05401_pk_ranges_noidx', use_skip_indexes = 0;
SYSTEM FLUSH LOGS query_log;
SELECT countIf(log_comment = '05401_pk_ranges_idx') > 0 AND countIf(log_comment = '05401_pk_ranges_noidx') > 0
    AND anyIf(read_rows, log_comment = '05401_pk_ranges_idx') = anyIf(read_rows, log_comment = '05401_pk_ranges_noidx')
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05401_pk_ranges_idx', '05401_pk_ranges_noidx');
DROP TABLE t_skip_final_float_pk_ranges;
