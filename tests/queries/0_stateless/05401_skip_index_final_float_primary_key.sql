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
SELECT id, f, v FROM t_skip_final_float_pk FINAL WHERE toString(f) = '0';
SELECT id, f, v FROM t_skip_final_float_pk FINAL WHERE toString(f) = '-0';
SELECT 'PrimaryKeyExpand for the float column, not for the integer one';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_skip_final_float_pk FINAL WHERE toString(f) = '0') WHERE explain ILIKE '%PrimaryKeyExpand%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_skip_final_float_pk FINAL WHERE id = 1) WHERE explain ILIKE '%PrimaryKeyExpand%';
DROP TABLE t_skip_final_float_pk;

SELECT 'NaN payloads';
DROP TABLE IF EXISTS t_skip_final_nan_pk;
CREATE TABLE t_skip_final_nan_pk (id UInt64, f Float64, v UInt64, INDEX ix_f f TYPE set(0) GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY (id, f);
SYSTEM STOP MERGES t_skip_final_nan_pk;
INSERT INTO t_skip_final_nan_pk VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090560)), 1);
INSERT INTO t_skip_final_nan_pk VALUES (1, reinterpretAsFloat64(toUInt64(9221120237041090561)), 2);
SELECT id, reinterpretAsUInt64(f), v FROM t_skip_final_nan_pk FINAL WHERE reinterpretAsUInt64(f) = 9221120237041090560;
DROP TABLE t_skip_final_nan_pk;
