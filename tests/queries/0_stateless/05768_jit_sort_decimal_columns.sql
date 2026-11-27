-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

DROP TABLE IF EXISTS t_jit_sort_dec;
DROP TABLE IF EXISTS t_jit_sort_dt64_mt;

CREATE TABLE t_jit_sort_dec
(
    k UInt8,
    d32 Decimal32(2),
    d64 Decimal64(4),
    dt DateTime64(3, 'UTC'),
    t64 Time64(3),
    nd Nullable(Decimal64(2)),
    x UInt32
) ENGINE = Memory;

-- One row per INSERT: every comparison happens while merging the sorted blocks.
INSERT INTO t_jit_sort_dec VALUES (1, 3.25, -0.0001, '2026-01-01 00:00:00.001', '10:00:00.250', 1.5, 1);
INSERT INTO t_jit_sort_dec VALUES (1, -12.5, 7.5, '1969-12-31 23:59:59.999', '-01:00:00.500', NULL, 2);
INSERT INTO t_jit_sort_dec VALUES (0, 0, -7.5, '1970-01-01 00:00:00.000', '00:00:00.000', -2.25, 3);
INSERT INTO t_jit_sort_dec VALUES (1, 0, 0, '1900-01-01 00:00:00.000', '00:00:00.001', 0, 4);
INSERT INTO t_jit_sort_dec VALUES (0, -0.01, 1234.5678, '2026-01-01 00:00:00.001', '-00:00:00.001', NULL, 5);
INSERT INTO t_jit_sort_dec VALUES (0, 99.99, -1234.5678, '1969-12-31 23:59:59.999', '999:59:59.999', -0.01, 6);
INSERT INTO t_jit_sort_dec VALUES (1, -12.5, -0.0001, '2299-12-31 23:59:59.999', '-999:59:59.999', 3, 7);
INSERT INTO t_jit_sort_dec VALUES (0, 3.25, 0, '1970-01-01 00:00:00.000', '10:00:00.250', -2.25, 8);

SELECT '--- k, d32 ---';
SELECT k, d32, x FROM t_jit_sort_dec ORDER BY k, d32, x;
SELECT '--- k, d64 DESC ---';
SELECT k, d64, x FROM t_jit_sort_dec ORDER BY k, d64 DESC, x;
SELECT '--- k, dt ---';
SELECT k, dt, x FROM t_jit_sort_dec ORDER BY k, dt, x;
SELECT '--- k DESC, t64 ---';
SELECT k, t64, x FROM t_jit_sort_dec ORDER BY k DESC, t64, x;
SELECT '--- nd NULLS FIRST ---';
SELECT nd, x FROM t_jit_sort_dec ORDER BY nd ASC NULLS FIRST, x;
SELECT '--- nd NULLS LAST ---';
SELECT nd, x FROM t_jit_sort_dec ORDER BY nd ASC NULLS LAST, x;
SELECT '--- k, dt DESC, d32 ---';
SELECT k, dt, d32, x FROM t_jit_sort_dec ORDER BY k, dt DESC, d32, x;

CREATE TABLE t_jit_sort_dt64_mt (k UInt64, dt DateTime64(3, 'UTC'), x UInt32) ENGINE = MergeTree ORDER BY (k, dt);

INSERT INTO t_jit_sort_dt64_mt VALUES (1, '1969-12-31 23:59:59.999', 1), (2, '2026-01-01 00:00:00.000', 2);
INSERT INTO t_jit_sort_dt64_mt VALUES (1, '1970-01-01 00:00:00.000', 3), (2, '1960-06-01 12:00:00.000', 4);
INSERT INTO t_jit_sort_dt64_mt VALUES (1, '1900-01-01 00:00:00.000', 5), (2, '2026-01-01 00:00:00.001', 6);

SELECT '--- MergeTree read in order ---';
SELECT k, dt, x FROM t_jit_sort_dt64_mt ORDER BY k, dt
SETTINGS optimize_read_in_order = 1, read_in_order_two_level_merge_threshold = 0, max_threads = 1;

DROP TABLE t_jit_sort_dec;
DROP TABLE t_jit_sort_dt64_mt;
