-- A wide part written by INSERT: columns whose rows all hold the type default (and the ones that only look like it)
-- must read back the same, with and without sparse serialization.

DROP TABLE IF EXISTS t_defaults_sparse;
DROP TABLE IF EXISTS t_defaults_no_sparse;
DROP TABLE IF EXISTS t_defaults_nullable_sparse;

CREATE TABLE t_defaults_sparse
(
    k UInt64, d_u64 UInt64, d_s String, d_fs FixedString(3), d_dec Decimal(18, 4), d_f64 Float64,
    f_neg Float64, u_last UInt64, n Nullable(UInt32), n_hidden Nullable(UInt32),
    lc LowCardinality(String), lc_n LowCardinality(Nullable(String))
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9375, nullable_serialization_version = 'basic';

CREATE TABLE t_defaults_no_sparse AS t_defaults_sparse
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1000,
    ratio_of_defaults_for_sparse_serialization = 1, nullable_serialization_version = 'basic';

CREATE TABLE t_defaults_nullable_sparse AS t_defaults_sparse
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9375, nullable_serialization_version = 'allow_sparse',
    serialization_info_version = 'with_types';

-- The key is in reverse order of the input, so the rows are permuted on write.
-- f_neg is -0.0 (not the default bitwise), u_last is non-zero only in the last row, n_hidden holds NULLs over 1..10000.
INSERT INTO t_defaults_sparse (k, f_neg, u_last, n_hidden)
SELECT 10000 - number, -0.0, if(number = 9999, 42, 0), if(number < 1e9, NULL, number + 1) FROM numbers(10000);
INSERT INTO t_defaults_no_sparse (k, f_neg, u_last, n_hidden)
SELECT 10000 - number, -0.0, if(number = 9999, 42, 0), if(number < 1e9, NULL, number + 1) FROM numbers(10000);
INSERT INTO t_defaults_nullable_sparse (k, f_neg, u_last, n_hidden)
SELECT 10000 - number, -0.0, if(number = 9999, 42, 0), if(number < 1e9, NULL, number + 1) FROM numbers(10000);

SELECT table, arraySort(groupUniqArrayIf(column, serialization_kind = 'Sparse')) AS sparse_columns
FROM system.parts_columns
WHERE database = currentDatabase() AND table LIKE 't_defaults_%' AND active
GROUP BY table
ORDER BY table;

SELECT
    count(),
    countIf(d_u64 != 0) + countIf(d_s != '') + countIf(d_fs != '\0\0\0') + countIf(d_dec != 0) + countIf(d_f64 != 0) + countIf(lc != '') AS non_defaults,
    countIf(1 / f_neg < 0) AS negative_zeros,
    argMin(u_last, k) AS u_last_at_min_k,
    countIf(u_last != 0) AS u_last_non_zero,
    countIf(n IS NULL) + countIf(n_hidden IS NULL) + countIf(lc_n IS NULL) AS nulls
FROM t_defaults_sparse;

SELECT
    count(),
    countIf(d_u64 != 0) + countIf(d_s != '') + countIf(d_fs != '\0\0\0') + countIf(d_dec != 0) + countIf(d_f64 != 0) + countIf(lc != '') AS non_defaults,
    countIf(1 / f_neg < 0) AS negative_zeros,
    argMin(u_last, k) AS u_last_at_min_k,
    countIf(u_last != 0) AS u_last_non_zero,
    countIf(n IS NULL) + countIf(n_hidden IS NULL) + countIf(lc_n IS NULL) AS nulls
FROM t_defaults_no_sparse;

SELECT
    count(),
    countIf(d_u64 != 0) + countIf(d_s != '') + countIf(d_fs != '\0\0\0') + countIf(d_dec != 0) + countIf(d_f64 != 0) + countIf(lc != '') AS non_defaults,
    countIf(1 / f_neg < 0) AS negative_zeros,
    argMin(u_last, k) AS u_last_at_min_k,
    countIf(u_last != 0) AS u_last_non_zero,
    countIf(n IS NULL) + countIf(n_hidden IS NULL) + countIf(lc_n IS NULL) AS nulls
FROM t_defaults_nullable_sparse;

-- The values under the NULLs move with their rows.
SELECT k, assumeNotNull(n_hidden) FROM t_defaults_sparse ORDER BY k LIMIT 3;
SELECT k, assumeNotNull(n_hidden) FROM t_defaults_no_sparse ORDER BY k LIMIT 3;
SELECT k, assumeNotNull(n_hidden) FROM t_defaults_nullable_sparse ORDER BY k LIMIT 3;

DROP TABLE t_defaults_sparse;
DROP TABLE t_defaults_no_sparse;
DROP TABLE t_defaults_nullable_sparse;
