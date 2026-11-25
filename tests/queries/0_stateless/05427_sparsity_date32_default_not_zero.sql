-- `Date32` default is 1900-01-01, but the per-part `num_defaults` counter counts bitwise-zero rows (1970-01-01),
-- so `count()` with an equality to the `Date32` default must not be served from that counter.
SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_date32_sparse;
DROP TABLE IF EXISTS t_date32_dense;

CREATE TABLE t_date32_sparse (id UInt32, d Date32)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_date32_sparse SELECT number, if(number % 50 = 7, toDate32('1900-01-01'), toDate32('1970-01-01')) FROM numbers(1000);

SELECT 'eq',          count() FROM t_date32_sparse WHERE d = toDate32('1900-01-01')  SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'eq_reversed', count() FROM t_date32_sparse WHERE toDate32('1900-01-01') = d  SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'ne',          count() FROM t_date32_sparse WHERE d != toDate32('1900-01-01') SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

CREATE TABLE t_date32_dense (d Date32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_date32_dense VALUES ('1900-01-01'), ('2000-02-29');

SELECT 'dense_eq', count() FROM t_date32_dense WHERE d = toDate32('1900-01-01') SETTINGS optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE t_date32_sparse;
DROP TABLE t_date32_dense;
