-- A `false_positive_rate` of `0.0001` selects 16-bit fingerprints in `cuckoo_filter` and `binary_fuse_filter`.
-- With more than 32 distinct values per granule `binary_fuse_filter` builds a fuse filter rather than an exact
-- list. Check the persisted 16-bit filters: present values must be found, and the results must be the same
-- with and without the skip indexes.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;

SELECT 'cuckoo_filter';

DROP TABLE IF EXISTS t_16_bit;
CREATE TABLE t_16_bit
(
    id UInt64,
    v UInt64,
    s String,
    INDEX idx_v v TYPE cuckoo_filter(0.0001) GRANULARITY 1,
    INDEX idx_s s TYPE cuckoo_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000;
INSERT INTO t_16_bit SELECT number, number * 3, toString(number * 3) FROM numbers(10000);

-- Every present value is found through the index.
SELECT count() FROM t_16_bit WHERE v IN (SELECT number * 3 FROM numbers(10000) WHERE number % 97 = 0) SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_16_bit WHERE v IN (SELECT number * 3 FROM numbers(10000) WHERE number % 97 = 0) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_16_bit WHERE s IN (SELECT toString(number * 3) FROM numbers(10000) WHERE number % 89 = 0) SETTINGS force_data_skipping_indices = 'idx_s';
SELECT count() FROM t_16_bit WHERE s IN (SELECT toString(number * 3) FROM numbers(10000) WHERE number % 89 = 0) SETTINGS use_skip_indexes = 0;
SELECT v FROM t_16_bit WHERE v = 29997 SETTINGS force_data_skipping_indices = 'idx_v';
SELECT s FROM t_16_bit WHERE s = '15000' SETTINGS force_data_skipping_indices = 'idx_s';

-- Absent values: no row matches either way.
SELECT count() FROM t_16_bit WHERE v IN (1, 2, 4, 5, 30001) SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_16_bit WHERE v IN (1, 2, 4, 5, 30001) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_16_bit WHERE s = '1' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT count() FROM t_16_bit WHERE s = '1' SETTINGS use_skip_indexes = 0;

DROP TABLE t_16_bit;

SELECT 'binary_fuse_filter';

DROP TABLE IF EXISTS t_16_bit;
CREATE TABLE t_16_bit
(
    id UInt64,
    v UInt64,
    s String,
    INDEX idx_v v TYPE binary_fuse_filter(0.0001) GRANULARITY 1,
    INDEX idx_s s TYPE binary_fuse_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000;
INSERT INTO t_16_bit SELECT number, number * 3, toString(number * 3) FROM numbers(10000);

-- Every present value is found through the index.
SELECT count() FROM t_16_bit WHERE v IN (SELECT number * 3 FROM numbers(10000) WHERE number % 97 = 0) SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_16_bit WHERE v IN (SELECT number * 3 FROM numbers(10000) WHERE number % 97 = 0) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_16_bit WHERE s IN (SELECT toString(number * 3) FROM numbers(10000) WHERE number % 89 = 0) SETTINGS force_data_skipping_indices = 'idx_s';
SELECT count() FROM t_16_bit WHERE s IN (SELECT toString(number * 3) FROM numbers(10000) WHERE number % 89 = 0) SETTINGS use_skip_indexes = 0;
SELECT v FROM t_16_bit WHERE v = 29997 SETTINGS force_data_skipping_indices = 'idx_v';
SELECT s FROM t_16_bit WHERE s = '15000' SETTINGS force_data_skipping_indices = 'idx_s';

-- Absent values: no row matches either way.
SELECT count() FROM t_16_bit WHERE v IN (1, 2, 4, 5, 30001) SETTINGS force_data_skipping_indices = 'idx_v';
SELECT count() FROM t_16_bit WHERE v IN (1, 2, 4, 5, 30001) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_16_bit WHERE s = '1' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT count() FROM t_16_bit WHERE s = '1' SETTINGS use_skip_indexes = 0;

DROP TABLE t_16_bit;
