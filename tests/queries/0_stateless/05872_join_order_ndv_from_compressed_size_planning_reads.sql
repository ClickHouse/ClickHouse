-- The planning of a join over cold parts reads only the headers of the data files of the columns whose compressed
-- size gives an estimate of the number of distinct values (`estimate_distinct_values_from_metadata`), to check that
-- they are stored by general purpose codecs. Columns that take too many compressed bits per row read nothing.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;
SET estimate_distinct_values_from_metadata = 1;

DROP TABLE IF EXISTS t;

-- `k` has 16 distinct values and gets an estimate; `r` and `s` are unique and take far more than 10 bits per row.
CREATE TABLE t (id UInt32, k UInt32 CODEC(ZSTD(1)), r UInt64 CODEC(LZ4), s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;

SYSTEM STOP MERGES t;

-- Ten parts of 20000 rows: each data file of `r` and `s` is larger than a read buffer of the default size.
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(0, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(20000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(40000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(60000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(80000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(100000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(120000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(140000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(160000, 20000);
INSERT INTO t SELECT number, intHash32(number) % 16, intHash64(number), toString(intHash64(number + 1)) FROM numbers(180000, 20000);

SELECT 'Join on columns without an estimate.';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.r = b.r AND a.s = b.s) SETTINGS log_comment = '05872_no_estimate';

SELECT 'Join on a column with an estimate.';
SELECT toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 1 / 1024 AND 1 / 4
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.k = b.k) SETTINGS log_comment = '05872_estimate';

SYSTEM FLUSH LOGS query_log;

-- A read buffer of the default size reads 128 KiB of each file of `r` and `s`, and the whole file of `k`, about 2 MiB in all.
-- The beginning of the first frame of a file, read to tell its codecs, is at most 281 bytes.
SELECT log_comment, ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] <= if(log_comment = '05872_estimate', 10 * 1024, 1024)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05872_%' AND query LIKE '%EXPLAIN%'
ORDER BY log_comment;

DROP TABLE t;
