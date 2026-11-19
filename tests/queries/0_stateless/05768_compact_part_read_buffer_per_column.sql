-- A stream of a compact part reads all columns, but seeks to the ones it needs and skips the others.
-- Its read buffer must be sized by a single column of a stripe, not by the whole stripe:
-- otherwise reading one of many separately compressed columns reads the whole stripe.

DROP TABLE IF EXISTS t_compact_read_buffer;

CREATE TABLE t_compact_read_buffer (c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64, c16 UInt64, c17 UInt64, c18 UInt64, c19 UInt64, c20 UInt64, c21 UInt64, c22 UInt64, c23 UInt64, c24 UInt64, c25 UInt64, c26 UInt64, c27 UInt64, c28 UInt64, c29 UInt64, c30 UInt64, c31 UInt64, c32 UInt64, c33 UInt64, c34 UInt64, c35 UInt64, c36 UInt64, c37 UInt64, c38 UInt64, c39 UInt64, c40 UInt64, c41 UInt64, c42 UInt64, c43 UInt64, c44 UInt64, c45 UInt64, c46 UInt64, c47 UInt64, c48 UInt64, c49 UInt64, c50 UInt64, c51 UInt64, c52 UInt64, c53 UInt64, c54 UInt64, c55 UInt64, c56 UInt64, c57 UInt64, c58 UInt64, c59 UInt64, c60 UInt64, c61 UInt64, c62 UInt64, c63 UInt64, c64 UInt64, c65 UInt64, c66 UInt64, c67 UInt64, c68 UInt64, c69 UInt64, c70 UInt64, c71 UInt64, c72 UInt64, c73 UInt64, c74 UInt64, c75 UInt64, c76 UInt64, c77 UInt64, c78 UInt64, c79 UInt64, c80 UInt64, c81 UInt64, c82 UInt64, c83 UInt64, c84 UInt64, c85 UInt64, c86 UInt64, c87 UInt64, c88 UInt64, c89 UInt64, c90 UInt64, c91 UInt64, c92 UInt64, c93 UInt64, c94 UInt64, c95 UInt64, c96 UInt64, c97 UInt64, c98 UInt64, c99 UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, compress_per_column_in_compact_parts = 1, index_granularity = 8192, index_granularity_bytes = '10M';

INSERT INTO t_compact_read_buffer SELECT number % 10 + 0, number % 10 + 1, number % 10 + 2, number % 10 + 3, number % 10 + 4, number % 10 + 5, number % 10 + 6, number % 10 + 7, number % 10 + 8, number % 10 + 9, number % 10 + 10, number % 10 + 11, number % 10 + 12, number % 10 + 13, number % 10 + 14, number % 10 + 15, number % 10 + 16, number % 10 + 17, number % 10 + 18, number % 10 + 19, number % 10 + 20, number % 10 + 21, number % 10 + 22, number % 10 + 23, number % 10 + 24, number % 10 + 25, number % 10 + 26, number % 10 + 27, number % 10 + 28, number % 10 + 29, number % 10 + 30, number % 10 + 31, number % 10 + 32, number % 10 + 33, number % 10 + 34, number % 10 + 35, number % 10 + 36, number % 10 + 37, number % 10 + 38, number % 10 + 39, number % 10 + 40, number % 10 + 41, number % 10 + 42, number % 10 + 43, number % 10 + 44, number % 10 + 45, number % 10 + 46, number % 10 + 47, number % 10 + 48, number % 10 + 49, number % 10 + 50, number % 10 + 51, number % 10 + 52, number % 10 + 53, number % 10 + 54, number % 10 + 55, number % 10 + 56, number % 10 + 57, number % 10 + 58, number % 10 + 59, number % 10 + 60, number % 10 + 61, number % 10 + 62, number % 10 + 63, number % 10 + 64, number % 10 + 65, number % 10 + 66, number % 10 + 67, number % 10 + 68, number % 10 + 69, number % 10 + 70, number % 10 + 71, number % 10 + 72, number % 10 + 73, number % 10 + 74, number % 10 + 75, number % 10 + 76, number % 10 + 77, number % 10 + 78, number % 10 + 79, number % 10 + 80, number % 10 + 81, number % 10 + 82, number % 10 + 83, number % 10 + 84, number % 10 + 85, number % 10 + 86, number % 10 + 87, number % 10 + 88, number % 10 + 89, number % 10 + 90, number % 10 + 91, number % 10 + 92, number % 10 + 93, number % 10 + 94, number % 10 + 95, number % 10 + 96, number % 10 + 97, number % 10 + 98, number % 10 + 99 FROM numbers(65536) SETTINGS max_insert_block_size = 65536, min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0;

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_read_buffer' AND active;

SELECT sum(c0 * c0) FROM t_compact_read_buffer SETTINGS max_threads = 1, use_uncompressed_cache = 0, log_comment = '05768_one_column';

SYSTEM FLUSH LOGS query_log;

-- The whole stripe is about 30 KB and it was read several times, a single column is a few hundred bytes.
SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < 50000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05768_one_column' AND type = 'QueryFinish' AND event_date >= yesterday();

DROP TABLE t_compact_read_buffer;
