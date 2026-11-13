-- The `FSST` codec: the data is the same as with another codec, and the values of a `String` column compressed by it
-- are checked against the string filters from `PREWHERE` (`apply_string_filters_during_scan`) without decompressing them.
-- Every query is run on a table without `FSST`, then on the tables with it.

-- The string filters are not applied when the read columns may be written to the columns cache (it is randomized in tests),
-- and the blocks are not read without decompression from the uncompressed cache.
SET use_columns_cache = 0;
SET use_uncompressed_cache = 0;
SET apply_string_filters_during_scan = 1;
SET enable_fsst_codec = 1;

DROP TABLE IF EXISTS t_fsst_plain;
DROP TABLE IF EXISTS t_fsst;
DROP TABLE IF EXISTS t_fsst_small_blocks;
DROP TABLE IF EXISTS t_fsst_compact;

CREATE TABLE t_fsst_plain (id UInt32, s String, z String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

-- `s` is compressed by `FSST` alone and `z` by a chain, where the other stages are decompressed before filtering.
-- A small index granularity makes the reads start in the middle of the compressed blocks.
-- The values are checked without decompression only with a separate stream of the sizes of the values, which is stored
-- with `serialization_info_version = 'with_types'` (it is randomized in tests).
CREATE TABLE t_fsst (id UInt32, s String CODEC(FSST), z String CODEC(FSST, ZSTD(1)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
    string_serialization_version = 'with_size_stream', serialization_info_version = 'with_types', index_granularity = 128, index_granularity_bytes = '10M';

-- Tiny compressed blocks: many values continue in the next block.
CREATE TABLE t_fsst_small_blocks (id UInt32, s String CODEC(FSST), z String CODEC(FSST, LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
    string_serialization_version = 'with_size_stream', serialization_info_version = 'with_types', min_compress_block_size = 100, max_compress_block_size = 300;

CREATE TABLE t_fsst_compact (id UInt32, s String CODEC(FSST), z String CODEC(FSST, ZSTD(1)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_fsst_plain
SELECT
    number,
    multiIf(
        number % 11 = 0, '',
        number % 7 = 0, 'https://clickhouse.com/docs/' || toString(number % 1000) || '/index.html',
        number % 5 = 0, 'https://example.com/' || toString(number) || '/page.html?needle=' || toString(number % 3),
        number % 3 = 0, 'mailto:user' || toString(number) || '@example.com',
        -- Incompressible binary values, which make some blocks stored without encoding.
        number % 1000 < 30, reinterpretAsString(cityHash64(number)) || reinterpretAsString(cityHash64(number + 1)),
        'http://www.example.org/search?q=' || toString(number % 100) || '&page=' || toString(number % 10)),
    multiIf(
        number % 997 = 0, repeat('long value ', 2000) || 'needle' || toString(number),
        number % 13 = 0, '',
        'z-value ' || toString(number % 500) || ' with a needle' || toString(number % 2))
FROM numbers(30000);

INSERT INTO t_fsst SELECT * FROM t_fsst_plain;
INSERT INTO t_fsst_small_blocks SELECT * FROM t_fsst_plain;
INSERT INTO t_fsst_compact SELECT * FROM t_fsst_plain;

SELECT 'full read';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst_plain;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst_small_blocks;
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst_compact;

SELECT 'substring';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE s LIKE '%clickhouse%';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_fsst PREWHERE s LIKE '%clickhouse%';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE s LIKE '%clickhouse%';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM t_fsst_compact PREWHERE s LIKE '%clickhouse%';

SELECT 'substring in a chain of codecs and in long values';
SELECT count(), sum(cityHash64(z)) FROM t_fsst_plain PREWHERE z LIKE '%needle1%';
SELECT count(), sum(cityHash64(z)) FROM t_fsst PREWHERE z LIKE '%needle1%';
SELECT count(), sum(cityHash64(z)) FROM t_fsst_small_blocks PREWHERE z LIKE '%needle1%';
SELECT count(), sum(cityHash64(z)) FROM t_fsst_compact PREWHERE z LIKE '%needle1%';

SELECT 'prefix';
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://example.com/12');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://example.com/12');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://example.com/12');

SELECT 'suffix';
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE s LIKE '%7/index.html';
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE s LIKE '%7/index.html';
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE s LIKE '%7/index.html';

SELECT 'equals';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE s = 'https://clickhouse.com/docs/42/index.html';
SELECT count(), sum(cityHash64(id)) FROM t_fsst PREWHERE s = 'https://clickhouse.com/docs/42/index.html';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE s = 'https://clickhouse.com/docs/42/index.html';

SELECT 'several conditions';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst_plain PREWHERE s LIKE 'http%example.com/1%' AND s LIKE '%needle=2' AND z LIKE '%value 1%';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst PREWHERE s LIKE 'http%example.com/1%' AND s LIKE '%needle=2' AND z LIKE '%value 1%';
SELECT count(), sum(cityHash64(s)), sum(cityHash64(z)) FROM t_fsst_small_blocks PREWHERE s LIKE 'http%example.com/1%' AND s LIKE '%needle=2' AND z LIKE '%value 1%';

SELECT 'prefixes of different lengths, compared on the codes';
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'h');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'h');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'h');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https:/');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https:/');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https:/');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://c');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://c');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://c');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://example');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://example');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://example');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://example.');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://example.');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://example.');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://example.c');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://example.c');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://example.c');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE startsWith(s, 'https://clickhouse.com/docs/1');
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE startsWith(s, 'https://clickhouse.com/docs/1');
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE startsWith(s, 'https://clickhouse.com/docs/1');

SELECT 'equality, compared on the codes';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE s = 'mailto:user9@example.com';
SELECT count(), sum(cityHash64(id)) FROM t_fsst PREWHERE s = 'mailto:user9@example.com';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE s = 'mailto:user9@example.com';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE s = 'mailto:user3@example.com';
SELECT count(), sum(cityHash64(id)) FROM t_fsst PREWHERE s = 'mailto:user3@example.com';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE s = 'mailto:user3@example.com';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE s = 'https://clickhouse.com/docs/7/index.html';
SELECT count(), sum(cityHash64(id)) FROM t_fsst PREWHERE s = 'https://clickhouse.com/docs/7/index.html';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE s = 'https://clickhouse.com/docs/7/index.html';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE z = 'z-value 1 with a needle1';
SELECT count(), sum(cityHash64(id)) FROM t_fsst PREWHERE z = 'z-value 1 with a needle1';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE z = 'z-value 1 with a needle1';

SELECT 'equality and substring';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_plain PREWHERE z = 'z-value 1 with a needle1' AND s LIKE '%example%';
SELECT count(), sum(cityHash64(id)) FROM t_fsst PREWHERE z = 'z-value 1 with a needle1' AND s LIKE '%example%';
SELECT count(), sum(cityHash64(id)) FROM t_fsst_small_blocks PREWHERE z = 'z-value 1 with a needle1' AND s LIKE '%example%';

SELECT 'a long needle';
SELECT count() FROM t_fsst_plain PREWHERE z LIKE '%long value long value long value long value long value long value long value%';
SELECT count() FROM t_fsst PREWHERE z LIKE '%long value long value long value long value long value long value long value%';

SELECT 'ranges of the primary key';
SELECT count(), sum(cityHash64(s)) FROM t_fsst_plain PREWHERE s LIKE '%search%' WHERE id BETWEEN 1000 AND 1500 OR id BETWEEN 7777 AND 9999 OR id > 29990;
SELECT count(), sum(cityHash64(s)) FROM t_fsst PREWHERE s LIKE '%search%' WHERE id BETWEEN 1000 AND 1500 OR id BETWEEN 7777 AND 9999 OR id > 29990;
SELECT count(), sum(cityHash64(s)) FROM t_fsst_small_blocks PREWHERE s LIKE '%search%' WHERE id BETWEEN 1000 AND 1500 OR id BETWEEN 7777 AND 9999 OR id > 29990;

SELECT 'the values are checked without decompression';
-- `enable_parallel_replicas = 0`: with parallel replicas another replica may do the reads,
-- and their `ProfileEvents` never reach the `query_log` rows checked below.
SELECT count() FROM t_fsst PREWHERE s LIKE '%rare-substring%' SETTINGS enable_parallel_replicas = 0, log_comment = '05627_fsst_filter';
SELECT count() FROM t_fsst PREWHERE z LIKE '%rare-substring%' SETTINGS enable_parallel_replicas = 0, log_comment = '05627_fsst_filter';
SYSTEM FLUSH LOGS query_log;
SELECT
    sum(ProfileEvents['CompressedReadBufferFSSTBlocksWithoutDecoding']) > 0,
    sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05627_fsst_filter';

DROP TABLE t_fsst_plain;
DROP TABLE t_fsst;
DROP TABLE t_fsst_small_blocks;
DROP TABLE t_fsst_compact;
