-- Tags: no-random-merge-tree-settings
-- The mark cache charges an entry by its memory usage, so retaining a compressed marks file for decoding on demand
-- must not take more memory than the bit-packed marks. Also, a retained file is converted to the bit-packed marks
-- once its readers have decoded as much as the whole file decompresses to.

-- Repeated marks of a `LowCardinality` dictionary in small compressed blocks: the bit-packed marks are much smaller.
CREATE TABLE marks_size_small_blocks (v LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 16, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, prewarm_mark_cache = 0,
    compress_marks = 1, marks_compress_block_size = 512;
-- Default compressed blocks.
CREATE TABLE marks_size_default_blocks (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 16, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, prewarm_mark_cache = 0,
    compress_marks = 1;
CREATE TABLE marks_size_small_blocks_plain AS marks_size_small_blocks;
ALTER TABLE marks_size_small_blocks_plain MODIFY SETTING compress_marks = 0;
CREATE TABLE marks_size_default_blocks_plain AS marks_size_default_blocks;
ALTER TABLE marks_size_default_blocks_plain MODIFY SETTING compress_marks = 0;

INSERT INTO marks_size_small_blocks SELECT 'x' FROM numbers(1000000);
INSERT INTO marks_size_small_blocks_plain SELECT 'x' FROM numbers(1000000);
INSERT INTO marks_size_default_blocks SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO marks_size_default_blocks_plain SELECT number, toString(number) FROM numbers(1000000);

SET load_marks_asynchronously = 0, max_threads = 1, enable_parallel_replicas = 0;

SELECT sum(cityHash64(v)) FROM marks_size_small_blocks SETTINGS log_comment = '05875_small_blocks';
SELECT sum(cityHash64(v)) FROM marks_size_small_blocks_plain SETTINGS log_comment = '05875_small_blocks_plain';

SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks SETTINGS log_comment = '05875_default_blocks';
SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks_plain SETTINGS log_comment = '05875_default_blocks_plain';

-- Reads of many small ranges decode every compressed block, so the marks are converted to the bit-packed marks
-- after a few queries, and the results stay the same.
SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks_plain WHERE k IN (SELECT number * 97 FROM numbers(10000));
SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks WHERE k IN (SELECT number * 97 FROM numbers(10000));
SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks WHERE k IN (SELECT number * 97 FROM numbers(10000));
SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks WHERE k IN (SELECT number * 97 FROM numbers(10000));
SELECT sum(k), sum(cityHash64(s)) FROM marks_size_default_blocks WHERE k IN (SELECT number * 97 FROM numbers(10000));
SELECT k, s FROM marks_size_default_blocks WHERE k IN (0, 17, 500000, 999999) ORDER BY k;

SYSTEM FLUSH LOGS query_log;

WITH
    maxIf(ProfileEvents['LoadedMarksMemoryBytes'], log_comment = '05875_small_blocks') AS compressed,
    maxIf(ProfileEvents['LoadedMarksMemoryBytes'], log_comment = '05875_small_blocks_plain') AS plain,
    maxIf(ProfileEvents['LoadedMarksMemoryBytes'], log_comment = '05875_default_blocks') AS default_compressed,
    maxIf(ProfileEvents['LoadedMarksMemoryBytes'], log_comment = '05875_default_blocks_plain') AS default_plain,
    plain > 0 AND compressed > 0 AND compressed <= plain * 1.1
        AND default_plain > 0 AND default_compressed > 0 AND default_compressed <= default_plain * 1.1 AS ok
SELECT ok, if(ok, [], [compressed, plain, default_compressed, default_plain]) AS sizes_if_not_ok
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday();

DROP TABLE marks_size_small_blocks, marks_size_small_blocks_plain, marks_size_default_blocks, marks_size_default_blocks_plain;
