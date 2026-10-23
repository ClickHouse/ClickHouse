-- Statistics are not loaded when the primary key leaves fewer than `min_marks_to_use_statistics` marks to read:
-- neither for pruning parts nor for ordering the conditions of `PREWHERE`.

SET enable_analyzer = 1, optimize_move_to_prewhere = 1;
-- Statistics-load counts measure one local planner, without additional parallel-replica candidate plans.
SET enable_parallel_replicas = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1, max_threads = 1;
SET use_statistics = 1, use_statistics_for_part_pruning = 1;
SET explain_query_plan_default = 'legacy';
SET min_marks_to_use_statistics = 1000;

DROP TABLE IF EXISTS statistics_small_reads;

CREATE TABLE statistics_small_reads (key UInt64, value UInt64, flag UInt8)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi', auto_statistics_types = 'basic, uniq_v2';

SYSTEM STOP MERGES statistics_small_reads;

-- Four parts of 625 marks each, `value` of the part `i` is in [i * 10000, i * 10000 + 9999].
INSERT INTO statistics_small_reads SELECT number, number, number % 2 FROM numbers(0, 10000);
INSERT INTO statistics_small_reads SELECT number, number, number % 2 FROM numbers(10000, 10000);
INSERT INTO statistics_small_reads SELECT number, number, number % 2 FROM numbers(20000, 10000);
INSERT INTO statistics_small_reads SELECT number, number, number % 2 FROM numbers(30000, 10000);

SELECT '-- the primary key leaves two marks of two parts: no statistics';
SELECT count() FROM statistics_small_reads WHERE key IN (5, 35000) AND value >= 30000 AND flag = 0
SETTINGS log_comment = '05407_query_small';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM statistics_small_reads WHERE key IN (5, 35000) AND value >= 30000 AND flag = 0)
WHERE explain LIKE '%Statistics%';

SELECT '-- the same read with the threshold disabled: statistics are used';
SELECT count() FROM statistics_small_reads WHERE key IN (5, 35000) AND value >= 30000 AND flag = 0
SETTINGS min_marks_to_use_statistics = 0, log_comment = '05407_query_small_no_threshold';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM statistics_small_reads WHERE key IN (5, 35000) AND value >= 30000 AND flag = 0 SETTINGS min_marks_to_use_statistics = 0)
WHERE explain LIKE '%Statistics%';

SELECT '-- a large read: statistics are used';
SELECT count() FROM statistics_small_reads WHERE key >= 1 AND value >= 30000 AND flag = 0
SETTINGS log_comment = '05407_query_large';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM statistics_small_reads WHERE key >= 1 AND value >= 30000 AND flag = 0)
WHERE explain LIKE '%Statistics%';

SELECT '-- the same read without statistics';
SELECT count() FROM statistics_small_reads WHERE key >= 1 AND value >= 30000 AND flag = 0
SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 0;

SYSTEM FLUSH LOGS query_log;

-- The statistics estimator increments `LoadedStatistics` once per part whose statistics it collects.
SELECT log_comment, ProfileEvents['LoadedStatistics'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05407_query_')
ORDER BY log_comment;

DROP TABLE statistics_small_reads;

SELECT '-- row limits apply to the parts left after pruning by statistics';
DROP TABLE IF EXISTS statistics_small_reads_limits;
CREATE TABLE statistics_small_reads_limits (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi', auto_statistics_types = 'basic';
SYSTEM STOP MERGES statistics_small_reads_limits;
INSERT INTO statistics_small_reads_limits SELECT number, 0 FROM numbers(10000);
INSERT INTO statistics_small_reads_limits SELECT number, 1 FROM numbers(10);
SELECT sum(k) FROM statistics_small_reads_limits WHERE v = 1
SETTINGS optimize_move_to_prewhere = 0, min_marks_to_use_statistics = 0, max_rows_to_read = 100;
SELECT sum(k) FROM statistics_small_reads_limits WHERE v = 1
SETTINGS optimize_move_to_prewhere = 0, min_marks_to_use_statistics = 0, max_rows_to_read_leaf = 100;
DROP TABLE statistics_small_reads_limits;
