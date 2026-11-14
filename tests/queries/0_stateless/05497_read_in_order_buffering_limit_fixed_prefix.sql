-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: needs 64-row granules and exactly one part per insert.
-- no-parallel-replicas: counts the per-input reads of a single-node read-in-order merge.

-- A `WHERE` that fixes a key prefix (`k = 1`) disables virtual rows, so every input of the read-in-order merge starts
-- with real data. A buffered input must stop reading ahead once the inputs that sort before it already cover the LIMIT.
-- One sparse part is slow to deliver its first rows (`sleepEachRow`), which delays the merge; meanwhile the other
-- sparse parts must not read to their own LIMIT.
-- T1 and T2 run on one thread, which starts with the input of the last inserted part, so the dense part is inserted
-- last. With more threads a sparse part can reach its LIMIT before the dense part delivers any rows.

SET optimize_read_in_order = 1, read_in_order_use_buffering = 1, read_in_order_use_virtual_row = 1,
    read_in_order_two_level_merge_threshold = 100, max_threads = 8, max_block_size = 128,
    preferred_block_size_bytes = 1000000, merge_tree_min_rows_for_concurrent_read = 163840,
    merge_tree_min_bytes_for_concurrent_read = 251658240, optimize_move_to_prewhere = 0,
    query_plan_optimize_prewhere = 0, use_query_condition_cache = 0, use_skip_indexes_for_top_k = 0,
    use_top_k_dynamic_filtering = 0, query_plan_optimize_lazy_materialization = 0,
    short_circuit_function_evaluation = 'force_enable', log_processors_profiles = 1;

DROP TABLE IF EXISTS tab_desc;
DROP TABLE IF EXISTS tab_asc;
DROP TABLE IF EXISTS tab_ovl;

-- Eight parts of 512 granules: seven sparse (1 matching row per 64), one dense, which is read first.
CREATE TABLE tab_desc (k UInt8, x UInt64, v UInt8) ENGINE = MergeTree ORDER BY (k, x)
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES tab_desc;
INSERT INTO tab_desc SELECT 1, 0 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 1 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 2 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 3 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 4 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 5 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 6 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_desc SELECT 1, 7 * 100000 + number, 1 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- The mirror image for ascending order, with the dense part again inserted last.
CREATE TABLE tab_asc (k UInt8, x UInt64, v UInt8) ENGINE = MergeTree ORDER BY (k, x)
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES tab_asc;
INSERT INTO tab_asc SELECT 1, 1 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, 2 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, 3 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, 4 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, 5 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, 6 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, 7 * 100000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_asc SELECT 1, number, 1 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- Overlapping inputs: A = {0} + [100000, 102000) sorts first, B = [1, 5000) second, so B may pause although the
-- merge needs its rows; it must resume.
CREATE TABLE tab_ovl (k UInt8, x UInt64, v UInt8) ENGINE = MergeTree ORDER BY (k, x)
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES tab_ovl;
INSERT INTO tab_ovl SELECT 1, if(number = 0, 0, 100000 + number - 1), 1 FROM numbers(2001) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_ovl SELECT 1, 1 + number, 1 FROM numbers(4999) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO tab_ovl SELECT 1, 500000 + number, number % 64 = 0 FROM numbers(32768) SETTINGS max_block_size = 100000, max_insert_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- T1, descending: the slow rows are the top 2048 rows of part 3.
SELECT x FROM tab_desc WHERE k = 1 AND (x < 330720 OR x >= 332768 OR sleepEachRow(0.0005) = 0) AND v = 1
ORDER BY x DESC LIMIT 100 FORMAT Null SETTINGS max_threads = 1, log_comment = '05497_t1';

-- T2, ascending: the slow rows are the bottom 2048 rows of part 4.
SELECT x FROM tab_asc WHERE k = 1 AND (x < 400000 OR x >= 402048 OR sleepEachRow(0.0005) = 0) AND v = 1
ORDER BY x LIMIT 100 FORMAT Null SETTINGS max_threads = 1, log_comment = '05497_t2';

-- T6: as T1, with a sort column after the key, so the merge has no LIMIT and the sort after it has.
SELECT 'finish', sum(x), count() FROM
(
    SELECT x FROM tab_desc WHERE k = 1 AND (x < 330720 OR x >= 332768 OR sleepEachRow(0.0005) = 0) AND v = 1
    ORDER BY x DESC, v LIMIT 100
)
SETTINGS max_threads = 1, log_comment = '05497_t6';

-- T5: 17 copies of the 8 parts of tab_desc make 136 inputs. With more than 128 inputs nothing pauses, so every input
-- reads to its own LIMIT.
SELECT 'many', sum(x), count() FROM
(
    SELECT x FROM
    (
        SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1 UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
        UNION ALL SELECT x FROM tab_desc WHERE k = 1 AND v = 1
    )
    ORDER BY x DESC LIMIT 4
)
SETTINGS max_threads = 1, log_comment = '05497_t5';

SYSTEM FLUSH LOGS query_log, processors_profile_log;
SET max_block_size = DEFAULT;

-- Inputs, inputs with a virtual row (none: the key prefix is fixed), inputs that read at least the LIMIT (in T1 and
-- T2 only the dense one).
SELECT
    log_comment,
    (
        SELECT (countIf(name = 'BufferChunks'), countIf(name LIKE '%VirtualRow%'), countIf(name = 'BufferChunks' AND input_rows >= q.lim))
        FROM system.processors_profile_log
        WHERE event_date >= today() - 1 AND query_id = q.query_id
    )
FROM
(
    SELECT log_comment, argMax(query_id, event_time_microseconds) AS query_id, if(log_comment = '05497_t5', 4, 100) AS lim
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment IN ('05497_t1', '05497_t2', '05497_t5', '05497_t6') AND type = 'QueryFinish'
        AND event_date >= today() - 1
    GROUP BY log_comment
) AS q
ORDER BY log_comment;
SET max_block_size = 128;

-- T3: the results are correct, also through the preliminary merge.
SELECT 'desc', sum(x), count() FROM (SELECT x FROM tab_desc WHERE k = 1 AND (x < 330720 OR x >= 332768 OR sleepEachRow(0.0005) = 0) AND v = 1 ORDER BY x DESC LIMIT 100);
SELECT 'desc', sum(x), count() FROM (SELECT x FROM tab_desc WHERE k = 1 AND (x < 330720 OR x >= 332768 OR sleepEachRow(0.0005) = 0) AND v = 1 ORDER BY x DESC LIMIT 100) SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 2;
SELECT 'asc', sum(x), count() FROM (SELECT x FROM tab_asc WHERE k = 1 AND (x < 400000 OR x >= 402048 OR sleepEachRow(0.0005) = 0) AND v = 1 ORDER BY x LIMIT 100);
SELECT 'asc', sum(x), count() FROM (SELECT x FROM tab_asc WHERE k = 1 AND (x < 400000 OR x >= 402048 OR sleepEachRow(0.0005) = 0) AND v = 1 ORDER BY x LIMIT 100) SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 2;
SELECT 'premerge',
    (SELECT countIf(explain LIKE '%MergingSortedTransform%') FROM (EXPLAIN PIPELINE SELECT x FROM tab_desc WHERE k = 1 AND v = 1 ORDER BY x DESC LIMIT 100 SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 2)) > 1,
    (SELECT countIf(explain LIKE '%MergingSortedTransform%') FROM (EXPLAIN PIPELINE SELECT x FROM tab_asc WHERE k = 1 AND v = 1 ORDER BY x LIMIT 100 SETTINGS read_in_order_two_level_merge_threshold = 0, max_threads = 2)) > 1;

-- T4: a paused overlapping input resumes when the merge needs it.
SELECT 'ovl', sum(x), count() FROM (SELECT x FROM tab_ovl WHERE k = 1 AND (x < 500000 OR x >= 502048 OR sleepEachRow(0.0005) = 0) AND v = 1 ORDER BY x LIMIT 1000);

DROP TABLE tab_desc;
DROP TABLE tab_asc;
DROP TABLE tab_ovl;
