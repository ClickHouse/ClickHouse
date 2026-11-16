-- The primary key probe that decides whether to load statistics analyzes the parts in parallel when their
-- primary indexes are not in memory, and decides as the serial probe does (see 05407_statistics_skip_small_reads).

SET enable_analyzer = 1, optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1, use_statistics_for_part_pruning = 1;
SET min_marks_per_part_to_load_statistics = 100;
SET max_threads = 4, max_threads_for_indexes = 0;

DROP TABLE IF EXISTS statistics_parallel_probe;

CREATE TABLE statistics_parallel_probe (key UInt64, value UInt64, flag UInt8)
ENGINE = MergeTree ORDER BY key PARTITION BY intDiv(key, 10000)
SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi', auto_statistics_types = 'basic, uniq_v2',
    prewarm_statistics_cache = 0, primary_key_lazy_load = 1, use_primary_key_cache = 0, prewarm_primary_key_cache = 0;

SYSTEM STOP MERGES statistics_parallel_probe;

INSERT INTO statistics_parallel_probe SELECT number, number, number % 2 FROM numbers(0, 10000);
INSERT INTO statistics_parallel_probe SELECT number, number, number % 2 FROM numbers(10000, 10000);
INSERT INTO statistics_parallel_probe SELECT number, number, number % 2 FROM numbers(20000, 10000);
INSERT INTO statistics_parallel_probe SELECT number, number, number % 2 FROM numbers(30000, 10000);

-- Read the statistics from disk again, with no primary index in memory.
DETACH TABLE statistics_parallel_probe;
ATTACH TABLE statistics_parallel_probe;

-- With `PREWHERE` the probe runs three times: in parallel while the indexes are not in memory, then serially.
SELECT count() FROM statistics_parallel_probe WHERE key IN (5, 35000) AND value >= 30000 AND flag = 0
SETTINGS log_comment = 'parallel_probe_small';

-- The same with the primary index cache, where an index in the cache is in memory.
ALTER TABLE statistics_parallel_probe MODIFY SETTING use_primary_key_cache = 1;
SYSTEM UNLOAD PRIMARY KEY statistics_parallel_probe;

SELECT count() FROM statistics_parallel_probe WHERE key IN (5, 35000) AND value >= 30000 AND flag = 0
SETTINGS log_comment = 'parallel_probe_small_cache';

-- Without `PREWHERE` the decision is made once, by the parallel analysis.
ALTER TABLE statistics_parallel_probe MODIFY SETTING use_primary_key_cache = 0;
SYSTEM UNLOAD PRIMARY KEY statistics_parallel_probe;

SELECT count() FROM statistics_parallel_probe WHERE key >= 1 AND value >= 30000 AND flag = 0
SETTINGS optimize_move_to_prewhere = 0, log_comment = 'parallel_probe_large';

SYSTEM FLUSH LOGS query_log, text_log;

-- The last column is the number of probes that analyzed the parts in parallel.
WITH (
    SELECT groupArray(query_id) FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND message_format_string = 'Analyzing the primary key of parts in {} threads to decide whether to load statistics'
) AS parallel_probes
SELECT log_comment, ProfileEvents['LoadedStatistics'] > 0, ProfileEvents['SelectedParts'], countEqual(parallel_probes, query_id)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, 'parallel_probe_')
ORDER BY log_comment;

DROP TABLE statistics_parallel_probe;
