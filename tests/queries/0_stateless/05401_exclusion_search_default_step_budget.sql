-- Tags: no-parallel-replicas
-- no-parallel-replicas: the profile events are checked via the query log of the initiator.

-- A filter on a primary key column that is not the first one makes the generic exclusion search split a part with
-- many one-row marks down to single marks. The default `merge_tree_generic_exclusion_search_rows_per_step` bounds that
-- search, also with an older `compatibility`, but not the row estimate of an absolute `SAMPLE`.

DROP TABLE IF EXISTS t_exclusion_search_default_budget;

CREATE TABLE t_exclusion_search_default_budget (c1 DateTime, c2 UInt8, c3 String, c4 UInt32)
ENGINE = MergeTree ORDER BY (c1, c2, c3, c4) SAMPLE BY c4
SETTINGS index_granularity = 1;

-- `c1` changes every 4 rows, so a range of marks can only be excluded when both of its boundary marks have the same
-- `c1`, and the exhaustive search needs more steps than the default budget. A single insert block creates a single
-- part. `OPTIMIZE FINAL` is avoided: a merge of tiny granules is very slow with the thread fuzzer.
INSERT INTO t_exclusion_search_default_budget
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + intDiv(number, 4), (number % 4) >= 2, toString(number % 97), intHash32(number)
FROM numbers(120000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000;

SELECT count(), min(marks) >= 120000 FROM system.parts
WHERE database = currentDatabase() AND table = 't_exclusion_search_default_budget' AND active;

-- `LIMIT 0` reads nothing, but the primary key analysis still runs.
SELECT c1, c3 FROM t_exclusion_search_default_budget WHERE c2 LIMIT 0
SETTINGS use_query_condition_cache = 0, log_comment = '05401_exclusion_search_default_budget default';
SELECT c1, c3 FROM t_exclusion_search_default_budget WHERE c2 LIMIT 0
SETTINGS use_query_condition_cache = 0, merge_tree_generic_exclusion_search_rows_per_step = 0, log_comment = '05401_exclusion_search_default_budget unlimited';

SYSTEM FLUSH LOGS query_log;

SELECT splitByChar(' ', log_comment)[2] AS budget, max(ProfileEvents['IndexGenericExclusionSearchStepLimitReached']) > 0 AS step_limit_reached
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment LIKE '05401_exclusion_search_default_budget %'
GROUP BY budget
ORDER BY budget;

-- The exhaustive search keeps 3 of every 4 one-row marks, 90000 rows, so `SAMPLE 10000` gives a factor of 9. An estimate
-- from the bounded search would count more rows and give a larger factor.
SELECT round(_sample_factor, 3) FROM t_exclusion_search_default_budget SAMPLE 10000 WHERE c2 LIMIT 1
SETTINGS use_query_condition_cache = 0, max_threads = 1, max_block_size = 128;

-- An older `compatibility` keeps the default budget. The second setting shows that `compatibility` is applied.
SET compatibility = '25.12';
SELECT name, value FROM system.settings
WHERE name IN ('merge_tree_generic_exclusion_search_rows_per_step', 'use_partition_minmax_for_primary_key_pruning')
ORDER BY name;

DROP TABLE t_exclusion_search_default_budget;
