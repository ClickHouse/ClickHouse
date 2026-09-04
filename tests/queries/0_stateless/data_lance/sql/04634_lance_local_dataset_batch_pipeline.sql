-- Dataset-level `Lance` reads use one scan producer and a bounded batch queue.
-- ProfileEvents are selected by unique log comments so the test can run in parallel.

DROP TABLE IF EXISTS lance_local_dataset_pipeline_multi;
DROP TABLE IF EXISTS lance_local_dataset_pipeline_pushdown;
DROP TABLE IF EXISTS lance_local_dataset_pipeline_rich;

CREATE TABLE lance_local_dataset_pipeline_multi
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/multi_frag.lance');

CREATE TABLE lance_local_dataset_pipeline_pushdown
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

CREATE TABLE lance_local_dataset_pipeline_rich
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/rich_types.lance');

-- Multi-fragment projection remains complete with multiple conversion Sources.
SELECT throwIf(count() != 64 OR sum(id) != 2080 OR uniqExact(id) != 64)
FROM lance_local_dataset_pipeline_multi
FORMAT Null
SETTINGS
    max_threads = 4,
    lance_scan_in_order = 0,
    lance_max_batch_sources = 4,
    lance_batch_queue_capacity = 2;

-- Complete and partial predicates preserve the residual-filter semantics.
SELECT throwIf(count() != 10 OR sum(id) != 55)
FROM lance_local_dataset_pipeline_multi
WHERE id <= 10
FORMAT Null
SETTINGS max_threads = 4;

SELECT throwIf(count() != 2 OR sum(id) != 11)
FROM lance_local_dataset_pipeline_pushdown
WHERE id IN (4, 7) AND lower(name) = 'x'
FORMAT Null
SETTINGS max_threads = 4;

-- Nullable, String, Array, and Tuple conversion remains valid.
SELECT throwIf(
    count() != 3
    OR count(string_value) != 2
    OR count(array_value) != 3
    OR count(struct_value) != 3)
FROM lance_local_dataset_pipeline_rich
FORMAT Null
SETTINGS max_threads = 4;

-- Virtual-only reads expose one stable dataset identity with no synthetic pack suffix.
SELECT throwIf(
    count() != 64
    OR uniqExact(_path) != 1
    OR uniqExact(_file) != 1
    OR NOT endsWith(any(_path), 'multi_frag.lance')
    OR any(_file) != 'multi_frag.lance')
FROM lance_local_dataset_pipeline_multi
FORMAT Null
SETTINGS optimize_count_from_files = 0, max_threads = 4;

-- A complete-predicate `LIMIT` is one global quota across all Sources.
SELECT throwIf(count() != 7 OR uniqExact(id) != 7)
FROM
(
    SELECT id
    FROM lance_local_dataset_pipeline_multi
    WHERE id > 0
    LIMIT 7
    SETTINGS max_threads = 4, lance_scan_in_order = 0
)
FORMAT Null;

SET log_queries = 1;

SELECT id, name
FROM lance_local_dataset_pipeline_multi
FORMAT Null
SETTINGS
    max_threads = 4,
    lance_scan_in_order = 0,
    lance_max_batch_sources = 4,
    lance_batch_queue_capacity = 2,
    log_comment = '04634_lance_dataset_pipeline';

SELECT id
FROM lance_local_dataset_pipeline_multi
FORMAT Null
SETTINGS
    max_threads = 4,
    lance_scan_in_order = 1,
    log_comment = '04634_lance_dataset_ordered';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['LanceDatasetOpen'] = 1,
    ProfileEvents['LancePlanScan'] = 1,
    ProfileEvents['LanceProducerTasks'] = 1,
    ProfileEvents['LanceScanSchemaExports'] = 1,
    ProfileEvents['LanceBatchSources'] = 4,
    ProfileEvents['LanceQueuePushBatches'] = ProfileEvents['LanceQueuePopBatches'],
    ProfileEvents['LanceQueuePeakBatches'] <= 2,
    ProfileEvents['LanceQueuePeakBytes'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04634_lance_dataset_pipeline'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

SELECT
    ProfileEvents['LancePlanScan'] = 1,
    ProfileEvents['LanceProducerTasks'] = 1,
    ProfileEvents['LanceBatchSources'] = 1,
    ProfileEvents['LanceBatchSourcesActive'] = 1
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04634_lance_dataset_ordered'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

DROP TABLE lance_local_dataset_pipeline_rich;
DROP TABLE lance_local_dataset_pipeline_pushdown;
DROP TABLE lance_local_dataset_pipeline_multi;
