DROP TABLE IF EXISTS lance_local_count_record_batch;

CREATE TABLE lance_local_count_record_batch
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/multi_frag.lance');

SET log_queries = 1;

SELECT count()
FROM lance_local_count_record_batch
FORMAT Null
SETTINGS
    enable_parallel_replicas = 0,
    log_comment = '04635_lance_fast_count';

SELECT
    count(),
    uniqExact(_path),
    uniqExact(_file),
    uniqExact(_data_lake_snapshot_version)
FROM lance_local_count_record_batch
FORMAT Null
SETTINGS
    enable_parallel_replicas = 0,
    optimize_count_from_files = 0,
    max_block_size = 7,
    log_comment = '04635_lance_zero_physical';

SELECT count()
FROM lance_local_count_record_batch
WHERE notEmpty(_path)
FORMAT Null
SETTINGS
    enable_parallel_replicas = 0,
    optimize_count_from_files = 0,
    max_block_size = 7,
    log_comment = '04635_lance_virtual_predicate';

SELECT count()
FROM lance_local_count_record_batch
WHERE lower(name) = 'x'
FORMAT Null
SETTINGS
    enable_parallel_replicas = 0,
    optimize_count_from_files = 0,
    log_comment = '04635_lance_residual';

SELECT id, name
FROM lance_local_count_record_batch
FORMAT Null
SETTINGS
    enable_parallel_replicas = 0,
    max_threads = 4,
    max_block_size = 4,
    lance_scan_in_order = 0,
    lance_max_batch_sources = 4,
    log_comment = '04635_lance_mapping_cache';

SYSTEM FLUSH LOGS query_log;

SELECT throwIf(
    count() != 1
    OR any(ProfileEvents['LanceCountRows'] != 1)
    OR any(ProfileEvents['LanceCountSources'] != 1)
    OR any(ProfileEvents['LanceListFragmentsCalls'] != 0)
    OR any(ProfileEvents['LanceFragmentsListed'] != 0)
    OR any(ProfileEvents['LancePlanScan'] != 0)
    OR any(ProfileEvents['LanceProducerTasks'] != 0)
    OR any(ProfileEvents['LanceBatchSources'] != 0)
    OR any(ProfileEvents['LanceQueuePushBatches'] != 0)
    OR any(ProfileEvents['LanceQueuePopBatches'] != 0)
    OR any(ProfileEvents['LanceReadBytes'] != 0))
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04635_lance_fast_count'
FORMAT Null;

SELECT throwIf(
    count() != 1
    OR any(ProfileEvents['LanceCountRows'] != 1)
    OR any(ProfileEvents['LanceCountSources'] != 1)
    OR any(ProfileEvents['LanceProjectedColumns'] != 0)
    OR any(ProfileEvents['LancePlanScan'] != 0)
    OR any(ProfileEvents['LanceReadBytes'] != 0))
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04635_lance_zero_physical'
FORMAT Null;

SELECT throwIf(
    count() != 1
    OR any(ProfileEvents['LanceCountRows'] != 1)
    OR any(ProfileEvents['LanceCountSources'] != 1)
    OR any(ProfileEvents['LanceProjectedColumns'] != 0)
    OR any(ProfileEvents['LancePlanScan'] != 0)
    OR any(ProfileEvents['LanceReadBytes'] != 0))
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04635_lance_virtual_predicate'
FORMAT Null;

SELECT throwIf(
    count() != 1
    OR any(ProfileEvents['LanceCountRows'] != 0)
    OR any(ProfileEvents['LancePlanScan'] != 1)
    OR any(ProfileEvents['LanceProjectedColumns'] != 1))
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04635_lance_residual'
FORMAT Null;

SELECT throwIf(
    count() != 1
    OR any(ProfileEvents['LancePlanScan'] != 1)
    OR any(ProfileEvents['LanceBatchSourcesActive'] = 0)
    OR any(ProfileEvents['LanceArrowFieldMappingsBuilt'] != ProfileEvents['LanceBatchSourcesActive'])
    OR any(ProfileEvents['LanceBatchesRead'] <= ProfileEvents['LanceArrowFieldMappingsBuilt']))
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04635_lance_mapping_cache'
FORMAT Null;

DROP TABLE lance_local_count_record_batch;
