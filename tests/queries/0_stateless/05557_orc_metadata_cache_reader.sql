-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: ORC support is not built in fast-test images.
-- no-parallel: this test clears the process-wide ORC metadata cache.
-- no-parallel-replicas: profile events are local to each replica.
SET engine_file_truncate_on_insert = 1;
SET use_cache_for_count_from_files = 0;
SET optimize_count_from_files = 0;
SET input_format_orc_filter_push_down = 1;
SET output_format_orc_compression_method = 'zstd';
SET max_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
SELECT number AS id, toString(number) AS name FROM numbers(1000);
-- Local file versions become usable cache identities only after settling.
SELECT sleepEachRow(4) FROM numbers(1) SETTINGS function_sleep_max_microseconds_per_block = 4000000 FORMAT Null;
SYSTEM DROP ORC METADATA CACHE;

SELECT sum(id), sum(length(name)) FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199 SETTINGS use_orc_metadata_cache = 1, log_comment = 'orc-cold';
SELECT sum(id), sum(length(name)) FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199 SETTINGS use_orc_metadata_cache = 1, log_comment = 'orc-warm';
SELECT sum(id), sum(length(name)) FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199 SETTINGS use_orc_metadata_cache = 1, input_format_allow_seeks = 0, log_comment = 'orc-unseekable';
SELECT sum(id), sum(length(name)) FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199 SETTINGS use_orc_metadata_cache = 0, log_comment = 'orc-disabled';

SET output_format_orc_compression_method = 'none';
INSERT INTO FUNCTION file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
SELECT number + 1000 AS id, toString(number + 1000) AS name FROM numbers(1000);
SELECT sleepEachRow(4) FROM numbers(1) SETTINGS function_sleep_max_microseconds_per_block = 4000000 FORMAT Null;

SELECT sum(id), sum(length(name)) FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199 SETTINGS use_orc_metadata_cache = 1, input_format_allow_seeks = 0, log_comment = 'orc-replaced-cold';
SELECT sum(id), sum(length(name)) FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199 SETTINGS use_orc_metadata_cache = 1, log_comment = 'orc-replaced-warm';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'orc-%'
ORDER BY log_comment;
