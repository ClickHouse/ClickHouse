-- Tags: no-fasttest, no-parallel-replicas
-- ORC support is not built in fast-test images.
-- Profile events of the ORC cache are recorded on the local server.

SET engine_file_truncate_on_insert = 1;
SET use_cache_for_count_from_files = 0;
SET optimize_count_from_files = 0;
SET use_orc_metadata_cache = 1;
SET input_format_orc_filter_push_down = 1;
SET output_format_orc_row_index_stride = 100;
SET output_format_orc_compression_method = 'zstd';

INSERT INTO FUNCTION file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
SELECT number AS id, toString(number) AS name FROM numbers(1000);

SELECT sleepEachRow(4) FROM numbers(1) SETTINGS function_sleep_max_microseconds_per_block = 4000000 FORMAT Null;

SYSTEM DROP ORC METADATA CACHE;

-- Read actual column data with a predicate on both a cache miss and a cache hit.
SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199
SETTINGS log_comment = '05557-cold';

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199
SETTINGS log_comment = '05557-warm';

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199
SETTINGS input_format_allow_seeks = 0, log_comment = '05557-warm-unseekable';

-- Replacing the file must invalidate its cached footer, including the compression kind.
SET output_format_orc_compression_method = 'none';
INSERT INTO FUNCTION file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
SELECT number + 1000 AS id, toString(number + 1000) AS name FROM numbers(1000);

SELECT sleepEachRow(4) FROM numbers(1) SETTINGS function_sleep_max_microseconds_per_block = 4000000 FORMAT Null;

-- Populate the new entry through the non-seekable path, then reuse it with seeks enabled.
SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199
SETTINGS input_format_allow_seeks = 0, log_comment = '05557-replaced-cold';

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199
SETTINGS log_comment = '05557-replaced-warm';

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199
SETTINGS use_orc_metadata_cache = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('05557-cold', '05557-warm', '05557-warm-unseekable', '05557-replaced-cold', '05557-replaced-warm')
ORDER BY log_comment;
