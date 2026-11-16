-- Tags: no-fasttest
-- ORC support is not built in fast-test images.

SET engine_file_truncate_on_insert = 1;
SET use_orc_metadata_cache = 1;
SET input_format_orc_filter_push_down = 1;
SET output_format_orc_row_index_stride = 100;
SET output_format_orc_compression_method = 'zstd';

INSERT INTO FUNCTION file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
SELECT number AS id, toString(number) AS name FROM numbers(1000);

-- Read actual column data with a predicate on both a cache miss and a cache hit.
SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199;

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199;

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 100 AND 199
SETTINGS input_format_allow_seeks = 0;

-- Replacing the file must invalidate its cached footer, including the compression kind.
SET output_format_orc_compression_method = 'none';
INSERT INTO FUNCTION file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
SELECT number + 1000 AS id, toString(number + 1000) AS name FROM numbers(1000);

-- Populate the new entry through the non-seekable path, then reuse it with seeks enabled.
SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199
SETTINGS input_format_allow_seeks = 0;

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199;

SELECT sum(id), sum(length(name))
FROM file(currentDatabase() || '_orc_metadata_cache_reader.orc', ORC)
WHERE id BETWEEN 1100 AND 1199
SETTINGS use_orc_metadata_cache = 0;
