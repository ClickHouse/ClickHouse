-- Tags: no-fasttest
-- Tag no-fasttest: depends on libarchive, minizip, Parquet and S3

-- dup.zip: three entries `a` with 4, 2 and 1 JSON lines.
-- dup.tar.gz: two entries `a.parquet`, with column `x` (1..4) and with column `y` (5, 6).
-- skip.zip: entries `a`, `b` and `a`, read with a glob that skips `b`.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip', 'test', 'testtest', 'RawBLOB', 'x String', 'none') SELECT unhex('504b030414000000000000002100718ee2f2200000002000000001000000617b2278223a317d0a7b2278223a327d0a7b2278223a337d0a7b2278223a347d0a504b03041400000000000000210094b9e414100000001000000001000000617b2279223a357d0a7b2279223a367d0a504b030414000000000000002100ae4c84c00e0000000e00000001000000617b2278223a372c2279223a387d0a504b0102140314000000000000002100718ee2f2200000002000000001000000000000000000000080010000000061504b010214031400000000000000210094b9e414100000001000000001000000000000000000000080013f00000061504b0102140314000000000000002100ae4c84c00e0000000e00000001000000000000000000000080016e00000061504b050600000000030003008d0000009b0000000000') SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.tar.gz', 'test', 'testtest', 'RawBLOB', 'x String', 'none') SELECT unhex('1f8b0800000000000203edd3bf4f83401407f08352c24835474ac2704b494d48c3d14269e2a22e4dec609c5c81d2d858ad426bacb3a37f8aff838b7f8a7f88efa83feb62d2b4b1fa3ee171e4ee719740be51e332caaea6e984ac8e0b8256ab18c1e2e8fa9ef7fe3c9f6ffb5e9b3097acc1349f441963e49f3ada3be654a18cb21ed5284c48503254094a81a204e635076e3a55a96a9d2862b6aa88be4a4534979e897d2f5954366b4437abd20d258666dc4970d9fbb6b6f886e9c07984ca62671d865d7188623a5d354f4ed3f348acc83502fbd48cdec3b6b605eb86665aa6b5ec29a25bb3c54ba47e78301a2667ddf1344fd9759ae5c3f105f38206771b9cd5e3e970d467fd811fc69e1b72370e9abc99f0b43988fdd8eb74222f09021e719ec461d8df312d0b767f7afd9a9bf5ffa3df907f977fcf7f0bf3bfc6fceb54ef89c010528652e7b987b83bc5da3cf76a91aff25beea5926cdf7ee4710679548cccc86caf48e397ee1f677ef629f34a91f9658e805e05f29ead22ee8f1b197784104208218410420821841042082184104208fd412fa0816fb200280000') SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05403_' || currentDatabase() || '_skip.zip', 'test', 'testtest', 'RawBLOB', 'x String', 'none') SELECT unhex('504b030414000000000000002100195067f5100000001000000001000000617b2278223a317d0a7b2278223a327d0a504b030414000000000000002100293b3b1c080000000800000001000000627b227a223a307d0a504b030414000000000000002100def7e998080000000800000001000000617b2279223a337d0a504b0102140314000000000000002100195067f5100000001000000001000000000000000000000080010000000061504b0102140314000000000000002100293b3b1c080000000800000001000000000000000000000080012f00000062504b0102140314000000000000002100def7e998080000000800000001000000000000000000000080015600000061504b050600000000030003008d0000007d0000000000') SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05403_dup.zip', 'RawBLOB', 'x String', 'none') SELECT unhex('504b030414000000000000002100718ee2f2200000002000000001000000617b2278223a317d0a7b2278223a327d0a7b2278223a337d0a7b2278223a347d0a504b03041400000000000000210094b9e414100000001000000001000000617b2279223a357d0a7b2279223a367d0a504b030414000000000000002100ae4c84c00e0000000e00000001000000617b2278223a372c2279223a387d0a504b0102140314000000000000002100718ee2f2200000002000000001000000000000000000000080010000000061504b010214031400000000000000210094b9e414100000001000000001000000000000000000000080013f00000061504b0102140314000000000000002100ae4c84c00e0000000e00000001000000000000000000000080016e00000061504b050600000000030003008d0000009b0000000000') SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05403_skip.zip', 'RawBLOB', 'x String', 'none') SELECT unhex('504b030414000000000000002100195067f5100000001000000001000000617b2278223a317d0a7b2278223a327d0a504b030414000000000000002100293b3b1c080000000800000001000000627b227a223a307d0a504b030414000000000000002100def7e998080000000800000001000000617b2279223a337d0a504b0102140314000000000000002100195067f5100000001000000001000000000000000000000080010000000061504b0102140314000000000000002100293b3b1c080000000800000001000000000000000000000080012f00000062504b0102140314000000000000002100def7e998080000000800000001000000000000000000000080015600000061504b050600000000030003008d0000007d0000000000') SETTINGS engine_file_truncate_on_insert = 1;

-- The schema and row count caches drop an entry registered in the second the object was modified.
SELECT sleep(1) FORMAT Null;

SELECT 's3 lines', _size, count(), arraySort(groupArray(line)) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'LineAsString') GROUP BY _size ORDER BY _size;

SELECT 's3 count', count() FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'LineAsString') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1;
SELECT 's3 count', count() FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'LineAsString') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1, log_comment = 'cache_hits_s3_count';
SELECT 'file count', count() FROM file(currentDatabase() || '_05403_dup.zip :: *', 'LineAsString') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1;
SELECT 'file count', count() FROM file(currentDatabase() || '_05403_dup.zip :: *', 'LineAsString') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1, log_comment = 'cache_hits_file_count';

SELECT 's3 union', count(), sum(x), sum(y) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'JSONEachRow') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_s3 = 1;
SELECT 's3 union', count(), sum(x), sum(y) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'JSONEachRow') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_s3 = 1, log_comment = 'cache_hits_s3_union';
SELECT 'file union', count(), sum(x), sum(y) FROM file(currentDatabase() || '_05403_dup.zip :: *', 'JSONEachRow') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_file = 1;
SELECT 'file union', count(), sum(x), sum(y) FROM file(currentDatabase() || '_05403_dup.zip :: *', 'JSONEachRow') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_file = 1, log_comment = 'cache_hits_file_union';
SELECT DISTINCT 's3 cache', format, extract(source, '^#[0-9]+#') AS position, if(format = 'LineAsString', number_of_rows, NULL) AS rows, match(schema, '\\bx ') AS has_x, match(schema, '\\by ') AS has_y FROM system.schema_inference_cache WHERE storage = 'S3' AND endsWith(source, '05403_' || currentDatabase() || '_dup.zip::a') AND format IN ('LineAsString', 'JSONEachRow') ORDER BY ALL;
SELECT DISTINCT 'file cache', format, extract(source, '^#[0-9]+#') AS position, if(format = 'LineAsString', number_of_rows, NULL) AS rows, match(schema, '\\bx ') AS has_x, match(schema, '\\by ') AS has_y FROM system.schema_inference_cache WHERE storage = 'File' AND endsWith(source, '/' || currentDatabase() || '_05403_dup.zip::a') AND format IN ('LineAsString', 'JSONEachRow') ORDER BY ALL;
SELECT 's3 auto', count(), sum(x), sum(y) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_s3 = 0;

SELECT 's3 parquet', _size, count(), sum(x), sum(y) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.tar.gz :: *', 'test', 'testtest', 'Parquet') GROUP BY _size ORDER BY _size SETTINGS schema_inference_mode = 'union', use_parquet_metadata_cache = 1;
SELECT 's3 parquet', _size, count(), sum(x), sum(y) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_dup.tar.gz :: *', 'test', 'testtest', 'Parquet') GROUP BY _size ORDER BY _size SETTINGS schema_inference_mode = 'union', use_parquet_metadata_cache = 1, log_comment = 'cache_hits_s3_parquet';

SELECT 's3 skip', count(), sum(x), sum(y) FROM s3('http://localhost:11111/test/05403_' || currentDatabase() || '_skip.zip :: a*', 'test', 'testtest', 'JSONEachRow') SETTINGS schema_inference_mode = 'union';
SELECT 'file skip count', count() FROM file(currentDatabase() || '_05403_skip.zip :: a*', 'LineAsString') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1;
SELECT 'file skip count', count() FROM file(currentDatabase() || '_05403_skip.zip :: a*', 'LineAsString') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1, log_comment = 'cache_hits_file_skip_count';
SELECT 'file skip union', count(), sum(x), sum(y) FROM file(currentDatabase() || '_05403_skip.zip :: a*', 'JSONEachRow') SETTINGS schema_inference_mode = 'union', schema_inference_use_cache_for_file = 1;

SELECT 's3Cluster lines', _size, count(), arraySort(groupArray(line)) FROM s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'LineAsString') GROUP BY _size ORDER BY _size SETTINGS cluster_function_process_archive_on_multiple_nodes = 1;
SELECT 's3Cluster parquet', _size, count(), sum(x), sum(y) FROM s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/05403_' || currentDatabase() || '_dup.tar.gz :: *', 'test', 'testtest', 'Parquet') GROUP BY _size ORDER BY _size SETTINGS cluster_function_process_archive_on_multiple_nodes = 1, schema_inference_mode = 'union';
-- Whole archives can be handed out more than once, so only distinct lines are compared.
SELECT 's3Cluster whole archive', _size, arraySort(groupUniqArray(line)) FROM s3Cluster('test_cluster_two_shards', 'http://localhost:11111/test/05403_' || currentDatabase() || '_dup.zip :: *', 'test', 'testtest', 'LineAsString') GROUP BY _size ORDER BY _size SETTINGS cluster_function_process_archive_on_multiple_nodes = 0;
SYSTEM FLUSH LOGS query_log;
SELECT 'cache hits', i.label, sum(l.ProfileEvents[i.event]) >= i.entries
FROM system.query_log AS l
INNER JOIN
(
    SELECT query_id, replaceOne(log_comment, 'cache_hits_', '') AS label,
        multiIf(endsWith(label, 'count'), 'SchemaInferenceCacheNumRowsHits', endsWith(label, 'union'), 'SchemaInferenceCacheSchemaHits', 'ParquetMetadataCacheHits') AS event,
        if(label IN ('file_skip_count', 's3_parquet'), 2, 3) AS entries
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND startsWith(log_comment, 'cache_hits_')
) AS i ON l.initial_query_id = i.query_id
WHERE l.event_date >= yesterday() AND l.type = 'QueryFinish'
GROUP BY i.label, i.event, i.entries
ORDER BY i.label;
