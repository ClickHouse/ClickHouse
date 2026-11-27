-- Random settings limits: input_format_parquet_memory_high_watermark=(67108864, None)

-- The same condition repeats below. Without this setting, the query condition cache can skip the row groups instead of the bloom filter or the dictionary.
SET use_query_condition_cache = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_uuid.parquet')
SELECT reinterpretAsUUID(sipHash128(toString(number))) AS id, toFixedString(UUIDStringToNum(toString(id)), 16) AS raw, number AS n
FROM numbers(4000)
SETTINGS output_format_parquet_row_group_size = 1000, max_block_size = 1000, output_format_parquet_write_bloom_filter = 1,
    output_format_parquet_bloom_filter_bits_per_value = 10.5, output_format_parquet_max_dictionary_size = 1048576;

SELECT n FROM file(currentDatabase() || '_uuid.parquet') WHERE id = reinterpretAsUUID(sipHash128('2500'))
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 0, log_comment = 'bloom';
SELECT n FROM file(currentDatabase() || '_uuid.parquet') WHERE id = reinterpretAsUUID(sipHash128('2500'))
SETTINGS input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 1048576, log_comment = 'dictionary';
SELECT n FROM file(currentDatabase() || '_uuid.parquet', Parquet, 'id String, n UInt64') WHERE id = toString(reinterpretAsUUID(sipHash128('2500')))
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576, log_comment = 'String hint';
SELECT n FROM file(currentDatabase() || '_uuid.parquet', Parquet, 'raw UUID, n UInt64') WHERE raw = reinterpretAsUUID(sipHash128('2500'))
SETTINGS input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 0, log_comment = 'raw FLBA, UUID hint';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetBloomFilterPrunedRowGroups'], ProfileEvents['ParquetDictionaryFilterPrunedRowGroups']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('bloom', 'dictionary', 'String hint', 'raw FLBA, UUID hint')
ORDER BY event_time_microseconds;
