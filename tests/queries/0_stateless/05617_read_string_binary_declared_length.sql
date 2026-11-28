-- A binary string that declares 1073741708 bytes (varint 8CFFFFFF03) and carries none must not allocate them.
SELECT * FROM format(RowBinaryWithNames, 'x UInt8', unhex('018CFFFFFF03'))
    SETTINGS log_comment = 'read_string_binary_c1_header_name'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT * FROM format(RowBinaryWithNamesAndTypes, 'x UInt8', unhex('01017906537472696E678CFFFFFF03'))
    SETTINGS input_format_skip_unknown_fields = 1, log_comment = 'read_string_binary_c2_skipped_string'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('018CFFFFFF03') AS AggregateFunction(distinctDynamicTypes, Dynamic)))
    SETTINGS log_comment = 'read_string_binary_c3_distinct_dynamic_types'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('0000000000000000008CFFFFFF03') AS AggregateFunction(groupArraySample(1), UInt8)))
    SETTINGS log_comment = 'read_string_binary_c4_group_array_sample'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('018CFFFFFF03') AS AggregateFunction(distinctJSONPaths, JSON)))
    SETTINGS log_comment = 'read_string_binary_c5_distinct_json_paths'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('000000000000000000000000000000008CFFFFFF03') AS AggregateFunction(quantile, UInt8)))
    SETTINGS log_comment = 'read_string_binary_c6_quantile'; -- { serverError CANNOT_READ_ALL_DATA }

-- A column name and a skipped String value of 100000 bytes each, read through 4096-byte buffers.
INSERT INTO FUNCTION file(currentDatabase() || '_05617.bin', 'RawBLOB', 'data String')
    SELECT concat(unhex('02A08D06'), repeat('n', 100000), unhex('0178'), unhex('06'), 'String', unhex('05'), 'UInt8', unhex('A08D06'), repeat('v', 100000), unhex('2A'))
    SETTINGS engine_file_truncate_on_insert = 1;
SELECT x FROM file(currentDatabase() || '_05617.bin', 'RowBinaryWithNamesAndTypes', 'x UInt8')
    SETTINGS input_format_skip_unknown_fields = 1, max_read_buffer_size = 4096, storage_file_read_method = 'pread';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, memory_usage < 100000000
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND startsWith(log_comment, 'read_string_binary_c')
ORDER BY log_comment;
