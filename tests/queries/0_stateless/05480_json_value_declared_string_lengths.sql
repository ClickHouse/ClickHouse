-- A JSON value whose string or path declares 1073741708 bytes (varint 8CFFFFFF03, under the 1 GiB limit) and carries none.
SELECT * FROM format(Native, 'x JSON', unhex('01010178044A534F4E01000000000000008CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'json_value_j1_native_string'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(Native, 'x Dynamic', unhex('010101780744796E616D696301000000000000000101044A534F4E00000000000000000100000000000000008CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'json_value_j2_dynamic_native_string'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(RowBinary, 'x JSON', unhex('8CFFFFFF03'))
    SETTINGS input_format_binary_read_json_as_string = 1, log_comment = 'json_value_j3_row_binary_string'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(RowBinary, 'x JSON', unhex('018CFFFFFF03'))
    SETTINGS log_comment = 'json_value_j4_row_binary_path'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
-- The unknown column `x JSON` is skipped.
SELECT * FROM format(RowBinaryWithNamesAndTypes, 'y UInt8', unhex('0201780179044A534F4E0555496E7438018CFFFFFF03'))
    SETTINGS input_format_skip_unknown_fields = 1, input_format_with_names_use_header = 1, input_format_with_types_use_header = 1, log_comment = 'json_value_j5_skipped_column_path'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, memory_usage < 100000000
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND startsWith(log_comment, 'json_value_j')
ORDER BY log_comment;
