-- A Native column header whose strings declare 1073741708 bytes (varint 8CFFFFFF03, under the 1 GiB limit) and carry none.
SELECT * FROM format(Native, 'x UInt8', unhex('01018CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'native_header_c1_name'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(Native, 'x UInt8', unhex('010101788CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'native_header_c2_type_name'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
-- Binary-encoded types: 0x12 = DateTime with a time zone; 0x25 = AggregateFunction, version 0, `sequenceMatch`, one String parameter (Field 0x0C).
SELECT * FROM format(Native, 'x DateTime', unhex('01010178128CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 1, output_format_native_encode_types_in_binary_format = 1, log_comment = 'native_header_c3_binary_type'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(Native, 'x UInt8', unhex('0101017825000D73657175656E63654D61746368010C8CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 1, output_format_native_encode_types_in_binary_format = 1, log_comment = 'native_header_c4_type_parameter'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
-- Dynamic and JSON columns: the structure prefix declares a variant type name or a path of 1073741708 bytes.
SELECT * FROM format(Native, 'x Dynamic', unhex('010101780744796E616D6963010000000000000001018CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'native_header_c5_dynamic_type_name'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(Native, 'x Dynamic', unhex('010101780744796E616D69630300000000000000018CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'native_header_c6_dynamic_flattened_type_name'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(Native, 'x JSON', unhex('01010178044A534F4E000000000000000001018CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'native_header_c7_json_dynamic_path'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT * FROM format(Native, 'x JSON', unhex('01010178044A534F4E0300000000000000018CFFFFFF03'))
    SETTINGS input_format_native_decode_types_in_binary_format = 0, output_format_native_encode_types_in_binary_format = 0, log_comment = 'native_header_c8_json_flattened_path'; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
-- A complete block with a binary-encoded DateTime('UTC') still decodes.
SELECT x, toTypeName(x) FROM format(Native, unhex('01010178120355544300000000'))
    SETTINGS input_format_native_decode_types_in_binary_format = 1, output_format_native_encode_types_in_binary_format = 1;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, memory_usage < 100000000
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND startsWith(log_comment, 'native_header_c')
ORDER BY log_comment;
