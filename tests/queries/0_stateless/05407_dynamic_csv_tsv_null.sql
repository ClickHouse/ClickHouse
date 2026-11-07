-- Dynamic NULL markers in CSV and TSV.
SET enable_dynamic_type = 1;
SET input_format_parallel_parsing = 0;
SET input_format_csv_detect_header = 0;
SET input_format_tsv_detect_header = 0;
SET input_format_csv_empty_as_default = 0;
SET input_format_tsv_empty_as_default = 0;
SET input_format_csv_use_best_effort_in_schema_inference = 1;
SET input_format_tsv_use_best_effort_in_schema_inference = 1;
SET input_format_defaults_for_omitted_fields = 1;

-- Hex output distinguishes backslashes, control characters and empty strings.
-- Check the NULL and String types without depending on inferred integer widths.
SET input_format_null_as_default = 0;

SET format_csv_null_representation = '\\N';
SET format_tsv_null_representation = '\\N';

SELECT 'CSV: null_as_default=0, default marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        '1,\\N\n',
        '2,42\n',
        '3,\\N\n',
        '4,hello\n',
        '5,\\N\n',
        '6,"\\N"\n',
        '7,\\N_suffix\n',
        '8,0'))
ORDER BY id;

SELECT 'TSV: null_as_default=0, default marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        '1\t\\N\n',
        '2\t42\n',
        '3\t\\N\n',
        '4\thello\n',
        '5\t\\N\n',
        '6\t\\\\N\n',
        '7\t\\\\N_suffix\n',
        '8\t0'))
ORDER BY id;

SET format_csv_null_representation = 'missing';
SET format_tsv_null_representation = 'missing';

SELECT 'CSV: null_as_default=0, custom marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        '1,missing\n',
        '2,42\n',
        '3,missing\n',
        '4,hello\n',
        '5,missing\n',
        '6,"missing"\n',
        '7,missing_suffix\n',
        '8,0'))
ORDER BY id;

SELECT 'TSV: null_as_default=0, custom marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        '1\tmissing\n',
        '2\t42\n',
        '3\tmissing\n',
        '4\thello\n',
        '5\tmissing\n',
        '6\t\\x6Dissing\n',
        '7\tmissing_suffix\n',
        '8\t0'))
ORDER BY id;

SET format_csv_null_representation = '\\N';
SET format_tsv_null_representation = '\\N';

-- Preserve the missing-value marker used to evaluate a column DEFAULT.
SELECT 'CSV: DEFAULT 99, null_as_default=0';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic DEFAULT 99', concat(
        '1,\\N\n',
        '2,42\n',
        '3,\\N'))
ORDER BY id;

SELECT 'TSV: DEFAULT 99, null_as_default=0';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic DEFAULT 99', concat(
        '1\t\\N\n',
        '2\t42\n',
        '3\t\\N'))
ORDER BY id;

SET input_format_null_as_default = 1;

SET format_csv_null_representation = '\\N';
SET format_tsv_null_representation = '\\N';

SELECT 'CSV: null_as_default=1, default marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        '1,\\N\n',
        '2,42\n',
        '3,\\N\n',
        '4,hello\n',
        '5,\\N\n',
        '6,"\\N"\n',
        '7,\\N_suffix\n',
        '8,0'))
ORDER BY id;

SELECT 'TSV: null_as_default=1, default marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        '1\t\\N\n',
        '2\t42\n',
        '3\t\\N\n',
        '4\thello\n',
        '5\t\\N\n',
        '6\t\\\\N\n',
        '7\t\\\\N_suffix\n',
        '8\t0'))
ORDER BY id;

SET format_csv_null_representation = 'missing';
SET format_tsv_null_representation = 'missing';

SELECT 'CSV: null_as_default=1, custom marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        '1,missing\n',
        '2,42\n',
        '3,missing\n',
        '4,hello\n',
        '5,missing\n',
        '6,"missing"\n',
        '7,missing_suffix\n',
        '8,0'))
ORDER BY id;

SELECT 'TSV: null_as_default=1, custom marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        '1\tmissing\n',
        '2\t42\n',
        '3\tmissing\n',
        '4\thello\n',
        '5\tmissing\n',
        '6\t\\x6Dissing\n',
        '7\tmissing_suffix\n',
        '8\t0'))
ORDER BY id;

SET format_csv_null_representation = '\\N';
SET format_tsv_null_representation = '\\N';

-- Preserve the missing-value marker used to evaluate a column DEFAULT.
SELECT 'CSV: DEFAULT 99, null_as_default=1';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic DEFAULT 99', concat(
        '1,\\N\n',
        '2,42\n',
        '3,\\N'))
ORDER BY id;

SELECT 'TSV: DEFAULT 99, null_as_default=1';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic DEFAULT 99', concat(
        '1\t\\N\n',
        '2\t42\n',
        '3\t\\N'))
ORDER BY id;

-- Null recognition must not depend on best-effort type inference.
SET input_format_null_as_default = 0;
SET input_format_csv_use_best_effort_in_schema_inference = 0;
SET input_format_tsv_use_best_effort_in_schema_inference = 0;
SELECT 'CSV: inference disabled';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        '1,\\N\n',
        '2,42\n',
        '3,"\\N"'))
ORDER BY id;

SELECT 'TSV: inference disabled';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        '1\t\\N\n',
        '2\t42\n',
        '3\t\\\\N'))
ORDER BY id;

SET input_format_csv_use_best_effort_in_schema_inference = 1;
SET input_format_tsv_use_best_effort_in_schema_inference = 1;

-- Empty custom null markers must not consume nonempty or escaped literal fields.
SET format_csv_null_representation = '';
SET format_tsv_null_representation = '';
SELECT 'CSV: empty null marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        '1,\n',
        '2,""\n',
        '3,42\n',
        '4,'))
ORDER BY id;

SELECT 'TSV: empty null marker';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        '1\t\n',
        '2\t\\\\N\n',
        '3\t42\n',
        '4\t'))
ORDER BY id;

SET format_csv_null_representation = '\\N';
SET format_tsv_null_representation = '\\N';

-- Export and import native NULLs, literal null markers and escaped control characters.
SELECT 'CSV: round trip';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(CSV, 'id UInt8, d Dynamic', concat(
        formatRow('CSV', toUInt8(1), CAST(NULL, 'Dynamic')),
        formatRow('CSV', toUInt8(2), CAST(42, 'Dynamic')),
        formatRow('CSV', toUInt8(3), CAST(NULL, 'Dynamic')),
        formatRow('CSV', toUInt8(4), CAST('\\N', 'Dynamic')),
        formatRow('CSV', toUInt8(5), CAST('a\tb\nc', 'Dynamic')),
        formatRow('CSV', toUInt8(6), CAST('', 'Dynamic'))))
ORDER BY id;

SELECT 'TSV: round trip';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', concat(
        formatRow('TSV', toUInt8(1), CAST(NULL, 'Dynamic')),
        formatRow('TSV', toUInt8(2), CAST(42, 'Dynamic')),
        formatRow('TSV', toUInt8(3), CAST(NULL, 'Dynamic')),
        formatRow('TSV', toUInt8(4), CAST('\\N', 'Dynamic')),
        formatRow('TSV', toUInt8(5), CAST('a\tb\nc', 'Dynamic')),
        formatRow('TSV', toUInt8(6), CAST('', 'Dynamic'))))
ORDER BY id;

-- Preserve NULL markers and escaped strings with CRLF row endings too.
SET input_format_tsv_crlf_end_of_line = 1;
SELECT 'TSV: CRLF';
SELECT id, isNull(d), dynamicType(d) = 'None' AS has_null_type, dynamicType(d) = 'String' AS is_string,
    concat('0x', hex(ifNull(d::Nullable(String), '<NULL>'))) AS value_hex
FROM format(TSV, 'id UInt8, d Dynamic', '1\t\\N\r\n2\t42\r\n3\t\\\\N\r\n')
ORDER BY id;

SET input_format_tsv_crlf_end_of_line = 0;
