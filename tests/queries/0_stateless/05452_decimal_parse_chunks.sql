-- Chunk boundaries, deferred fractional zeroes, truncation and exponents for each decimal width.
SELECT 'Decimal32', count(), countIf(toString(toDecimal32(input, 3)) != expected)
FROM values('input String, expected String',
    ('123456.7899', '123456.789'),
    ('-123456.7899', '-123456.789'),
    ('000001.230000', '1.23'),
    ('0.0009', '0'),
    ('1.002003', '1.002'),
    ('123456789e-3', '123456.789'),
    ('1e-100', '0'),
    ('999999.999', '999999.999'));

SELECT 'Decimal64', count(), countIf(toString(toDecimal64(input, 9)) != expected)
FROM values('input String, expected String',
    ('123456789.1234567899', '123456789.123456789'),
    ('-123456789.1234567899', '-123456789.123456789'),
    ('000000000123456789.000000001', '123456789.000000001'),
    ('0.0000000009', '0'),
    ('123456789123456789e-9', '123456789.123456789'),
    ('0.000000001000000000', '0.000000001'));

SELECT 'Decimal128', count(), countIf(toString(toDecimal128(input, 18)) != expected)
FROM values('input String, expected String',
    ('12345678901234567890.1234567890123456789', '12345678901234567890.123456789012345678'),
    ('-12345678901234567890.1234567890123456789', '-12345678901234567890.123456789012345678'),
    ('0.000000000000000001000', '0.000000000000000001'),
    ('00000123456789012345678.123456789012345678', '123456789012345678.123456789012345678'),
    ('12345678901234567890123456789012345678e-18', '12345678901234567890.123456789012345678'),
    ('1.00000000000000000000000000000000000009', '1'));

SELECT 'Decimal256', count(), countIf(toString(toDecimal256(input, 38)) != expected)
FROM values('input String, expected String',
    ('12345678901234567890123456789012345678.12345678901234567890123456789012345678',
     '12345678901234567890123456789012345678.12345678901234567890123456789012345678'),
    ('-12345678901234567890123456789012345678.123456789012345678901234567890123456789',
     '-12345678901234567890123456789012345678.12345678901234567890123456789012345678'),
    ('0.12345678901234567001', '0.12345678901234567001'),
    ('0.12345678901234567000', '0.12345678901234567'),
    ('-123456789012345678901234567890123456789e-1', '-12345678901234567890123456789012345678.9'),
    ('1.00000000000000000000000000000000000001', '1.00000000000000000000000000000000000001'),
    ('0.00000000000000000000000000000000000000000000000000000001',
     '0'),
    ('99999999999999999999999999999999999999.999999999999999999999999999999999999999',
     '99999999999999999999999999999999999999.99999999999999999999999999999999999999'));

SELECT
    isNull(toDecimal32OrNull('1000000', 3)),
    isNull(toDecimal64OrNull('1000000000', 9)),
    isNull(toDecimal128OrNull('100000000000000000000', 18)),
    isNull(toDecimal256OrNull('100000000000000000000000000000000000000', 38));

-- Delimiters and quoted fields must remain available to the enclosing format reader.
SELECT 'CSV', count(), sum(x) = toDecimal256(1, 38), sum(id) = 3
FROM format(CSV, 'x Decimal256(38), id UInt8',
    '"12345678901234567890123456789012345678.12345678901234567890123456789012345678",1\n"-12345678901234567890123456789012345677.12345678901234567890123456789012345678",2\n');

SELECT 'TSV', count(), sum(x) = toDecimal256(1, 38), sum(id) = 3
FROM format(TSV, 'x Decimal256(38), id UInt8',
    '12345678901234567890123456789012345678.12345678901234567890123456789012345678\t1\n-12345678901234567890123456789012345677.12345678901234567890123456789012345678\t2\n');

-- Unlike `format`, `file` refills its input buffer. The first TSV value's fractional
-- chunk ends in deferred zeroes at each of the 17/18/19-byte boundaries, before the final 1.
INSERT INTO FUNCTION file(currentDatabase() || '_decimal_parse_chunks.tsv', 'RawBLOB')
SELECT '.1234567890123400001\t17\tzeroes\n-123456789012345678901234567890123456789e-1\t29\texponent\n1.00000000000000000000000000000000000001\t43\tfraction\n'
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_decimal_parse_chunks.csv', 'CSV')
SELECT * FROM file(currentDatabase() || '_decimal_parse_chunks.tsv', 'TSV', 'x String, id UInt8, tag String')
SETTINGS engine_file_truncate_on_insert = 1, input_format_tsv_detect_header = 0;

SET storage_file_read_method = 'pread', input_format_parallel_parsing = 0,
    input_format_tsv_detect_header = 0, input_format_csv_detect_header = 0;

CREATE TEMPORARY TABLE decimal_refill_expected ENGINE = Memory AS
SELECT * FROM file(currentDatabase() || '_decimal_parse_chunks.tsv', 'TSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 65536;

-- Pin exact values and subsequent fields independently of the small-buffer comparisons.
SELECT toString(x), id, tag FROM decimal_refill_expected ORDER BY id;

SELECT 'TSV', 17, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.tsv', 'TSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 17;

SELECT 'TSV', 18, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.tsv', 'TSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 18;

SELECT 'TSV', 19, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.tsv', 'TSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 19;

SELECT 'CSV', 65536, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.csv', 'CSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 65536;

SELECT 'CSV', 17, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.csv', 'CSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 17;

SELECT 'CSV', 18, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.csv', 'CSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 18;

SELECT 'CSV', 19, arraySort(groupArray(tuple(x, id, tag))) =
    (SELECT arraySort(groupArray(tuple(x, id, tag))) FROM decimal_refill_expected)
FROM file(currentDatabase() || '_decimal_parse_chunks.csv', 'CSV', 'x Decimal256(38), id UInt8, tag String')
SETTINGS max_read_buffer_size = 19;

DROP TABLE decimal_refill_expected;
