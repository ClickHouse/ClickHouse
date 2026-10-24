-- Tags: no-fasttest
-- Tag no-fasttest: Parquet is not built in fasttest

-- Old versions of parquet-cpp wrote a column of the Arrow null type with converted type 24 and no logical type.
-- The value is not in parquet.thrift. Such a column holds only nulls and is read by its physical type, like one with
-- the null logical type; the file must not be rejected.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(concat(currentDatabase(), '_05450_plain.parquet'), Parquet)
SELECT toInt32(number + 1) AS x, NULL::Nullable(UInt32) AS nullcol FROM numbers(3);

-- Replace converted type UINT_32 and logical type INTEGER of `nullcol` with converted type 24, then fix the footer length.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_05450_legacy.parquet'), RawBLOB)
WITH
    replaceOne(raw, 'nullcol' || unhex('251A4CAC1320120000'), 'nullcol' || unhex('2530')) AS patched,
    reinterpretAsUInt32(substring(raw, length(raw) - 7, 4)) - 7 AS footer_len
SELECT substring(patched, 1, length(patched) - 8)
    || char(footer_len % 256, intDiv(footer_len, 256) % 256, intDiv(footer_len, 65536) % 256, intDiv(footer_len, 16777216))
    || 'PAR1'
FROM file(concat(currentDatabase(), '_05450_plain.parquet'), RawBLOB, 'raw String');

SELECT countSubstrings(raw, 'nullcol' || unhex('253000')) FROM file(concat(currentDatabase(), '_05450_legacy.parquet'), RawBLOB, 'raw String');

SELECT x FROM file(concat(currentDatabase(), '_05450_legacy.parquet'), Parquet, 'x Int32') ORDER BY x;
SELECT count() FROM file(concat(currentDatabase(), '_05450_legacy.parquet'), Parquet, 'x Int32');
DESC file(concat(currentDatabase(), '_05450_legacy.parquet'));
SELECT * FROM file(concat(currentDatabase(), '_05450_legacy.parquet')) ORDER BY x;
