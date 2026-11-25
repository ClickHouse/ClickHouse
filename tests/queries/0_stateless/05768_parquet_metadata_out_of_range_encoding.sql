-- Tags: no-fasttest
-- Random settings limits: output_format_parquet_max_dictionary_size=(1048576, None)
-- no-fasttest: the ParquetMetadata format needs the Arrow library

-- The ParquetMetadata format shows a column chunk encoding that is not a known Parquet encoding as `UNDEFINED`.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05768_plain.parquet') SELECT 1::Int32 AS bad_encoding;

-- Each file patches one value of the column chunk's `encodings` list [PLAIN, RLE_DICTIONARY] (compact protocol: list
-- header, zigzag values). The writer stores the column chunk twice, after its data and in the footer, so patch both.
-- PLAIN -> -1
INSERT INTO FUNCTION file(currentDatabase() || '_05768_minus_one.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('25001019180C') || 'bad_encoding', unhex('25011019180C') || 'bad_encoding')
FROM file(currentDatabase() || '_05768_plain.parquet', RawBLOB, 'raw String');
-- RLE_DICTIONARY -> -9
INSERT INTO FUNCTION file(currentDatabase() || '_05768_minus_nine.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('25001019180C') || 'bad_encoding', unhex('25001119180C') || 'bad_encoding')
FROM file(currentDatabase() || '_05768_plain.parquet', RawBLOB, 'raw String');
-- PLAIN -> 42
INSERT INTO FUNCTION file(currentDatabase() || '_05768_forty_two.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('25001019180C') || 'bad_encoding', unhex('25541019180C') || 'bad_encoding')
FROM file(currentDatabase() || '_05768_plain.parquet', RawBLOB, 'raw String');
-- PLAIN -> ALP (10), a known encoding
INSERT INTO FUNCTION file(currentDatabase() || '_05768_alp.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('25001019180C') || 'bad_encoding', unhex('25141019180C') || 'bad_encoding')
FROM file(currentDatabase() || '_05768_plain.parquet', RawBLOB, 'raw String');

-- Every patch landed in both copies.
SELECT
    (SELECT (countSubstrings(raw, unhex('25001019180C') || 'bad_encoding'), countSubstrings(raw, unhex('25011019180C') || 'bad_encoding')) FROM file(currentDatabase() || '_05768_minus_one.parquet', RawBLOB, 'raw String')),
    (SELECT (countSubstrings(raw, unhex('25001019180C') || 'bad_encoding'), countSubstrings(raw, unhex('25001119180C') || 'bad_encoding')) FROM file(currentDatabase() || '_05768_minus_nine.parquet', RawBLOB, 'raw String')),
    (SELECT (countSubstrings(raw, unhex('25001019180C') || 'bad_encoding'), countSubstrings(raw, unhex('25541019180C') || 'bad_encoding')) FROM file(currentDatabase() || '_05768_forty_two.parquet', RawBLOB, 'raw String')),
    (SELECT (countSubstrings(raw, unhex('25001019180C') || 'bad_encoding'), countSubstrings(raw, unhex('25141019180C') || 'bad_encoding')) FROM file(currentDatabase() || '_05768_alp.parquet', RawBLOB, 'raw String'));

SELECT tupleElement(columns[1], 'encodings') FROM file(currentDatabase() || '_05768_plain.parquet', ParquetMetadata);
SELECT tupleElement(columns[1], 'encodings') FROM file(currentDatabase() || '_05768_minus_one.parquet', ParquetMetadata);
SELECT tupleElement(columns[1], 'encodings') FROM file(currentDatabase() || '_05768_minus_nine.parquet', ParquetMetadata);
SELECT tupleElement(columns[1], 'encodings') FROM file(currentDatabase() || '_05768_forty_two.parquet', ParquetMetadata);
SELECT tupleElement(columns[1], 'encodings') FROM file(currentDatabase() || '_05768_alp.parquet', ParquetMetadata);
