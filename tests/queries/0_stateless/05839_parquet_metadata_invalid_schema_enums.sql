-- Tags: no-fasttest
-- no-fasttest: the ParquetMetadata format needs the Parquet library

-- The ParquetMetadata format reads the enums of the Parquet schema that are out of range without undefined behavior.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05839_good.parquet') SELECT 1::Int32 AS bad_enum;

-- In the footer, the element of the column is: `type` = INT32 (field header 0x15, zigzag value 2), `repetition_type` =
-- REQUIRED (field header 0x25, value 0), `name`. Replace a value with the zigzag-encoded 1, which is -1.
INSERT INTO FUNCTION file(currentDatabase() || '_05839_bad_type.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('150225001808') || 'bad_enum', unhex('150125001808') || 'bad_enum')
FROM file(currentDatabase() || '_05839_good.parquet', RawBLOB, 'raw String');

INSERT INTO FUNCTION file(currentDatabase() || '_05839_bad_repetition_type.parquet', RawBLOB)
SELECT replaceAll(raw, unhex('150225001808') || 'bad_enum', unhex('150225011808') || 'bad_enum')
FROM file(currentDatabase() || '_05839_good.parquet', RawBLOB, 'raw String');

-- The patches landed.
SELECT
    (SELECT countSubstrings(raw, unhex('150125001808') || 'bad_enum') FROM file(currentDatabase() || '_05839_bad_type.parquet', RawBLOB, 'raw String')),
    (SELECT countSubstrings(raw, unhex('150225011808') || 'bad_enum') FROM file(currentDatabase() || '_05839_bad_repetition_type.parquet', RawBLOB, 'raw String'));

SELECT columns.physical_type, columns.max_definition_level, columns.max_repetition_level
FROM file(currentDatabase() || '_05839_good.parquet', ParquetMetadata);

-- An unknown physical type is shown as such. The statistics of its column chunks cannot be decoded.
SELECT columns.physical_type, columns.max_definition_level, columns.max_repetition_level
FROM file(currentDatabase() || '_05839_bad_type.parquet', ParquetMetadata);
SELECT row_groups FROM file(currentDatabase() || '_05839_bad_type.parquet', ParquetMetadata); -- { serverError INCORRECT_DATA }

-- An unknown repetition type does not add levels.
SELECT columns.physical_type, columns.max_definition_level, columns.max_repetition_level
FROM file(currentDatabase() || '_05839_bad_repetition_type.parquet', ParquetMetadata);
SELECT length(row_groups) FROM file(currentDatabase() || '_05839_bad_repetition_type.parquet', ParquetMetadata);
