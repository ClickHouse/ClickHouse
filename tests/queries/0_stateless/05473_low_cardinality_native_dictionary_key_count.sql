-- `LowCardinality` columns in `Native` whose dictionary declares more keys than the data holds.
SELECT * FROM format(Native, 'x LowCardinality(String)', unhex('01010178164c6f7743617264696e616c69747928537472696e672901000000000000000002000000000000000000000000001d')); -- { serverError CANNOT_READ_ALL_DATA }
SELECT * FROM format(Native, 'x LowCardinality(String)', unhex('01010178164c6f7743617264696e616c69747928537472696e6729010000000000000000020000000000000000000000010000')); -- { serverError CANNOT_READ_ALL_DATA }
SELECT * FROM format(Native, 'x LowCardinality(Nullable(UInt64))', unhex('01010178204c6f7743617264696e616c697479284e756c6c61626c652855496e743634292901000000000000000002000000000000000000000000001d')) SETTINGS allow_suspicious_low_cardinality_types = 1; -- { serverError CANNOT_READ_ALL_DATA }

-- A block whose keys span several read batches is read completely.
INSERT INTO FUNCTION file(currentDatabase() || '_lc_keys.native', Native, 'x LowCardinality(String)')
    SELECT toString(number) FROM numbers(200000)
    SETTINGS engine_file_truncate_on_insert = 1, max_block_size = 200000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SELECT count(), uniqExact(x), sum(toUInt64(x)) FROM file(currentDatabase() || '_lc_keys.native', Native, 'x LowCardinality(String)');
