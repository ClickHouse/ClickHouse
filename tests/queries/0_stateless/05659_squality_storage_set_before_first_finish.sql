SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_storage_set_before_finish;
CREATE TABLE squality_storage_set_before_finish (k Nullable(UInt64)) ENGINE = Set SETTINGS persistent = 0;

INSERT INTO squality_storage_set_before_finish
SELECT if(number = 0, CAST(NULL AS Nullable(UInt64)), toNullable(number + throwIf(number = 1000)))
FROM numbers(1001)
SETTINGS max_block_size = 1, max_threads = 1,
         min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT toUInt64(1) IN squality_storage_set_before_finish,
       toUInt64(1001) IN squality_storage_set_before_finish;

TRUNCATE TABLE squality_storage_set_before_finish;
INSERT INTO squality_storage_set_before_finish
SELECT if(number = 0, CAST(NULL AS Nullable(UInt64)), toNullable(number + throwIf(number = 1000)))
FROM numbers(1001)
SETTINGS max_block_size = 1, max_threads = 1,
         min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT toUInt64(1) IN squality_storage_set_before_finish,
       toUInt64(1001) IN squality_storage_set_before_finish;

DROP TABLE squality_storage_set_before_finish;
