-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- `MODIFY COLUMN` of an Iceberg table threw a `Poco` bad cast exception for every type change other than
-- int -> long and float -> double: a decimal type is the string "decimal(P, S)" in the metadata, not an object.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05634_iceberg_modify_column_decimal_precision/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (id Int64, d Decimal(5, 0), s String) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
INSERT INTO t VALUES (1, 12345, 'a');

-- The precision of a decimal may grow.
ALTER TABLE t MODIFY COLUMN d Decimal(14, 0);
INSERT INTO t VALUES (2, 12345678901234, 'b');
SELECT id, d, toTypeName(d) FROM t ORDER BY id;

-- Its scale may not change, and a string is not promoted to a number.
ALTER TABLE t MODIFY COLUMN d Decimal(15, 1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t MODIFY COLUMN s Int64; -- { serverError BAD_ARGUMENTS }
DESCRIBE t;
