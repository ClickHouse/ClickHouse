-- The size columns of `system.projections` report the same values for a table and for an `Alias` of it.

SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS alias_of_base;
DROP TABLE IF EXISTS base;

CREATE TABLE base (k UInt64, v UInt64, PROJECTION p (SELECT v ORDER BY v)) ENGINE = MergeTree ORDER BY k;
INSERT INTO base SELECT number, number * 2 FROM numbers(10);
INSERT INTO base SELECT number, number * 3 FROM numbers(5);

CREATE TABLE alias_of_base ENGINE = Alias('base');

SELECT table, name, total_rows, data_compressed_bytes > 0, data_uncompressed_bytes > 0
FROM system.projections
WHERE database = currentDatabase()
ORDER BY table;

SELECT count(DISTINCT (total_rows, data_compressed_bytes, data_uncompressed_bytes))
FROM system.projections
WHERE database = currentDatabase();

-- Only the metadata columns.
SELECT table, name FROM system.projections WHERE database = currentDatabase() ORDER BY table;

DROP TABLE alias_of_base;
DROP TABLE base;
