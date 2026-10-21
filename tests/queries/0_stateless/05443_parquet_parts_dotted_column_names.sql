-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- A column of a data part in Parquet format is not confused with a nested field of another column
-- that has the same name, e.g. the column `m.value` with the values of the map `m`.

DROP TABLE IF EXISTS t_parquet_dotted;

CREATE TABLE t_parquet_dotted (id UInt64, m Map(String, UInt64), `m.value` UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, default_compression_codec = 'LZ4';
INSERT INTO t_parquet_dotted VALUES (1, {'x': 11}, 99);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_dotted' AND active;
SELECT id, m, `m.value` FROM t_parquet_dotted;
SELECT `m.value` FROM t_parquet_dotted;
SELECT m.values FROM t_parquet_dotted;
SELECT m.values, `m.value` FROM t_parquet_dotted;

DROP TABLE t_parquet_dotted;
