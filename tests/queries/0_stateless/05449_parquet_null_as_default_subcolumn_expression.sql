-- Tags: no-fasttest
-- Tag no-fasttest: depends on Parquet

-- https://github.com/ClickHouse/ClickHouse/issues/121905
-- A DEFAULT which depends on a subcolumn reads its column.
INSERT INTO FUNCTION file(currentDatabase() || '_05449_tuple.parquet', Parquet, 'a Tuple(x UInt32), b Nullable(UInt32)')
    SELECT * FROM values('a Tuple(x UInt32), b Nullable(UInt32)', (tuple(6), NULL), (tuple(7), 3)) SETTINGS engine_file_truncate_on_insert = 1;

SELECT b FROM file(currentDatabase() || '_05449_tuple.parquet', Parquet, 'a Tuple(x UInt32), b UInt32 DEFAULT a.x * 2');
SELECT a.x, b FROM file(currentDatabase() || '_05449_tuple.parquet', Parquet, 'a Tuple(x UInt32), b UInt32 DEFAULT a.x * 2');
