-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- The uncompressed size of a column of aggregate states in a part in `Parquet` format is the size of the serialized
-- states, so the limit on the uncompressed size of the table applies to it, even when a row group holds a part of
-- the column which shares the arena of the states with the whole column.

SET allow_experimental_parquet_parts = 1;

DROP TABLE IF EXISTS t_parquet_aggregate_state_size;

CREATE TABLE t_parquet_aggregate_state_size (id UInt64, s AggregateFunction(groupArray, String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 1, max_table_size_bytes_uncompressed = 65536;

INSERT INTO t_parquet_aggregate_state_size SELECT number % 2, groupArrayState(repeat('x', 1024)) FROM numbers(2000) GROUP BY number % 2 SETTINGS max_insert_threads = 1;

SELECT part_type, data_uncompressed_bytes > 2000000 FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_aggregate_state_size' AND active;
SELECT total_bytes_uncompressed > 2000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_aggregate_state_size';

INSERT INTO t_parquet_aggregate_state_size SELECT number % 2, groupArrayState(repeat('y', 1024)) FROM numbers(2000) GROUP BY number % 2 SETTINGS max_insert_threads = 1; -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

DETACH TABLE t_parquet_aggregate_state_size;
ATTACH TABLE t_parquet_aggregate_state_size;

SELECT total_bytes_uncompressed > 2000000 FROM system.tables WHERE database = currentDatabase() AND name = 't_parquet_aggregate_state_size';
SELECT count(), sum(length(finalizeAggregation(s))) FROM t_parquet_aggregate_state_size;

DROP TABLE t_parquet_aggregate_state_size;
