-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- Replication of data parts in Parquet format.

DROP TABLE IF EXISTS t_parquet_r1 SYNC;
DROP TABLE IF EXISTS t_parquet_r2 SYNC;

CREATE TABLE t_parquet_r1 (key UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parquet_replicated', 'r1') ORDER BY key
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 100;

CREATE TABLE t_parquet_r2 (key UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_parquet_replicated', 'r2') ORDER BY key
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 100;

INSERT INTO t_parquet_r1 SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_parquet_r1 SELECT number + 1000, toString(number) FROM numbers(1000);
SYSTEM SYNC REPLICA t_parquet_r2;

SELECT 'fetched';
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_r2' AND active ORDER BY name;
SELECT count(), sum(key), sum(length(value)) FROM t_parquet_r2;

SELECT 'merged';
OPTIMIZE TABLE t_parquet_r1 FINAL SETTINGS alter_sync = 2;
SYSTEM SYNC REPLICA t_parquet_r2;
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_r2' AND active ORDER BY name;
SELECT count(), sum(key), sum(length(value)) FROM t_parquet_r2;

DROP TABLE t_parquet_r1 SYNC;
DROP TABLE t_parquet_r2 SYNC;
