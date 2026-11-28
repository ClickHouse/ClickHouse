-- Tags: no-replicated-database, no-parallel-replicas
-- Tag no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

-- A `STREAM` query and an incremental refresh work on a `MergeTree` table that has not been loaded yet.

SET enable_streaming_queries = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';
CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_log (k UInt64) ENGINE = Log;
CREATE TABLE src_ts (k UInt64, ts DateTime) ENGINE = MergeTree ORDER BY k;
INSERT INTO src VALUES (1), (2), (3);
INSERT INTO src_ts VALUES (1, '2020-01-01 00:00:10'), (2, '2020-01-01 00:00:20'), (3, '2020-01-01 00:00:30');
INSERT INTO t_log VALUES (1);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'cold engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 'src';
SELECT 'stream', count(), sum(k) FROM src STREAM BOUNDED;
SELECT count() FROM t_log STREAM BOUNDED; -- { serverError ILLEGAL_STREAM }
SELECT 'cold watermark', count(), min(_time_attribute), max(_time_attribute) FROM src_ts STREAM BOUNDED WATERMARK FOR ts AS ts;
SELECT 'loaded watermark', count(), min(_time_attribute), max(_time_attribute) FROM src_ts STREAM BOUNDED WATERMARK FOR ts AS ts;

CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT k FROM src;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT 'refresh', groupArray(k) FROM (SELECT k FROM tgt ORDER BY k);

DROP TABLE mv;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
