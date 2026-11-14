-- Tags: atomic-database
-- A detached source of an incremental refreshable view still blocks `RENAME DATABASE` and `DROP DATABASE`:
-- both free the source's name, so a new table under it would be read from the view's stale cursor.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO tgt EMPTY
    AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;

DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src SYNC;

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- The source comes back and the view keeps reading it from its cursor.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT 'after_rejected_ddl', groupArray(k) FROM (SELECT k FROM tgt ORDER BY k);

-- Without the view the database with a detached table can be dropped.
DROP TABLE mv;
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src SYNC;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'dropped_without_view', count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP TABLE tgt;
