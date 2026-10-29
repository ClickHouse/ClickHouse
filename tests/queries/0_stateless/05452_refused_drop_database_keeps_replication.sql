-- Tags: zookeeper

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier} SYNC FORMAT Null;
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
ENGINE = Replicated('/clickhouse/05452_refused_drop_database_keeps_replication/{database}', 's1', 'r1') FORMAT Null;
USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x FORMAT Null;
INSERT INTO t SELECT number FROM numbers(1000);

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier} SETTINGS max_table_size_to_drop = 1; -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }

CREATE TABLE after_refused_drop (x UInt64) ENGINE = Memory FORMAT Null;
INSERT INTO t VALUES (1000);
SELECT count() FROM t;
DROP TABLE after_refused_drop FORMAT Null;

DETACH DATABASE {CLICKHOUSE_DATABASE:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE:Identifier};
CREATE TABLE after_attach (x UInt64) ENGINE = Memory FORMAT Null;
SELECT count() FROM t;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier} SYNC FORMAT Null;
