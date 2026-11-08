-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced.

-- `RENAME` of a table that is not loaded yet loads it before taking the database mutex: building the table
-- analyzes a constraint with a scalar subquery, which looks up a table in the same database. The loaded
-- storage still decides whether it can be renamed.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE ref (x UInt64) ENGINE = Memory;
CREATE TABLE t (id UInt64, CONSTRAINT c CHECK id < (SELECT count() + 100 FROM ref)) ENGINE = MergeTree ORDER BY id COMMENT 'first';
CREATE TABLE u (id UInt64, CONSTRAINT c CHECK id < (SELECT count() + 100 FROM ref)) ENGINE = MergeTree ORDER BY id COMMENT 'second';
-- The path holds the implicit `{database}` and `{table}` macros, so this table refuses to be renamed.
CREATE TABLE r (id UInt64) ENGINE = ReplicatedMergeTree('{default_path_test}test_05567', '{default_name_test}') ORDER BY id;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'before', name, is_loaded FROM system.tables WHERE database = currentDatabase() AND name IN ('t', 'u', 'r') ORDER BY name;

RENAME TABLE t TO t2;
EXCHANGE TABLES t2 AND u;
RENAME TABLE r TO r2; -- { serverError NOT_IMPLEMENTED }

SELECT 'after', name, is_loaded, comment FROM system.tables WHERE database = currentDatabase() AND name IN ('t2', 'u', 'r') ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
