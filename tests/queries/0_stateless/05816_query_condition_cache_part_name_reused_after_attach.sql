-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: their block numbers do not restart, so no part name is reused.

-- Attaching a MergeTree table again numbers its parts from those on disk, so after TRUNCATE the next INSERT
-- writes part `all_1_1_0` again. The query condition cache must not return the removed part's verdicts.

SET use_query_condition_cache = 1;
-- The uncompressed cache is keyed by file path, so it can serve the removed part's data here too.
SET use_uncompressed_cache = 0;

CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 0;
INSERT INTO t VALUES (1, 'pear');
SELECT sum(k) FROM t WHERE length(s) = 5;
TRUNCATE TABLE t;
DETACH TABLE t;
ATTACH TABLE t;
INSERT INTO t VALUES (2, 'apple');
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT sum(k) FROM t WHERE length(s) = 5;

-- The same through ATTACH DATABASE, with a new part that has more marks than the removed one.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS old_parts_lifetime = 0, index_granularity = 8, index_granularity_bytes = '10Mi';
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT number, 'pear' FROM numbers(16);
SELECT sum(k) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE length(s) = 5;
TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t;
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT number, 'apple' FROM numbers(1000);
SELECT name FROM system.parts WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't' AND active;
SELECT sum(k) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE length(s) = 5;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
