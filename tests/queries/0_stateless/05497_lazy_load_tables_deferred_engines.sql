-- Tags: no-replicated-database, no-shared-merge-tree, no-fasttest
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced, and it keeps parts elsewhere.
-- no-fasttest: `EmbeddedRocksDB` and `KeeperMap` are not available in the fast test.

-- With `lazy_load_tables` the catalog holds a stand-in for a table whose engine has something to load and no
-- background work. The stand-in is an implementation detail: it looks like the table, except for `is_loaded`.
-- Deferring an engine that works in the background would cancel that work rather than delay it, so opting in
-- is per engine, and an engine with nothing to load or with work in the background is loaded at startup.

SELECT 'opted in', count() FROM system.table_engines
WHERE supports_deferred_load AND name IN ('MergeTree', 'Set', 'EmbeddedRocksDB', 'Log', 'S3', 'MySQL', 'KeeperMap', 'Kafka', 'FileLog', 'S3Queue');
SELECT 'background engines opted in', count() FROM system.table_engines
WHERE supports_deferred_load AND name IN ('Distributed', 'Buffer', 'MaterializedPostgreSQL', 'MaterializedView', 'Dictionary');

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(10);
CREATE TABLE mem (id UInt64) ENGINE = Memory;
CREATE TABLE lg (id UInt64) ENGINE = Log;
INSERT INTO lg SELECT number FROM numbers(10);
CREATE TABLE tlg (id UInt64) ENGINE = TinyLog;
CREATE TABLE slg (id UInt64) ENGINE = StripeLog;
CREATE TABLE nul (id UInt64) ENGINE = Null;
CREATE TABLE km (k String, v String) ENGINE = KeeperMap(concat('/', {CLICKHOUSE_DATABASE_1:String}, '/km')) PRIMARY KEY k;
INSERT INTO km SELECT toString(number), toString(number) FROM numbers(10);
CREATE TABLE mrg (id UInt64) ENGINE = Merge({CLICKHOUSE_DATABASE_1:String}, '^src$');
CREATE TABLE al ENGINE = Alias(src);
CREATE TABLE buf (id UInt64) ENGINE = Buffer({CLICKHOUSE_DATABASE_1:String}, 'src', 1, 100, 100, 10, 100, 10000, 1000000);
CREATE TABLE dist (id UInt64) ENGINE = Distributed('test_shard_localhost', {CLICKHOUSE_DATABASE_1:String}, 'src');
-- `Set` and `Join` read their whole contents into memory in the constructor and RocksDB opens its
-- database there, so they are worth deferring, and each is reached by a concrete-type cast.
CREATE TABLE st (id UInt64) ENGINE = Set;
INSERT INTO st VALUES (1), (2);
CREATE TABLE jn (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO jn VALUES (1, 'one');
CREATE TABLE kv (k String, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv VALUES ('a', 'first');

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- The engine and `has_own_data` are answered from the engine while the storage does not exist.
SELECT name, engine, is_loaded, has_own_data FROM system.tables WHERE database = currentDatabase() ORDER BY name;
-- Reading `system.tables` does not load anything.
SELECT 'deferred after listing', count() FROM system.tables WHERE database = currentDatabase() AND NOT is_loaded;

SELECT 'src', count() FROM src;
SELECT 'lg', count() FROM lg;
SELECT 'km', count() FROM km;
SELECT 'set', count() FROM numbers(4) WHERE number IN st;
SELECT 'join', v FROM (SELECT toUInt64(1) AS k) AS s ANY LEFT JOIN jn USING (k);
SELECT 'joinGet', joinGet({CLICKHOUSE_DATABASE_1:String} || '.jn', 'v', toUInt64(1));
SELECT 'rocksdb', v FROM kv WHERE k = 'a';
SELECT 'loaded', groupArray(name) FROM (SELECT name FROM system.tables WHERE database = currentDatabase() AND is_loaded ORDER BY name);

-- The direct join reaches a key-value engine through the `IKeyValueEntity` interface, and asking for it
-- explicitly turns skipping it into an error rather than a slower plan.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'direct join', v FROM (SELECT 'a' AS k) AS s JOIN kv USING (k) SETTINGS join_algorithm = 'direct';

-- `system.graphite_retentions` reaches `GraphiteMergeTree` through a `MergeTreeData` cast.
CREATE TABLE graphite (metric String, value Float64, timestamp UInt32, date Date, updated UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (metric, timestamp);
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'graphite', is_loaded FROM system.tables WHERE database = currentDatabase() AND name = 'graphite';
SELECT 'graphite', count() FROM graphite;
SELECT 'graphite retentions', count() > 0 FROM system.graphite_retentions
WHERE has(Tables.database, currentDatabase()) AND has(Tables.table, 'graphite');

-- A temporary table is never behind a stand-in.
CREATE TEMPORARY TABLE tmp (id UInt64) ENGINE = Memory;
SELECT 'temporary', is_loaded FROM system.tables WHERE is_temporary AND name = 'tmp';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
