-- Tags: no-ordinary-database
-- A read evaluates the `DEFAULT` expression of a column that an old part does not store. When the
-- expression reads a dictionary, the rows change with the dictionary while the table stays the same, so
-- the table has no modification hash, and the consistent query cache returns the fresh value.
-- The same check applies to the tables behind a `Distributed` table, on a local and on a remote shard.

DROP TABLE IF EXISTS t_05444;
DROP TABLE IF EXISTS u_05444;
DROP TABLE IF EXISTS plain_05444;
DROP TABLE IF EXISTS dist_local_05444;
DROP TABLE IF EXISTS dist_remote_05444;
DROP TABLE IF EXISTS dist_plain_05444;
DROP DICTIONARY IF EXISTS dict_05444;
DROP TABLE IF EXISTS dict_src_05444;

CREATE TABLE dict_src_05444 (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dict_src_05444 VALUES (1, 'a');
CREATE DICTIONARY dict_05444 (k UInt64, v String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 'dict_src_05444')) LAYOUT(FLAT()) LIFETIME(0);

CREATE TABLE t_05444 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05444 VALUES (1);
ALTER TABLE t_05444 ADD COLUMN y String DEFAULT dictGet('dict_05444', 'v', k);

SELECT 'default evaluated on read has no hash', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_05444';

SELECT y FROM t_05444 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
TRUNCATE TABLE dict_src_05444;
INSERT INTO dict_src_05444 VALUES (1, 'b');
SYSTEM RELOAD DICTIONARY dict_05444;
SELECT y FROM t_05444 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- Once every part stores the column, the read no longer evaluates the expression.
ALTER TABLE t_05444 MATERIALIZE COLUMN y SETTINGS mutations_sync = 2;
SELECT 'default stored in every part has a hash', modification_hash IS NOT NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_05444';

-- `y` is an ordinary column of the `Distributed` table, but an `ALIAS` of the table behind it.
CREATE TABLE u_05444 (k UInt64, y String ALIAS dictGet('dict_05444', 'v', k)) ENGINE = MergeTree ORDER BY k;
INSERT INTO u_05444 VALUES (1);
CREATE TABLE plain_05444 (k UInt64, y String) ENGINE = MergeTree ORDER BY k;
INSERT INTO plain_05444 VALUES (1, 'a');
CREATE TABLE dist_local_05444 (k UInt64, y String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), u_05444);
CREATE TABLE dist_remote_05444 (k UInt64, y String) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), u_05444);
CREATE TABLE dist_plain_05444 (k UInt64, y String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), plain_05444);

SELECT name, modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name LIKE 'dist_%' ORDER BY name;

DROP TABLE dist_local_05444;
DROP TABLE dist_remote_05444;
DROP TABLE dist_plain_05444;
DROP TABLE u_05444;
DROP TABLE plain_05444;
DROP TABLE t_05444;
DROP DICTIONARY dict_05444;
DROP TABLE dict_src_05444;
