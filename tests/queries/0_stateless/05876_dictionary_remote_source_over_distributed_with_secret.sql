-- Tags: no-fasttest
-- no-fasttest: the cluster secret requires SSL

-- A remote (non-local) ClickHouse dictionary source sends its query without `initial_user`,
-- and the remote server has to forward it over a cluster with a secret.

DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS data;

CREATE TABLE data (key UInt64, value String) ENGINE = MergeTree ORDER BY key;
INSERT INTO data VALUES (1, 'a');
CREATE TABLE dist AS data ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), data);

CREATE DICTIONARY dict (key UInt64, value String)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST '127.0.0.2' PORT tcpPort() DB currentDatabase() TABLE 'dist'))
LAYOUT(HASHED())
LIFETIME(0);

SELECT dictGet('dict', 'value', 1::UInt64);

DROP DICTIONARY dict;
DROP TABLE dist;
DROP TABLE data;
