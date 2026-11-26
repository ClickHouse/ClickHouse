-- Tags: zookeeper, no-fasttest
-- Tag no-fasttest: needs the test clusters and a Replicated table

-- `INSERT ... SELECT ... FROM <cluster table function>` with `parallel_distributed_insert_select` is forwarded to
-- the nodes, which interpret it in their own default database: the names of the `SELECT` that depend on the
-- current database must mean what they mean on the initiator.

DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS loc;
DROP DICTIONARY IF EXISTS dct;
DROP TABLE IF EXISTS dsrc;
DROP TABLE IF EXISTS allow;

INSERT INTO FUNCTION file(currentDatabase() || '_05951_0.tsv', 'TSV', 'x UInt64') SELECT number + 1 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05951_1.tsv', 'TSV', 'x UInt64') SELECT number + 101 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05951_2.tsv', 'TSV', 'x UInt64') SELECT number + 201 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05951_3.tsv', 'TSV', 'x UInt64') SELECT number + 301 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05951_4.tsv', 'TSV', 'x UInt64') SELECT number + 401 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05951_5.tsv', 'TSV', 'x UInt64') SELECT number + 501 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

CREATE TABLE dsrc (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dsrc SELECT number, number * 10 FROM numbers(1000);
CREATE DICTIONARY dct (k UInt64, v UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dsrc')) LAYOUT(FLAT()) LIFETIME(0);

CREATE TABLE allow (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO allow SELECT number * 2 FROM numbers(300);

CREATE TABLE dst (c String, s String, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_05951', 'r1') ORDER BY tuple();
CREATE TABLE loc (c String, s String, x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist AS loc ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), loc, rand());

-- Into a Replicated table.
INSERT INTO dst SELECT 'db', currentDatabase() AS d, x
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
WHERE d = DATABASE()
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_1';

INSERT INTO dst SELECT 'dict', '', dictGetUInt64('dct', 'v', x)
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_2';

INSERT INTO dst SELECT 'schema', current_schema(), x
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_2a';

-- A lambda named like the function is called instead of it, in the path too: these queries are not forwarded.
INSERT INTO dst WITH (() -> 'shadow') AS currentDatabase SELECT 'shadow', currentDatabase(), x
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05951_*.tsv', 'TSV', 'x UInt64')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_2b';

INSERT INTO dst WITH (() -> 'shadow') AS f, f AS currentDatabase SELECT 'shadow_ident', currentDatabase(), x
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05951_*.tsv', 'TSV', 'x UInt64')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_2c';

INSERT INTO dst SELECT 'renamed', currentDatabase(), x AS database
FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_2d';

-- Into a Distributed table.
INSERT INTO dist SELECT 'db', currentDatabase() AS d, x
FROM fileCluster('test_cluster_two_shards_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
WHERE d = DATABASE()
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_3';

INSERT INTO dist SELECT 'dict', '', dictGetUInt64('dct', 'v', x)
FROM fileCluster('test_cluster_two_shards_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_4';

INSERT INTO dist SELECT 'in_subquery', '', x
FROM fileCluster('test_cluster_two_shards_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
WHERE x IN (SELECT x FROM allow)
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_5';

INSERT INTO dist SELECT 'in_table', '', x
FROM fileCluster('test_cluster_two_shards_localhost', currentDatabase() || '_05951_*.tsv', 'TSV', 'x UInt64')
WHERE x IN allow
SETTINGS parallel_distributed_insert_select = 2, log_comment = '05951_6';

SELECT 'dst';
SELECT c, count(), countIf(s = currentDatabase()), sum(x) FROM dst GROUP BY c ORDER BY c;
SELECT DISTINCT s FROM dst WHERE c IN ('shadow', 'shadow_ident');
SELECT 'loc';
SELECT c, count(), countIf(s = currentDatabase()), sum(x) FROM loc GROUP BY c ORDER BY c;

-- The queries that are expected to be forwarded were forwarded.
SYSTEM FLUSH LOGS query_log;
SELECT 'forwarded';
SELECT log_comment, count() > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0 AND query_kind = 'Insert'
    AND has(databases, currentDatabase()) AND log_comment LIKE '05951\_%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE dist;
DROP TABLE loc;
DROP TABLE dst;
DROP DICTIONARY dct;
DROP TABLE dsrc;
DROP TABLE allow;
