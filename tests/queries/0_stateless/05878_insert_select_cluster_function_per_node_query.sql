-- Tags: zookeeper, no-fasttest
-- Tag no-fasttest: needs the test clusters

-- `INSERT INTO <replicated table> SELECT ... FROM <cluster table function>` with `parallel_distributed_insert_select`
-- sends the INSERT to every node of the cluster, and each node inserts the result of the SELECT over its share of the
-- files. That is the result of the SELECT only when the SELECT does not combine rows across the shares (aggregation,
-- window functions, OFFSET, ...) and reads nothing but the source (subqueries). Any other SELECT is run by the initiator.

SET parallel_distributed_insert_select = 2, insert_deduplicate = 0;

INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05878_0.tsv', 'TSV', 'x UInt64') SELECT number + 1 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05878_1.tsv', 'TSV', 'x UInt64') SELECT number + 101 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05878_2.tsv', 'TSV', 'x UInt64') SELECT number + 201 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05878_3.tsv', 'TSV', 'x UInt64') SELECT number + 301 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05878_4.tsv', 'TSV', 'x UInt64') SELECT number + 401 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05878_5.tsv', 'TSV', 'x UInt64') SELECT number + 501 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS dst SYNC;
DROP TABLE IF EXISTS allow;
CREATE TABLE dst (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_05878', 'r1') ORDER BY tuple();
CREATE TABLE allow (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO allow SELECT number * 2 FROM numbers(300);

-- Not dropped at the end: another run of this test may use them at the same time.
CREATE OR REPLACE FUNCTION udf_05878_inc AS (v) -> v + 1;
CREATE OR REPLACE FUNCTION udf_05878_sum AS (v) -> sum(v);
CREATE OR REPLACE FUNCTION udf_05878_max AS () -> (SELECT x FROM allow ORDER BY x DESC LIMIT 1);

-- Still run on each node.
INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_01_plain';
SELECT 'plain', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT udf_05878_inc(x) FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_02_udf_row_local';
SELECT 'udf row-local', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

-- Combine rows across the shares.
INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') OFFSET 1 SETTINGS log_comment = 'case_03_offset';
SELECT 'offset', count() FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT row_number() OVER () FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_04_row_number';
SELECT 'row_number', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') QUALIFY row_number() OVER (ORDER BY x) <= 3 SETTINGS log_comment = 'case_05_qualify';
SELECT 'qualify', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT * APPLY sum FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_06_apply_sum';
SELECT 'apply sum', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT udf_05878_sum(x) FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_07_udf_sum';
SELECT 'udf sum', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

-- Read more than their share of the source, or another table.
INSERT INTO dst SELECT (SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') ORDER BY x LIMIT 1) FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_08_scalar_same_source';
SELECT 'scalar subquery over the source', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') WHERE x IN (SELECT x FROM allow) SETTINGS log_comment = 'case_09_in_subquery';
SELECT 'IN subquery', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') WHERE x > udf_05878_max() - 10 SETTINGS log_comment = 'case_10_udf_scalar';
SELECT 'udf with a scalar subquery', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT * APPLY (v -> v + (SELECT x FROM allow ORDER BY x LIMIT 1)) FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_11_apply_scalar';
SELECT 'APPLY with a scalar subquery', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') WHERE has((SELECT [x, x + 2] FROM allow WHERE x = 596), x) SETTINGS log_comment = 'case_12_array_scalar';
SELECT 'array scalar subquery', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

INSERT INTO dst SELECT x FROM fileCluster('test_cluster_one_shard_three_replicas_localhost', {CLICKHOUSE_DATABASE:String} || '_05878_*.tsv', 'TSV', 'x UInt64') WHERE x IN allow SETTINGS log_comment = 'case_13_in_table';
SELECT 'IN table', count(), sum(x), min(x), max(x) FROM dst;
TRUNCATE TABLE dst;

-- Which INSERTs were sent to the nodes.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment,
    query_id IN (SELECT initial_query_id FROM system.query_log WHERE event_date >= yesterday() AND NOT is_initial_query AND query_kind = 'Insert') AS sent_to_nodes
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query AND query_kind = 'Insert'
    AND type = 'QueryFinish' AND log_comment LIKE 'case\_%'
ORDER BY log_comment;

DROP TABLE dst SYNC;
DROP TABLE allow;
