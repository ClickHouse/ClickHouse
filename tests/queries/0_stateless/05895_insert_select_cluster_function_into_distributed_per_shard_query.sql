-- Tags: no-fasttest
-- Tag no-fasttest: needs the test clusters

-- `INSERT INTO <Distributed table> SELECT ... FROM <cluster table function>` with `parallel_distributed_insert_select`
-- sends the INSERT to every shard of the destination, and each shard inserts the result of the SELECT over its share of
-- the files. That is the result of the SELECT only when the SELECT does not combine rows across the shares (aggregation,
-- window functions, LIMIT, OFFSET, ...) and reads nothing but the source (subqueries). Any other SELECT is run by the
-- initiator. Both shards of the cluster are this server, so every row lands in `loc` whichever way the INSERT runs.

SET parallel_distributed_insert_select = 2, insert_deduplicate = 0, distributed_foreground_insert = 1;

INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05895_0.tsv', 'TSV', 'x UInt64') SELECT number + 1 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05895_1.tsv', 'TSV', 'x UInt64') SELECT number + 101 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05895_2.tsv', 'TSV', 'x UInt64') SELECT number + 201 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05895_3.tsv', 'TSV', 'x UInt64') SELECT number + 301 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05895_4.tsv', 'TSV', 'x UInt64') SELECT number + 401 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05895_5.tsv', 'TSV', 'x UInt64') SELECT number + 501 FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS loc;
DROP TABLE IF EXISTS allow;
CREATE TABLE loc (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist AS loc ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), loc, rand());
CREATE TABLE allow (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO allow SELECT number * 2 FROM numbers(300);

-- Not dropped at the end: another run of this test may use them at the same time.
CREATE OR REPLACE FUNCTION udf_05895_inc AS (v) -> v + 1;
CREATE OR REPLACE FUNCTION udf_05895_sum AS (v) -> sum(v);

-- Still run on each shard.
INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_01_plain';
SELECT 'plain', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_02_plain_pdis_1', parallel_distributed_insert_select = 1;
SELECT 'plain, parallel_distributed_insert_select = 1', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT udf_05895_inc(x) FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_03_udf_row_local';
SELECT 'udf row-local', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

-- Combine rows across the shares.
INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') LIMIT 5 SETTINGS log_comment = 'case_04_limit';
SELECT 'limit', count() FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') LIMIT 5 SETTINGS log_comment = 'case_05_limit_pdis_1', parallel_distributed_insert_select = 1;
SELECT 'limit, parallel_distributed_insert_select = 1', count() FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') ORDER BY x LIMIT 5 SETTINGS log_comment = 'case_06_order_by_limit';
SELECT 'order by limit', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') OFFSET 1 SETTINGS log_comment = 'case_07_offset';
SELECT 'offset', count() FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT DISTINCT x % 7 FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_08_distinct';
SELECT 'distinct', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT count() FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') GROUP BY x % 3 SETTINGS log_comment = 'case_09_group_by';
SELECT 'group by', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT sum(x) FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_10_sum';
SELECT 'sum', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT * APPLY sum FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_11_apply_sum';
SELECT 'apply sum', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT udf_05895_sum(x) FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_12_udf_sum';
SELECT 'udf sum', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT row_number() OVER () FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_13_row_number';
SELECT 'row_number', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') QUALIFY row_number() OVER (ORDER BY x) <= 3 SETTINGS log_comment = 'case_14_qualify';
SELECT 'qualify', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

-- Read more than their share of the source, or another table.
INSERT INTO dist SELECT (SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') ORDER BY x LIMIT 1) FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') SETTINGS log_comment = 'case_15_scalar_same_source';
SELECT 'scalar subquery over the source', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') WHERE x IN (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.allow) SETTINGS log_comment = 'case_16_in_subquery';
SELECT 'IN subquery', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') WHERE x IN {CLICKHOUSE_DATABASE:Identifier}.allow SETTINGS log_comment = 'case_17_in_table';
SELECT 'IN table', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') WHERE x > (SELECT max(x) FROM {CLICKHOUSE_DATABASE:Identifier}.allow) - 10 SETTINGS log_comment = 'case_18_folded_scalar';
SELECT 'folded scalar subquery', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') WHERE has((SELECT [x, x + 2] FROM {CLICKHOUSE_DATABASE:Identifier}.allow WHERE x = 596), x) SETTINGS log_comment = 'case_19_array_scalar';
SELECT 'array scalar subquery', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

INSERT INTO dist SELECT x FROM fileCluster('test_cluster_two_shards_localhost', {CLICKHOUSE_DATABASE:String} || '_05895_*.tsv', 'TSV', 'x UInt64') WHERE EXISTS (SELECT 1 FROM {CLICKHOUSE_DATABASE:Identifier}.allow WHERE x = 2) SETTINGS log_comment = 'case_20_exists';
SELECT 'EXISTS', count(), sum(x), min(x), max(x) FROM loc;
TRUNCATE TABLE loc;

-- Which INSERTs were sent to the shards. The INSERTs `Distributed` itself sends to the shards do not name the source.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment,
    query_id IN (SELECT initial_query_id FROM system.query_log WHERE event_date >= yesterday() AND NOT is_initial_query AND query_kind = 'Insert' AND query ILIKE '%fileCluster%') AS sent_to_shards
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query AND query_kind = 'Insert'
    AND type = 'QueryFinish' AND log_comment LIKE 'case\_%'
ORDER BY log_comment;

DROP TABLE dist;
DROP TABLE loc;
DROP TABLE allow;
