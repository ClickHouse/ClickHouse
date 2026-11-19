-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: Depends on S3
-- Tag no-parallel-replicas: counts the S3 requests of each query, which differ under parallel replicas

-- Hive partition columns are read from the directory part of a path. When every glob is in the file name, the
-- pattern has the directory of every object it matches, so the columns are resolved without listing the storage.

SET use_hive_partitioning = 1;
SET parallel_replicas_for_cluster_engines = 0;
SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_1.tsv', format = TSV) SELECT 1 AS v;
INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_2.tsv', format = TSV) SELECT 2 AS v;

SELECT v, a, b, toTypeName(a), toTypeName(b), _file
FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_*.tsv', format = TSV, structure = 'v UInt64')
ORDER BY v SETTINGS log_comment = '05832:star';

SELECT v, a
FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_{1..2}.tsv', format = TSV, structure = 'v UInt64')
ORDER BY v SETTINGS log_comment = '05832:range';

SELECT v, b
FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_?.tsv', format = TSV, structure = 'v UInt64')
ORDER BY v SETTINGS log_comment = '05832:question_mark';

SELECT v, a, b
FROM s3Cluster('test_shard_localhost', s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_*.tsv', format = TSV, structure = 'v UInt64')
ORDER BY v SETTINGS log_comment = '05832:cluster';

DROP TABLE IF EXISTS t_05832;
CREATE TABLE t_05832 (v UInt64) ENGINE = S3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/f_*.tsv', format = TSV);
SELECT v, a, b FROM t_05832 ORDER BY v SETTINGS log_comment = '05832:table';

-- A glob in a directory still lists the storage to pick the sample path.
SELECT v, a
FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=*/b=x/f_1.tsv', format = TSV, structure = 'v UInt64')
ORDER BY v SETTINGS log_comment = '05832:directory_glob';

-- A file name glob that matches no object has the hive columns of its directory, like an explicit key.
SELECT count()
FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05832/a=1/b=x/nomatch_*.tsv', format = TSV, structure = 'v UInt64')
WHERE a = 1 SETTINGS log_comment = '05832:no_match';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['S3ListObjects']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05832:%'
ORDER BY event_time_microseconds;

SELECT 'cluster replica', sum(ProfileEvents['S3ListObjects'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
            AND type = 'QueryFinish' AND is_initial_query AND log_comment = '05832:cluster');

DROP TABLE t_05832;
