-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET cluster_function_process_archive_on_multiple_nodes = 0;

SELECT count() FROM s3Cluster('test_shard_localhost', s3_conn, filename = '03036_archive*.zip :: example*.csv');
SELECT id, _file, _path FROM s3Cluster('test_cluster_two_shards', s3_conn, filename = '03036_archive*.zip :: example*.csv') ORDER BY id, _path;
