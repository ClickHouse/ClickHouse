-- Tags: no-fasttest, need-query-parameters
-- Tag no-fasttest: Iceberg needs Avro.

-- Writes to an Iceberg table that is not managed by a catalog, with parallel replicas enabled.
-- `icebergS3Cluster` resolves to `StorageObjectStorageCluster`, which must implement the write side
-- too, or `INSERT` fails with `Method write is not supported by storage IcebergS3`.

SET allow_insert_into_iceberg = 1;
SET parallel_replicas_for_cluster_engines = 1, enable_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = IcebergS3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/t/') SETTINGS iceberg_format_version = 2;

-- Engine table: stays a plain `StorageObjectStorage`.
INSERT INTO t VALUES (1);
-- Plain table function: not auto-converted to the cluster variant for `INSERT`.
INSERT INTO FUNCTION icebergS3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/t/') VALUES (2);
-- Explicit cluster table function: the write is performed by the initiator.
INSERT INTO FUNCTION icebergS3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/t/') VALUES (3);

SELECT x FROM t ORDER BY ALL;
SELECT x FROM icebergS3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/t/') ORDER BY ALL;
SELECT x FROM icebergS3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/t/') ORDER BY ALL;

DROP TABLE t;
