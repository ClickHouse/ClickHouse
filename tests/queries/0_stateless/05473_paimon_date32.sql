-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: Depends on AWS
-- Tag no-parallel-replicas: ProfileEvents of the replicas are not collected by the coordinator

-- Paimon DATE is a signed day count and must be read as Date32, not Date. https://github.com/ClickHouse/ClickHouse/issues/128306
DESC paimonS3(s3_conn, filename = 'paimon_date32/t_parquet');
SELECT * FROM paimonS3(s3_conn, filename = 'paimon_date32/t_parquet') ORDER BY id;
SELECT '=== orc';
SELECT id, d FROM paimonS3(s3_conn, filename = 'paimon_date32/t_orc', format = 'ORC') ORDER BY id;
SELECT '=== avro';
SELECT id, d FROM paimonS3(s3_conn, filename = 'paimon_date32/t_avro', format = 'Avro') ORDER BY id;
SELECT '=== partitioned';
SELECT id, d FROM paimonS3(s3_conn, filename = 'paimon_date32/t_partitioned') ORDER BY id;
-- pruning on both sides of the old Date range: 2 of the 3 partitions match
SELECT id, d FROM paimonS3(s3_conn, filename = 'paimon_date32/t_partitioned')
WHERE d < '1970-01-01' OR d > '2149-06-06' ORDER BY id
SETTINGS use_paimon_partition_pruning = 0, use_query_condition_cache = 0, log_comment = '05473_pruning_off';
SELECT id, d FROM paimonS3(s3_conn, filename = 'paimon_date32/t_partitioned')
WHERE d < '1970-01-01' OR d > '2149-06-06' ORDER BY id
SETTINGS use_paimon_partition_pruning = 1, use_query_condition_cache = 0, log_comment = '05473_pruning_on';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05473_pruning_%'
ORDER BY log_comment;
