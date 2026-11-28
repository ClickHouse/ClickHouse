-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: depends on AWS S3, HDFS, Azure, Hive and the DataLakeCatalog libraries
-- Tag no-replicated-database: uses ON CLUSTER

CREATE TABLE t_url_ipv6 (a String) ENGINE = URL('http://[::1/x', 'CSV'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_url_port (a String) ENGINE = URL('http://h:999999999/x', 'CSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://[::1/x', 'CSV', 'a String'); -- { serverError BAD_ARGUMENTS }
DESCRIBE url('http://[::1/x', 'CSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_shard_localhost', 'http://[::1/x', 'CSV', 'a String'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://[::1/*.csv', 'CSV', 'a String') SETTINGS allow_url_wildcard_from_index_pages = 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://127.0.0.1:1/bad\tpath/*.csv', 'CSV', 'a String') SETTINGS allow_url_wildcard_from_index_pages = 1, use_hive_partitioning = 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('http://127.0.0.1:1/bad%zzpath/*.csv', 'CSV', 'a String') SETTINGS allow_url_wildcard_from_index_pages = 1, use_hive_partitioning = 0, enable_url_encoding = 1; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_s3 (a String) ENGINE = S3('http://[::1/b/k', 'CSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://[::1/b/k', 'CSV', 'a String'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_s3_query (a String) ENGINE = S3('http://127.0.0.1:1/b/k?versionId=%', 'CSV'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_disk (a Int8) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(type = 's3', endpoint = 'http://[::1/b/', access_key_id = 'a', secret_access_key = 'b'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_hdfs (a String) ENGINE = HDFS('hdfs://[::1/x', 'CSV'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_hdfs_port (a String) ENGINE = HDFS('hdfs://h:999999999/x', 'CSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfs('hdfs://[::1/x', 'CSV', 'a String'); -- { serverError BAD_ARGUMENTS }
DESCRIBE hdfs('hdfs://[::1/x', 'CSV'); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = HDFS('hdfs://[::1/'); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = HDFS;
DESCRIBE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`hdfs://[::1/x`; -- { serverError BAD_ARGUMENTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE t_hdfs_disk (a Int8) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(type = 'hdfs', endpoint = 'hdfs://[::1/x/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_iceberg_hdfs ENGINE = IcebergHDFS('hdfs://[::1/x/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_azure (a String) ENGINE = AzureBlobStorage('http://[::1/a', 'c', 'x.csv', 'acc', 'a2V5', 'CSV'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_iceberg_azure ENGINE = IcebergAzure('http://[::1/a', 'c', 'p', 'acc', 'a2V5'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_azure_queue (a String) ENGINE = AzureQueue('http://[::1/a', 'c', 'x/*.csv', 'acc', 'a2V5', 'CSV')
    SETTINGS mode = 'unordered'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_azure_queue_move (a String) ENGINE = AzureQueue('http://127.0.0.1:1/a', 'c', 'x/*.csv', 'acc', 'a2V5', 'CSV')
    SETTINGS mode = 'unordered', after_processing = 'move', after_processing_move_container = 'm',
    after_processing_move_connection_string = 'http://[::1/m?sv=1&sig=x'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_hive (a String) ENGINE = Hive('thrift://[::1/x', 'db', 't') PARTITION BY tuple(); -- { serverError BAD_ARGUMENTS }

SET allow_database_iceberg = 1, allow_database_glue_catalog = 1, allow_database_unity_catalog = 1, enable_database_paimon_rest_catalog = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://[::1/x')
    SETTINGS catalog_type = 'rest', warehouse = 'w'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://127.0.0.1:1/x')
    SETTINGS catalog_type = 'rest', warehouse = 'w', catalog_credential = 'a:b', oauth_server_uri = 'http://[::1/t'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://127.0.0.1:1/%zz')
    SETTINGS catalog_type = 'rest', warehouse = 'w', catalog_credential = 'a:b'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://127.0.0.1:1/%zz')
    SETTINGS catalog_type = 'rest', warehouse = 'w', catalog_credential = 'a:b', oauth_server_use_request_body = 0; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://127.0.0.1:1/x')
    SETTINGS catalog_type = 'onelake', warehouse = 'w', onelake_tenant_id = '%zz', onelake_client_id = 'c',
    onelake_client_secret = 's', onelake_refresh_token = 'r'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://127.0.0.1:1/x')
    SETTINGS catalog_type = 'onelake', warehouse = 'w', onelake_tenant_id = 't', onelake_client_id = 'c',
    onelake_client_secret = 's', onelake_refresh_token = 'r', oauth_server_uri = 'http://[::1/t'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://[::1/x')
    SETTINGS catalog_type = 'glue', region = 'us-east-1', aws_access_key_id = 'a', aws_secret_access_key = 'b'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://[::1/x')
    SETTINGS catalog_type = 'paimon_rest', warehouse = 'w', catalog_credential = 'a'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://[::1/x')
    SETTINGS catalog_type = 's3tables', warehouse = 'arn:aws:s3tables:us-east-1:1:bucket/b', region = 'us-east-1',
    aws_access_key_id = 'a', aws_secret_access_key = 'b'; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://[::1/x')
    SETTINGS catalog_type = 'unity', warehouse = 'w';
SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError BAD_ARGUMENTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://[::1/x')
    SETTINGS catalog_type = 'unity', warehouse = 'w', use_unity_catalog_v2 = 1, catalog_credential = 'a:b';
SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError BAD_ARGUMENTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://127.0.0.1:1/x')
    SETTINGS catalog_type = 'unity', warehouse = 'w', use_unity_catalog_v2 = 1, catalog_credential = 'a:b', oauth_server_uri = 'http://[::1/t';
SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError BAD_ARGUMENTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SET distributed_ddl_output_mode = 'throw';
CREATE TABLE t_url_on_cluster ON CLUSTER test_shard_localhost (a String) ENGINE = URL('http://[::1/x', 'CSV') FORMAT Null; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_hdfs_on_cluster ON CLUSTER test_shard_localhost (a String) ENGINE = HDFS('hdfs://[::1/x', 'CSV') FORMAT Null; -- { serverError BAD_ARGUMENTS }
