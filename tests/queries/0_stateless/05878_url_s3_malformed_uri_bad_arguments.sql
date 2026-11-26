-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: depends on AWS S3
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
CREATE TABLE t_disk (a Int8) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(type = 's3', endpoint = 'http://[::1/b/', access_key_id = 'a', secret_access_key = 'b'); -- { serverError BAD_ARGUMENTS }

SET distributed_ddl_output_mode = 'throw';
CREATE TABLE t_url_on_cluster ON CLUSTER test_shard_localhost (a String) ENGINE = URL('http://[::1/x', 'CSV') FORMAT Null; -- { serverError BAD_ARGUMENTS }
