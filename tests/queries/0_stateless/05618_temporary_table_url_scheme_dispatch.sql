-- Tags: no-fasttest
-- no-fasttest: the `s3://` scheme needs the `S3` engine, which the fast test build lacks.

-- The `URL` engine dispatches to another engine by scheme; a temporary table is checked against that engine too.
CREATE TEMPORARY TABLE t_s3 (x UInt64) ENGINE = URL('s3://bucket/key.csv', CSV); -- { serverError INCORRECT_QUERY }

-- `http` stays with `URL` itself.
CREATE TEMPORARY TABLE t_http (x UInt64) ENGINE = URL('http://127.0.0.1:8123/?query=SELECT+1', CSV);
SELECT engine FROM system.tables WHERE is_temporary AND name = 't_http'; -- No need in "database = currentDatabase()"

-- `File` may back a temporary table, but not in `cloud_mode`, where it stores data on the local disk.
CREATE TEMPORARY TABLE t_file (x UInt64) ENGINE = URL('file://05618_temporary_table_url_scheme_dispatch.csv', CSV);
SELECT engine FROM system.tables WHERE is_temporary AND name = 't_file'; -- No need in "database = currentDatabase()"
SET cloud_mode = 1;
CREATE TEMPORARY TABLE t_file_cloud (x UInt64) ENGINE = URL('file://05618_temporary_table_url_scheme_dispatch.csv', CSV); -- { serverError INCORRECT_QUERY }
