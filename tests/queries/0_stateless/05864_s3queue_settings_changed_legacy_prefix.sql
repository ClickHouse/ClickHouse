-- Tags: no-fasttest, zookeeper
-- no-fasttest: the `S3Queue` engine is not built in the fast test.

-- `system.s3_queue_settings` reports `changed = 1` for a setting written with the legacy `s3queue_` prefix,
-- as it does for the same setting written without the prefix.

DROP TABLE IF EXISTS t_legacy;
DROP TABLE IF EXISTS t_canonical;

CREATE TABLE t_legacy (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_legacy/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', s3queue_loading_retries = 7, s3queue_enable_logging_to_s3queue_log = 1;

CREATE TABLE t_canonical (a String)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/t_canonical/*', NOSIGN, 'CSV')
SETTINGS mode = 'unordered', loading_retries = 7, enable_logging_to_queue_log = 1;

SELECT table, name, value, changed
FROM system.s3_queue_settings
WHERE database = currentDatabase() AND name IN ('mode', 'loading_retries', 'enable_logging_to_queue_log', 'tracked_files_limit')
ORDER BY table, name;

DROP TABLE t_legacy;
DROP TABLE t_canonical;
