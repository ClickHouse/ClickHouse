-- Tags: no-fasttest
-- Tag no-fasttest: needs S3

-- s3_strict_upload_part_size above s3_max_upload_part_size is rejected up front, for an S3 disk and for s3() query settings.

CREATE TABLE t_strict (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05508/', no_sign_request = 1, skip_access_check = 1,
    s3_strict_upload_part_size = 2097152, s3_max_upload_part_size = 1048576, s3_min_upload_part_size = 1048576); -- { serverError INVALID_SETTING_VALUE }

INSERT INTO FUNCTION s3('http://localhost:11111/test/05508.tsv', NOSIGN, 'TSV', 'x UInt8')
SETTINGS s3_strict_upload_part_size = 2097152, s3_max_upload_part_size = 1048576, s3_min_upload_part_size = 1048576 SELECT 1; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE t_strict_eq_max (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05508/', no_sign_request = 1, skip_access_check = 1,
    s3_strict_upload_part_size = 1048576, s3_max_upload_part_size = 1048576, s3_min_upload_part_size = 1048576);

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_strict_eq_max';

DROP TABLE t_strict_eq_max;

-- With a strict size, the min is still checked: native copies (BACKUP to S3, copies between S3 disks) split by min and max.
CREATE TABLE t_strict_min_above_max (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05508/', no_sign_request = 1, skip_access_check = 1,
    s3_strict_upload_part_size = 1048576, s3_max_upload_part_size = 1048576, s3_min_upload_part_size = 16777216); -- { serverError INVALID_SETTING_VALUE }

INSERT INTO FUNCTION s3('http://localhost:11111/test/05508.tsv', NOSIGN, 'TSV', 'x UInt8')
SETTINGS s3_strict_upload_part_size = 1048576, s3_max_upload_part_size = 1048576, s3_min_upload_part_size = 16777216 SELECT 1; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE t_strict_min_zero (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05508/', no_sign_request = 1, skip_access_check = 1,
    s3_strict_upload_part_size = 1048576, s3_min_upload_part_size = 0); -- { serverError INVALID_SETTING_VALUE }

INSERT INTO FUNCTION s3('http://localhost:11111/test/05508.tsv', NOSIGN, 'TSV', 'x UInt8')
SETTINGS s3_strict_upload_part_size = 1048576, s3_min_upload_part_size = 0 SELECT 1; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE t_backup (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_backup VALUES (1);
BACKUP TABLE t_backup TO S3(s3_conn, 'backups/05508_strict_min_zero')
SETTINGS s3_strict_upload_part_size = 1048576, s3_min_upload_part_size = 0; -- { serverError INVALID_SETTING_VALUE }
DROP TABLE t_backup;
