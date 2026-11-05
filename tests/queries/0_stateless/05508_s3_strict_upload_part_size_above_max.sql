-- Tags: no-fasttest
-- Tag no-fasttest: needs S3

-- s3_strict_upload_part_size above s3_max_upload_part_size is rejected up front, for an S3 disk and for s3() query settings.

CREATE TABLE t_strict (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05508/', no_sign_request = 1, skip_access_check = 1,
    s3_strict_upload_part_size = 2097152, s3_max_upload_part_size = 1048576); -- { serverError INVALID_SETTING_VALUE }

INSERT INTO FUNCTION s3('http://localhost:11111/test/05508.tsv', NOSIGN, 'TSV', 'x UInt8')
SETTINGS s3_strict_upload_part_size = 2097152, s3_max_upload_part_size = 1048576 SELECT 1; -- { serverError INVALID_SETTING_VALUE }

-- With validation turned off, the oversized first part fails as a configuration error.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05508_no_validation.tsv', NOSIGN, 'TSV', 's String')
SETTINGS s3_validate_request_settings = 0, s3_strict_upload_part_size = 2097152, s3_max_upload_part_size = 1048576,
    s3_truncate_on_insert = 1
SELECT randomString(3000000); -- { serverError INVALID_CONFIG_PARAMETER }

CREATE TABLE t_strict_eq_max (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05508/', no_sign_request = 1, skip_access_check = 1,
    s3_strict_upload_part_size = 1048576, s3_max_upload_part_size = 1048576);

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_strict_eq_max';

DROP TABLE t_strict_eq_max;
