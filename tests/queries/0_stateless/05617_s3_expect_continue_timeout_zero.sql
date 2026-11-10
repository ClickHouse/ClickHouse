-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- A zero timeout would wait for `100 Continue` forever; `s3_expect_continue_min_body_size = 0` disables the header.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05617_' || currentDatabase() || '.csv', 'test', 'testtest', 'CSV', 'x UInt64')
SELECT 1 SETTINGS s3_truncate_on_insert = 1, s3_expect_continue_timeout_ms = 0; -- { serverError INVALID_SETTING_VALUE }

INSERT INTO FUNCTION s3('http://localhost:11111/test/05617_' || currentDatabase() || '.csv', 'test', 'testtest', 'CSV', 'x UInt64')
SELECT 1 SETTINGS s3_truncate_on_insert = 1, s3_expect_continue_timeout_ms = 0, s3_expect_continue_min_body_size = 0; -- { serverError INVALID_SETTING_VALUE }
