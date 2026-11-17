-- Tags: no-fasttest
-- An unknown key-value argument of the S3 parser is rejected instead of being ignored (issue #132003).

SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', foo_bar = 'x'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', SECRET_ACCESS_KEY = 'x'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', secret_acess_key = 'x'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', compression = 'none'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', NOSIGN, 'TSV', 'a UInt8, b UInt8, c UInt8', foo_bar = 'x'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM icebergS3('http://localhost:11111/test/05814_no_such_table/', foo_bar = 'x'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_05814_engine (a UInt8, b UInt8, c UInt8) ENGINE = S3('http://localhost:11111/test/a.tsv', 'TSV', foo_bar = 'x'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05814_as AS s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', foo_bar = 'x'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05814_queue (a String) ENGINE = S3Queue('http://localhost:11111/test/05814_queue/*', 'CSV', foo_bar = 'x') SETTINGS mode = 'unordered'; -- { serverError BAD_ARGUMENTS }

-- Every key the parser reads is still accepted.
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', format = 'TSV', structure = 'a UInt8, b UInt8, c UInt8', compression_method = 'none', no_sign = 1, storage_class_name = 'STANDARD');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', storage_class = 'STANDARD', partition_strategy = 'wildcard', partition_columns_in_data_file = 1, upload_checksum_algorithm = 'CRC32', delete_checksum_algorithm = 'CRC32');
SELECT count() FROM s3('http://localhost:11111/test/a.tsv', 'TSV', 'a UInt8, b UInt8, c UInt8', access_key_id = 'clickhouse', secret_access_key = 'clickhouse', session_token = '');
