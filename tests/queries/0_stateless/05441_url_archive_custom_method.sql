DROP NAMED COLLECTION IF EXISTS url_archive_custom_method;
CREATE NAMED COLLECTION url_archive_custom_method AS
    url = 'http://127.0.0.1:1/missing.zip::entry.csv',
    format = 'CSV', structure = 'x UInt64', method = 'POST';

-- Reject archive syntax before either execution or schema inference can make HTTP requests.
SET allow_archive_path_syntax = 1;
SELECT * FROM url(url_archive_custom_method); -- { serverError BAD_ARGUMENTS }
SELECT * FROM urlCluster('test_shard_localhost', url_archive_custom_method); -- { serverError BAD_ARGUMENTS }
DESCRIBE TABLE url(url_archive_custom_method, structure = 'auto'); -- { serverError BAD_ARGUMENTS }
DESCRIBE TABLE urlCluster('test_shard_localhost', url_archive_custom_method, structure = 'auto'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(url_archive_custom_method, http_method = 'PUT'); -- { serverError BAD_ARGUMENTS }

-- With archive syntax disabled, the same URL remains literal and explicit schema needs no HTTP request.
SET allow_archive_path_syntax = 0;
DESCRIBE TABLE url(url_archive_custom_method);
DESCRIBE TABLE urlCluster('test_shard_localhost', url_archive_custom_method);

DROP NAMED COLLECTION url_archive_custom_method;
