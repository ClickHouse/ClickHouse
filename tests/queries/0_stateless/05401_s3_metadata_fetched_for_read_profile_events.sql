-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: Depends on S3
-- Tag no-parallel-replicas: compares the S3 requests of a query, and under parallel replicas they are made by other replicas

-- Object metadata requests made to read objects are counted in `ObjectStorageMetadataFetchedForRead`,
-- also when they are made by the file iterator, as for explicit and brace-expanded keys.

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/05401_data/' || currentDatabase() || '_a.csv', structure = 'number UInt64', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(10);
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/05401_data/' || currentDatabase() || '_b.csv', structure = 'number UInt64', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(10, 10);

SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/05401_data/' || currentDatabase() || '_a.csv', structure = 'number UInt64', format = CSV)
    SETTINGS log_comment = '05401_explicit_key';
SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/05401_data/' || currentDatabase() || '_{a,b}.csv', structure = 'number UInt64', format = CSV)
    SETTINGS log_comment = '05401_brace_expansion';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ObjectStorageMetadataFetchedForRead'] AS fetched,
    fetched = ProfileEvents['S3HeadObject'],
    ProfileEvents['ObjectStorageMetadataFetchMicroseconds'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment IN ('05401_explicit_key', '05401_brace_expansion')
ORDER BY log_comment;
