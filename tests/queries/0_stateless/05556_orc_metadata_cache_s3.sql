-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
-- no-fasttest: requires the S3 test service and an ORC-enabled build
-- no-parallel: ORC metadata cache is shared, and this test checks its hit/miss counters
-- no-parallel-replicas: profile events are not available on the second replica
-- no-random-settings: the test checks explicit cache setting behavior

SET log_queries = 1;
SET s3_truncate_on_insert = 1;
SET max_threads = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05556_orc_metadata_cache_s3.orc', format = ORC)
SELECT number AS id, toString(number) AS value FROM numbers(10);

SYSTEM DROP ORC METADATA CACHE;

SELECT sum(id) FROM s3(s3_conn, filename = currentDatabase() || '/05556_orc_metadata_cache_s3.orc', format = ORC)
    SETTINGS use_orc_metadata_cache = 1, log_comment = '05556-first';

SELECT sum(id) FROM s3(s3_conn, filename = currentDatabase() || '/05556_orc_metadata_cache_s3.orc', format = ORC)
    SETTINGS use_orc_metadata_cache = 1, log_comment = '05556-second';

SELECT sum(id) FROM s3(s3_conn, filename = currentDatabase() || '/05556_orc_metadata_cache_s3.orc', format = ORC)
    SETTINGS use_orc_metadata_cache = 0, log_comment = '05556-disabled';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ORCMetadataCacheHits'], ProfileEvents['ORCMetadataCacheMisses']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05556-%'
ORDER BY log_comment;

SYSTEM DROP ORC METADATA CACHE;
