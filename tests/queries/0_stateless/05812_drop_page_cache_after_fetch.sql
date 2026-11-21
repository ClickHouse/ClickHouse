-- Tags: replica, no-replicated-database, no-shared-merge-tree
-- no-shared-merge-tree: the part has to be fetched from the other replica

DROP TABLE IF EXISTS drop_page_cache_after_fetch_r1 SYNC;
DROP TABLE IF EXISTS drop_page_cache_after_fetch_r2 SYNC;

CREATE TABLE drop_page_cache_after_fetch_r1 (x UInt64, s String, PROJECTION p (SELECT s, sum(x) GROUP BY s))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05812/drop_page_cache_after_fetch', 'r1')
ORDER BY x;

CREATE TABLE drop_page_cache_after_fetch_r2 (x UInt64, s String, PROJECTION p (SELECT s, sum(x) GROUP BY s))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05812/drop_page_cache_after_fetch', 'r2')
ORDER BY x
SETTINGS min_compressed_bytes_to_drop_page_cache_after_fetch = 1;

INSERT INTO drop_page_cache_after_fetch_r1 SELECT number, toString(number % 10) FROM numbers(100000);

SYSTEM SYNC REPLICA drop_page_cache_after_fetch_r2;

SELECT count(), sum(x) FROM drop_page_cache_after_fetch_r2;
SELECT s, sum(x) FROM drop_page_cache_after_fetch_r2 GROUP BY s ORDER BY s;
SELECT count() > 0 FROM system.projection_parts WHERE database = currentDatabase() AND table = 'drop_page_cache_after_fetch_r2' AND active;

SYSTEM FLUSH LOGS part_log;

SELECT count() > 0
FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 'drop_page_cache_after_fetch_r2'
    AND event_type = 'DownloadPart' AND error = 0;

DROP TABLE drop_page_cache_after_fetch_r1 SYNC;
DROP TABLE drop_page_cache_after_fetch_r2 SYNC;
