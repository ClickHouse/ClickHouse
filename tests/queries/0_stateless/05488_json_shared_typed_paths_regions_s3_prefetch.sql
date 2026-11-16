-- Tags: no-fasttest
-- no-fasttest: needs the S3 disk.

-- SHARED typed paths of v2 parts on an S3 disk with the filesystem cache read over their regions in several threads with
-- prefetches, compared with a Memory table: the views of the regions are prefetched, the prefixes are deserialized in
-- the thread pool and cached, and the queries run again from the caches.

SET max_threads = 4, max_block_size = 13, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1, filesystem_prefetch_step_marks = 0;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_s3;

CREATE TABLE t_mem (x UInt64, json JSON(
    a Nullable(String), b Array(Nullable(UInt32)), c LowCardinality(String), e Dynamic(max_types = 2),
    h JSON(max_dynamic_paths = 1), `s.p1` UInt32, `s.p2` String))
ENGINE = Memory;

CREATE TABLE t_s3 (x UInt64, json JSON(
    SHARED a Nullable(String), SHARED b Array(Nullable(UInt32)), SHARED c LowCardinality(String), SHARED e Dynamic(max_types = 2),
    SHARED h JSON(max_dynamic_paths = 1), SHARED `s.p1` UInt32, SHARED `s.p2` String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', storage_policy = 's3_cache';

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"a":', if(number % 3 = 0, 'null', concat('"', repeat('a', number % 5), '"')),
    ',"b":', toJSONString(arrayMap(i -> if(i % 2 = 1, NULL, i), range(number % 4))),
    ',"c":"v', toString(number % 3), '"',
    ',"e":', multiIf(number % 4 = 0, toString(number), number % 4 = 1, concat('"s', toString(number), '"'), number % 4 = 2, '[1,2]', 'true'),
    ',"h":{"k0":', toString(number), ',"k1":"h', toString(number), '"}',
    ',"s":{"p1":', toString(number), ',"p2":"', toString(number * 2), '"}',
    '}') AS json FROM numbers(2000);

INSERT INTO t_mem SELECT x, json FROM src;
-- Several parts, read by several threads.
INSERT INTO t_s3 SELECT x, json FROM src WHERE x < 700;
INSERT INTO t_s3 SELECT x, json FROM src WHERE x >= 700 AND x < 1400;
INSERT INTO t_s3 SELECT x, json FROM src WHERE x >= 1400;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_s3' AND active;

SELECT '-- first run';
SELECT 'json.b.size0', (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_s3) = (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_mem);
SELECT 'json.a.null', (SELECT arraySort(groupArray((x, json.a.null))) FROM t_s3) = (SELECT arraySort(groupArray((x, json.a.null))) FROM t_mem);
SELECT 'json.c', (SELECT arraySort(groupArray((x, toString(json.c)))) FROM t_s3) = (SELECT arraySort(groupArray((x, toString(json.c)))) FROM t_mem);
SELECT 'json.e.String', (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_s3) = (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_mem);
SELECT 'json.h.k1', (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_s3) = (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_mem);
SELECT 'json.^s', (SELECT arraySort(groupArray((x, toString(json.^s)))) FROM t_s3) = (SELECT arraySort(groupArray((x, toString(json.^s)))) FROM t_mem);
SELECT 'everything', (SELECT arraySort(groupArray((x, toString(json.a), json.b.size0, toString(json.c), toString(json.e), toString(json.h.k1), json.s.p1, toString(json)))) FROM t_s3)
    = (SELECT arraySort(groupArray((x, toString(json.a), json.b.size0, toString(json.c), toString(json.e), toString(json.h.k1), json.s.p1, toString(json)))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.b), toString(json.h.k1)))) FROM t_s3 WHERE x % 97 < 5)
    = (SELECT arraySort(groupArray((x, toString(json.b), toString(json.h.k1)))) FROM t_mem WHERE x % 97 < 5);
SELECT 'PREWHERE', (SELECT arraySort(groupArray((x, toString(json.e), json.s.p2))) FROM t_s3 PREWHERE json.s.p1 % 11 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.e), json.s.p2))) FROM t_mem WHERE json.s.p1 % 11 = 0);

SELECT '-- second run, from the caches';
SELECT 'json.b.size0', (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_s3) = (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_mem);
SELECT 'json.e.String', (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_s3) = (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_mem);
SELECT 'json.h.k1', (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_s3) = (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_mem);
SELECT 'everything', (SELECT arraySort(groupArray((x, toString(json.a), json.b.size0, toString(json.c), toString(json.e), toString(json.h.k1), json.s.p1, toString(json)))) FROM t_s3)
    = (SELECT arraySort(groupArray((x, toString(json.a), json.b.size0, toString(json.c), toString(json.e), toString(json.h.k1), json.s.p1, toString(json)))) FROM t_mem);
SELECT 'PREWHERE', (SELECT arraySort(groupArray((x, toString(json.e), json.s.p2))) FROM t_s3 PREWHERE json.s.p1 % 11 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.e), json.s.p2))) FROM t_mem WHERE json.s.p1 % 11 = 0);

-- Without the columns cache, which would serve the columns the queries above read.
SELECT toString(json.h.k1), json.b.size0 FROM t_s3 SETTINGS log_comment = '05488_s3', use_columns_cache = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'layouts read', ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05488_s3';

DROP TABLE t_mem;
DROP TABLE t_s3;
