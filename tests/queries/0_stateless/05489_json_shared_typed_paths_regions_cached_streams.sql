-- Tags: no-fasttest
-- no-fasttest: needs the S3 disk.

-- SHARED typed paths of a v2 part on an S3 disk, read with prefetches once the part has listed the streams of their
-- subcolumns: the reader opens and prefetches the regions from that listing before the prefixes are read, also for
-- ranges that do not start at the first mark. Read together with the whole column or with a sub-object, the paths are
-- read from the plain layout while the listing names their regions. Compared with a Memory table.

SET max_threads = 4, max_block_size = 13, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1, filesystem_prefetch_step_marks = 0, use_columns_cache = 0;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_s3;

CREATE TABLE t_mem (x UInt64, json JSON(
    a Nullable(String), c LowCardinality(String), e Dynamic(max_types = 2), f Variant(UInt32, String), `s.p1` UInt32, `s.p2` String))
ENGINE = Memory;

CREATE TABLE t_s3 (x UInt64, json JSON(
    SHARED a Nullable(String), SHARED c LowCardinality(String), SHARED e Dynamic(max_types = 2), SHARED f Variant(UInt32, String),
    SHARED `s.p1` UInt32, SHARED `s.p2` String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', storage_policy = 's3_cache';

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"a":', if(number % 3 = 0, 'null', concat('"', repeat('a', number % 5), '"')),
    ',"c":"v', toString(number % 3), '"',
    ',"e":', multiIf(number % 4 = 0, toString(number), number % 4 = 1, concat('"s', toString(number), '"'), number % 4 = 2, '[1,2]', 'true'),
    ',"f":', if(number % 2 = 1, toString(number), concat('"t', toString(number), '"')),
    ',"s":{"p1":', toString(number), ',"p2":"', toString(number * 2), '"}',
    '}') AS json FROM numbers(2000);

INSERT INTO t_mem SELECT x, json FROM src;
INSERT INTO t_s3 SELECT x, json FROM src;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_s3' AND active;

-- The planning of a read lists the streams of every subcolumn it reads in the part.
SELECT json.a, json.c, json.e, json.f, json.s.p1 FROM t_s3 FORMAT Null;

SELECT 'subcolumns', (SELECT arraySort(groupArray((x, toString(json.a), toString(json.c), toString(json.e), toString(json.f), json.s.p1))) FROM t_s3)
    = (SELECT arraySort(groupArray((x, toString(json.a), toString(json.c), toString(json.e), toString(json.f), json.s.p1))) FROM t_mem);

SELECT 'ranges after the first mark', (SELECT arraySort(groupArray((x, toString(json.c), toString(json.e), toString(json.f)))) FROM t_s3 WHERE x BETWEEN 500 AND 1300)
    = (SELECT arraySort(groupArray((x, toString(json.c), toString(json.e), toString(json.f)))) FROM t_mem WHERE x BETWEEN 500 AND 1300);

SELECT 'with the whole column', (SELECT arraySort(groupArray((x, toString(json), toString(json.c)))) FROM t_s3)
    = (SELECT arraySort(groupArray((x, toString(json), toString(json.c)))) FROM t_mem);

SELECT 'with a sub-object', (SELECT arraySort(groupArray((x, toString(json.^s), json.s.p1))) FROM t_s3 SETTINGS object_shared_data_max_sub_object_paths_to_read_separately = 0)
    = (SELECT arraySort(groupArray((x, toString(json.^s), json.s.p1))) FROM t_mem);

SELECT toString(json.c), json.s.p1 FROM t_s3 SETTINGS log_comment = '05489_regions' FORMAT Null;
SELECT toString(json), toString(json.c) FROM t_s3 SETTINGS log_comment = '05489_plain' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'layouts read', log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05489_regions', '05489_plain')
ORDER BY log_comment;

DROP TABLE t_mem;
DROP TABLE t_s3;
