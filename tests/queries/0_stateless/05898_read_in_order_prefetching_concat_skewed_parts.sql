-- Tags: no-random-settings, no-random-merge-tree-settings

-- A read in order concatenates the streams of one part (`PrefetchingConcat`) only when at least
-- one part gets more than one stream. When the parts take the whole stream budget (as many parts
-- as streams), no part can be split, so there is nothing to concatenate, and the read must keep
-- dividing the marks evenly among the streams: otherwise one large part next to tiny ones is read
-- by a single source, which serializes its reading, decompression and `PREWHERE`.

DROP TABLE IF EXISTS t_prefetching_concat_skewed;
CREATE TABLE t_prefetching_concat_skewed (key UInt64, part UInt8, value String)
ENGINE = MergeTree PARTITION BY part ORDER BY key
SETTINGS index_granularity = 1024;

-- One large part and three parts of a single granule.
INSERT INTO t_prefetching_concat_skewed SELECT number, 0, toString(number) FROM numbers(200000);
INSERT INTO t_prefetching_concat_skewed SELECT number, 1, toString(number) FROM numbers(100);
INSERT INTO t_prefetching_concat_skewed SELECT number, 2, toString(number) FROM numbers(100);
INSERT INTO t_prefetching_concat_skewed SELECT number, 3, toString(number) FROM numbers(100);
OPTIMIZE TABLE t_prefetching_concat_skewed FINAL;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_prefetching_concat_skewed' AND active;

-- Four parts and four streams: the large part is still divided among several sources.
SELECT 'as_many_parts_as_streams_no_prefetching';
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_skewed
    PREWHERE value LIKE '%5%'
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'as_many_parts_as_streams_large_part_is_split';
SELECT sum(streams) > 4 FROM (
    SELECT
        if(match(explain, 'MergeTreeSelect.*× (\\d+)'),
           toUInt32(extract(explain, 'MergeTreeSelect.*× (\\d+)')),
           1) AS streams
    FROM (
        EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_skewed
        PREWHERE value LIKE '%5%'
        ORDER BY key
        SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
                 merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
    )
    WHERE explain LIKE '%MergeTreeSelect%'
);

SELECT 'as_many_parts_as_streams_correctness';
SELECT count(), groupArray(key) = arraySort(groupArray(key)) FROM (
    SELECT key FROM t_prefetching_concat_skewed PREWHERE value LIKE '%5%' ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

-- With streams to spare the large part is split on its own and its streams are concatenated.
SELECT 'more_streams_than_parts_prefetching';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT key FROM t_prefetching_concat_skewed
    PREWHERE value LIKE '%5%'
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 8, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'more_streams_than_parts_correctness';
SELECT count(), groupArray(key) = arraySort(groupArray(key)) FROM (
    SELECT key FROM t_prefetching_concat_skewed PREWHERE value LIKE '%5%' ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 8, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

DROP TABLE t_prefetching_concat_skewed;
