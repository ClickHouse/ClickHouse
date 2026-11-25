-- The ranges a read counts are told apart by file. A `JSON` typed path `postings` of a column `skp_idx_idx` is written to
-- `skp_idx_idx.postings.bin`, next to the files `skp_idx_idx.pst.idx` and `skp_idx_idx.pos.idx` of text index `idx`. The
-- two tables below differ only in the name of that column, so a query reading the column while filtering through the
-- index must record the same input bytes on both: on `t_collide` it used to merge the ranges of the column's streams with
-- those of the index files, as if they were one file.

DROP TABLE IF EXISTS t_collide;
DROP TABLE IF EXISTS t_apart;

CREATE TABLE t_collide (k UInt64, message String, skp_idx_idx JSON(postings UInt64, positions UInt64),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1))
ENGINE = MergeTree ORDER BY k
-- Without a posting list codec the phrase is not matched by the positions.
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'pfor', index_granularity = 8192,
    index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_apart (k UInt64, message String, other JSON(postings UInt64, positions UInt64),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'pfor', index_granularity = 8192,
    index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Each table is one part written from the same rows, so that the files of the two tables are the same. Every granule
-- holds the phrase, so the column is read from the start of its files to their end, over the offsets the index reads.
SET max_insert_block_size = 2000000, min_insert_block_size_rows = 2000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO t_collide SELECT intDiv(number, 100000),
    if(number % 1000 = 0, 'w1 w2', concat('w', toString(cityHash64(number) % 200), ' w', toString(cityHash64(number, 1) % 200), ' w', toString(cityHash64(number, 2) % 200))),
    toJSONString(map('postings', cityHash64(number, 3), 'positions', cityHash64(number, 4)))
FROM numbers(1000000);
INSERT INTO t_apart SELECT intDiv(number, 100000),
    if(number % 1000 = 0, 'w1 w2', concat('w', toString(cityHash64(number) % 200), ' w', toString(cityHash64(number, 1) % 200), ' w', toString(cityHash64(number, 2) % 200))),
    toJSONString(map('postings', cityHash64(number, 3), 'positions', cityHash64(number, 4)))
FROM numbers(1000000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0, max_rows_to_read = 0;
-- See `05871_autopr_input_bytes_text_index_postings`.
SET parallel_replicas_plan_based = 0;
-- The postings cache serves a posting list without reading it.
SET use_query_condition_cache = 0, use_text_index_postings_cache = 0, use_skip_indexes = 1,
    query_plan_direct_read_from_text_index = 1;

SELECT k, sum(skp_idx_idx.postings), sum(skp_idx_idx.positions) FROM t_collide WHERE hasPhrase(message, 'w1 w2') GROUP BY k
FORMAT Null SETTINGS log_comment = '05876_collide';
SELECT k, sum(other.postings), sum(other.positions) FROM t_apart WHERE hasPhrase(message, 'w1 w2') GROUP BY k
FORMAT Null SETTINGS log_comment = '05876_apart';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT min(input_bytes) > 0 AS measured, min(input_bytes) = max(input_bytes) AS same_input_bytes
FROM
(
    SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
        AND (current_database = currentDatabase())
        AND (log_comment IN ('05876_collide', '05876_apart'))
        AND (type = 'QueryFinish') AND is_initial_query
);

DROP TABLE t_collide;
DROP TABLE t_apart;
