-- A phrase search answered by a text index reads the postings and positions files of the index, which are not compressed.
-- Those reads are input bytes too, so the statistics must hold at least the positions bytes the read reports.

DROP TABLE IF EXISTS t_text;

CREATE TABLE t_text (k UInt64, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1))
ENGINE = MergeTree ORDER BY k
-- Without a posting list codec the phrase is not matched by the positions.
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'pfor', index_granularity = 8192,
    index_granularity_bytes = '10Mi';

INSERT INTO t_text SELECT intDiv(number, 100000),
    concat('w', toString(cityHash64(number) % 200), ' w', toString(cityHash64(number, 1) % 200), ' w', toString(cityHash64(number, 2) % 200))
FROM numbers(1000000);
OPTIMIZE TABLE t_text FINAL;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0, max_rows_to_read = 0;
-- The postings cache serves a posting list without reading it.
SET use_query_condition_cache = 0, use_text_index_postings_cache = 0, use_skip_indexes = 1,
    query_plan_direct_read_from_text_index = 1;

SELECT k, count() FROM t_text WHERE hasPhrase(message, 'w1 w2') GROUP BY k
FORMAT Null SETTINGS log_comment = '05871_phrase';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log, text_log;

SELECT q.ProfileEvents['TextIndexPositionsBytesRead'] > 0 AS positions_read,
    toUInt64(extract(t.message, 'input bytes=(\\d+)')) >= q.ProfileEvents['TextIndexPositionsBytesRead'] AS positions_counted
FROM system.text_log AS t
INNER JOIN system.query_log AS q ON t.query_id = q.query_id
WHERE q.event_date >= yesterday() AND q.event_time >= now() - INTERVAL 15 MINUTE AND q.current_database = currentDatabase()
    AND q.log_comment = '05871_phrase' AND q.type = 'QueryFinish'
    AND t.event_date >= yesterday() AND t.message LIKE 'Collected statistics:%';

DROP TABLE t_text;
