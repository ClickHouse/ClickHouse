-- Tags: shard

-- The set that is sent to a shard for a pushed down `IN` does not have a block for every block of the subquery.

SET distributed_product_mode = 'global', allow_push_predicate_ast_for_distributed_subqueries = 1, serialize_query_plan = 0,
    enable_parallel_replicas = 0, prefer_localhost_replica = 0, network_compression_method = 'none',
    send_profile_events = 0, interactive_delay = 1000000000; -- send little to the client

SELECT count() FROM (SELECT number FROM remote('127.0.0.2', numbers(10))) WHERE number IN (SELECT number FROM numbers(1000) SETTINGS max_block_size = 1)
    SETTINGS log_comment = '05644';

SYSTEM FLUSH LOGS query_log;

-- 8 bytes for a value. A block has more than 50 bytes of headers.
SELECT ProfileEvents['NetworkSendBytes'] BETWEEN 8000 AND 40000 FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05644' AND is_initial_query AND type = 'QueryFinish';
