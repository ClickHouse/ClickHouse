SET allow_experimental_shuffle_query = 1;
SET serialize_query_plan = 1, enable_parallel_replicas = 0, prefer_localhost_replica = 0;
SET make_distributed_plan = 0;
SET max_threads = 2, max_block_size = 17;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;
SET max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;
SET query_plan_use_shuffle_for_order_by_rand = 1;

-- Both full and bounded `SHUFFLE` plans carry an appended key from shards to the initiator.
SELECT count(), sum(number) FROM
    (SELECT number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE);
SELECT count(), uniqExact((s, number)) FROM
    (SELECT shardNum() AS s, number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE LIMIT 100);

-- The replacement-key variant must preserve its descending direction.
SELECT count(), arrayReverseSort(groupArray(r)) = groupArray(r) FROM
    (SELECT rand() AS r FROM remote('127.0.0.{1,2}', numbers(1000)) ORDER BY r DESC LIMIT 100);

-- Also exercise the distributed-plan path used by the targeted CI job.
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT count(), sum(number) FROM
    (SELECT number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE);
SELECT count(), uniqExact((s, number)) FROM
    (SELECT shardNum() AS s, number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE LIMIT 100);
