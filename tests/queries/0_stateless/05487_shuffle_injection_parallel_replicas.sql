-- The injected `SHUFFLE` must survive conversion back to SQL on parallel replicas with its user gate disabled.
SET allow_experimental_shuffle_query = 0;
SET make_distributed_plan = 0, serialize_query_plan = 0;
SET automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0, parallel_replicas_plan_based = 0;
SET max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

DROP TABLE IF EXISTS shuffle_injection_replicated;
CREATE TABLE shuffle_injection_replicated (number UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/shuffle_injection_replicated', 'r1') ORDER BY number;

SELECT number FROM shuffle_injection_replicated
SETTINGS enable_parallel_replicas = 1, inject_random_order_for_select_without_order_by = 1 FORMAT Null;

INSERT INTO shuffle_injection_replicated SELECT number FROM numbers(1000);
SELECT number FROM shuffle_injection_replicated
SETTINGS enable_parallel_replicas = 1, inject_random_order_for_select_without_order_by = 1, parallel_replicas_min_number_of_rows_per_replica = 0 FORMAT Null;
SELECT count(), sum(number) FROM shuffle_injection_replicated
SETTINGS enable_parallel_replicas = 1, inject_random_order_for_select_without_order_by = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

-- An explicit user `SHUFFLE` is still rejected before injection runs.
SELECT number FROM numbers(10) SHUFFLE SETTINGS inject_random_order_for_select_without_order_by = 1; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE shuffle_injection_replicated;
