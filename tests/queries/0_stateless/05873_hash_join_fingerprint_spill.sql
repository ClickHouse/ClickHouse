-- A legacy row limit triggers repartitioning. A final in-memory bucket can exceed it.
SET join_algorithm = 'grace_hash', legacy_join_size_limits_trigger_spilling = 1,
    grace_hash_join_initial_buckets = 1, max_rows_in_join = 1000, join_overflow_mode = 'throw',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    max_block_size = 65536, max_threads = 1, enable_join_runtime_filters = 0;

SELECT count(), sum(r.v)
FROM numbers(10000) AS l
ALL INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS r ON l.number = r.k
SETTINGS experimental_join_fingerprint_mode = 0;

SELECT count(), sum(r.v)
FROM numbers(10000) AS l
ALL INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS r ON l.number = r.k
SETTINGS experimental_join_fingerprint_mode = 1;

SELECT count(), sum(r.v)
FROM numbers(10000) AS l
ALL INNER JOIN (SELECT number AS k, number AS v FROM numbers(10000)) AS r ON l.number = r.k
SETTINGS experimental_join_fingerprint_mode = 2;
