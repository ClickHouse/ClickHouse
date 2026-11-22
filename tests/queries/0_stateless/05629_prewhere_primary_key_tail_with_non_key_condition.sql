-- Random settings limits: optimize_move_to_prewhere=(1, 1); allow_reorder_prewhere_conditions=(1, 1); enable_multiple_prewhere_read_steps=(1, 1); use_statistics=(1, 1); materialize_statistics_on_insert=(1, 1); query_plan_optimize_join_order_randomize=(0, 0); join_runtime_filter_min_probe_rows=(0, 0); query_plan_merge_filters=(1, 1)

-- A condition on a primary key column that cannot use the key, e.g. a join runtime filter on the join key,
-- must not take the primary key conditions out of the end of PREWHERE.

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET move_primary_key_columns_to_end_of_prewhere = 1, move_all_conditions_to_prewhere = 1;
SET enable_join_runtime_filters = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;

CREATE TABLE t_probe (key String, kind String, payload String) ENGINE = MergeTree ORDER BY (key, kind)
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_probe SELECT ['a1', 'a2', 'a3', 'a4'][1 + number % 4], if(number % 100 = 0, 'e2', 'e3'),
    concat('i', toString(1 + intHash32(number) % 4), repeat('z', 20)) FROM numbers(100000);
CREATE TABLE t_build (key String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_build VALUES ('a1'), ('a2'), ('a3');

-- `kind` and `key` stay at the end of PREWHERE, after `payload`: with a join runtime filter on `key` ...
SELECT count() FROM (EXPLAIN actions = 1
    SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.key = b.key
    WHERE p.key IN ('a1', 'a2', 'a3') AND p.kind = 'e3' AND p.payload = 'i9')
WHERE explain ILIKE '%Prewhere filter column: and(%payload, \'i9\'%kind, \'e3\'%in(%key, %'
    AND explain ILIKE '%Prewhere filter column: and(%applyFilter(%key)%';

-- ... with a condition on `key` that cannot use the primary key ...
SELECT count() FROM (EXPLAIN actions = 1
    SELECT count() FROM t_probe WHERE key IN ('a1', 'a2', 'a3') AND length(key) = 2 AND kind = 'e3' AND payload = 'i9')
WHERE explain ILIKE '%Prewhere filter column: and(%payload, \'i9\'%kind, \'e3\'%in(%key, %';

-- ... and with one that may throw, which stays after the condition written before it on `key`.
SELECT count() FROM (EXPLAIN actions = 1
    SELECT count() FROM t_probe WHERE key IN ('a1', 'a2', 'a3') AND position(key, 'a') > 0 AND kind = 'e3' AND payload = 'i9')
WHERE explain ILIKE '%Prewhere filter column: and(%payload, \'i9\'%kind, \'e3\'%'
    AND explain ILIKE '%Prewhere filter column: and(%in(%key, %position(%key, %';

DROP TABLE t_probe;
DROP TABLE t_build;
