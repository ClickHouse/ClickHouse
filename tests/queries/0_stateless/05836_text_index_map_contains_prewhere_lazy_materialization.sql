-- Lazy materialization must not read past the header when the direct read from a text index on `mapKeys`
-- rewrites both `mapContains(m, k)` and `m[k]` in an explicit PREWHERE, including the local plan of parallel replicas.

SET enable_full_text_index = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    ts DateTime,
    payload String,
    flags Map(LowCardinality(String), Bool),
    INDEX idx_flags_keys mapKeys(flags) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 8;

INSERT INTO tab SELECT toDateTime('2026-01-01 00:00:00') + number, toString(number), map('ok', number % 5 = 0, 'other', true) FROM numbers(100);

SELECT '-- without parallel replicas';

SELECT ts, payload FROM tab
PREWHERE mapContains(flags, 'ok') AND NOT flags['ok']
ORDER BY ts DESC
LIMIT 3;

SELECT ts, payload FROM tab
PREWHERE mapContains(flags, 'ok') AND flags['ok']
ORDER BY ts DESC
LIMIT 3;

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;

SELECT '-- with parallel replicas';

SELECT ts, payload FROM tab
PREWHERE mapContains(flags, 'ok') AND NOT flags['ok']
ORDER BY ts DESC
LIMIT 3;

SELECT ts, payload FROM tab
PREWHERE mapContains(flags, 'ok') AND flags['ok']
ORDER BY ts DESC
LIMIT 3;

DROP TABLE tab;
