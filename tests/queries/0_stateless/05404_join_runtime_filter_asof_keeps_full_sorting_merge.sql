-- `supportsRuntimeFilter` only keeps `hash`/`parallel_hash` for an `ASOF` join (never `full_sorting_merge`, see
-- 04812_join_runtime_filter_asof), so `tryAddJoinRuntimeFilter` must not erase `full_sorting_merge` from
-- `join_algorithms` when it was listed ahead of `hash`: doing so would force `HashJoin` to run the `ASOF` join,
-- which only supports numeric/`Decimal`/`DateTime64` closest-match columns and cannot spill, unlike
-- `full_sorting_merge`. The filter must simply not be built in that case, leaving `full_sorting_merge` in charge.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS t_rf_asof_fsm_left;
DROP TABLE IF EXISTS t_rf_asof_fsm_right;

CREATE TABLE t_rf_asof_fsm_left (id UInt64, ts String) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE t_rf_asof_fsm_right (id UInt64, ts String, val UInt64) ENGINE = MergeTree ORDER BY (id, ts);

INSERT INTO t_rf_asof_fsm_left VALUES (1, 'b'), (1, 'd'), (2, 'c'), (3, 'z');
INSERT INTO t_rf_asof_fsm_right VALUES (1, 'a', 10), (1, 'c', 11), (3, 'x', 30);

SELECT '--- full_sorting_merge listed first: ASOF on a String key must still succeed with the filter enabled ---';

SELECT l.id, l.ts, r.ts, r.val
FROM t_rf_asof_fsm_left l ASOF INNER JOIN t_rf_asof_fsm_right r ON l.id = r.id AND l.ts >= r.ts
ORDER BY l.id, l.ts
SETTINGS join_algorithm = 'full_sorting_merge,hash', enable_join_runtime_filters = 0;

SELECT l.id, l.ts, r.ts, r.val
FROM t_rf_asof_fsm_left l ASOF INNER JOIN t_rf_asof_fsm_right r ON l.id = r.id AND l.ts >= r.ts
ORDER BY l.id, l.ts
SETTINGS join_algorithm = 'full_sorting_merge,hash', enable_join_runtime_filters = 1;

SELECT '--- full_sorting_merge listed first: no runtime filter is built (would force hash) ---';

SELECT replaceRegexpOne(explain, '^[\\s└├─│]+', '')
FROM (
    EXPLAIN PLAN
    SELECT l.id, l.ts, r.ts, r.val
    FROM t_rf_asof_fsm_left l ASOF INNER JOIN t_rf_asof_fsm_right r ON l.id = r.id AND l.ts >= r.ts
    ORDER BY l.id, l.ts
    SETTINGS join_algorithm = 'full_sorting_merge,hash', enable_join_runtime_filters = 1
)
WHERE explain LIKE '%Build runtime join filter%';

SELECT '--- hash listed first: runtime filter is still built, same as before this test ---';

SELECT replaceRegexpOne(explain, '^[\\s└├─│]+', '')
FROM (
    EXPLAIN PLAN
    SELECT l.id, l.ts, r.ts, r.val
    FROM t_rf_asof_fsm_left l ASOF INNER JOIN t_rf_asof_fsm_right r ON l.id = r.id AND l.ts >= r.ts
    ORDER BY l.id, l.ts
    SETTINGS join_algorithm = 'hash,full_sorting_merge', enable_join_runtime_filters = 1
)
WHERE explain LIKE '%Build runtime join filter%';

DROP TABLE t_rf_asof_fsm_left;
DROP TABLE t_rf_asof_fsm_right;
