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

-- `hash` is only able to run an `ASOF` join on a numeric/`Decimal`/`DateTime64` closest-match column (hence the
-- `String` key above), so this part uses a numeric key: the point here is only to confirm that explicitly listing
-- `hash` ahead of `full_sorting_merge` still builds the filter as before this change, not to exercise the type
-- restriction again.
DROP TABLE IF EXISTS t_rf_asof_fsm_num_left;
DROP TABLE IF EXISTS t_rf_asof_fsm_num_right;

CREATE TABLE t_rf_asof_fsm_num_left (id UInt64, ts UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE t_rf_asof_fsm_num_right (id UInt64, ts UInt64, val UInt64) ENGINE = MergeTree ORDER BY (id, ts);

INSERT INTO t_rf_asof_fsm_num_left VALUES (1, 2), (1, 4), (2, 3), (3, 9);
INSERT INTO t_rf_asof_fsm_num_right VALUES (1, 1, 10), (1, 3, 11), (3, 5, 30);

SELECT '--- hash listed first: runtime filter is still built, same as before this test ---';

SELECT replaceRegexpOne(explain, '^[\\s└├─│]+', '')
FROM (
    EXPLAIN PLAN
    SELECT l.id, l.ts, r.ts, r.val
    FROM t_rf_asof_fsm_num_left l ASOF INNER JOIN t_rf_asof_fsm_num_right r ON l.id = r.id AND l.ts >= r.ts
    ORDER BY l.id, l.ts
    SETTINGS join_algorithm = 'hash,full_sorting_merge', enable_join_runtime_filters = 1
)
WHERE explain LIKE '%Build runtime join filter%';

DROP TABLE t_rf_asof_fsm_left;
DROP TABLE t_rf_asof_fsm_right;
DROP TABLE t_rf_asof_fsm_num_left;
DROP TABLE t_rf_asof_fsm_num_right;
