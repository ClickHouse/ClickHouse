-- `FINAL` merges the columns it reads. A subcolumn read alone was combined as a column of its own type, not as a part
-- of its column, so it disagreed with the merged column: https://github.com/ClickHouse/ClickHouse/issues/110028

SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_coalescing;
DROP TABLE IF EXISTS t_summing;
DROP TABLE IF EXISTS t_aggregating;

CREATE TABLE t_coalescing (id UInt32, j Nullable(JSON), s Nullable(String), env Nullable(Tuple(v UInt64, arr Array(String))))
ENGINE = CoalescingMergeTree ORDER BY id;
SYSTEM STOP MERGES t_coalescing;
INSERT INTO t_coalescing (id, j, s, env) VALUES (1, '{"score":42}', 'x', (7, ['x', 'y']));
INSERT INTO t_coalescing (id) VALUES (1);

SELECT 'coalescing';
SELECT toString(j), j.score, s, s.null, env, env.v, env.arr, env.null FROM t_coalescing FINAL;
SELECT j.score, s.null, env.arr, env.null FROM t_coalescing FINAL;
SELECT count() FROM t_coalescing FINAL WHERE j.score = 42 AND NOT s.null AND has(env.arr, 'y');
SELECT count() FROM t_coalescing FINAL PREWHERE j.score = 42 SETTINGS apply_prewhere_after_final = 1;
SELECT j.score, s.null, env.arr, env.null FROM t_coalescing FINAL
SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, enable_parallel_replicas = 0;

-- `arr` is only used by an expression of the key, so the merged rows can have different values of it.
CREATE TABLE t_summing (id UInt32, t Tuple(a UInt64, b UInt64), arr Array(UInt64), n UInt64)
ENGINE = SummingMergeTree ORDER BY (id, length(arr));
SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing VALUES (1, (1, 10), [1, 2], 1);
INSERT INTO t_summing VALUES (1, (2, 20), [3, 4], 2);

SELECT 'summing';
SELECT t, t.a, t.b, arr, arr.size0, n FROM t_summing FINAL;
SELECT t.a, t.b, arr.size0 FROM t_summing FINAL;

CREATE TABLE t_aggregating
(
    id UInt32,
    m SimpleAggregateFunction(max, Tuple(a UInt64, b UInt64)),
    x SimpleAggregateFunction(anyLast, Nullable(UInt64))
)
ENGINE = AggregatingMergeTree ORDER BY id;
SYSTEM STOP MERGES t_aggregating;
INSERT INTO t_aggregating VALUES (1, (2, 9), NULL);
INSERT INTO t_aggregating VALUES (1, (5, 1), 3);

SELECT 'aggregating';
SELECT m, m.a, m.b, x, x.null FROM t_aggregating FINAL;
SELECT m.a, m.b, x.null FROM t_aggregating FINAL;

DROP TABLE t_coalescing;
DROP TABLE t_summing;
DROP TABLE t_aggregating;
