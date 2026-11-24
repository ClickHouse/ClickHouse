-- The row policy filter is rebuilt on every step of a recursive CTE and must not retain memory per step.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1);
CREATE ROW POLICY 05871_p ON t USING x = 1 TO ALL;

WITH RECURSIVE r AS
(
    SELECT toUInt64(0) AS n
    UNION ALL
    SELECT r.n + 1 FROM r CROSS JOIN t WHERE r.n < 3000
)
SELECT max(n) FROM r
SETTINGS max_recursive_cte_evaluation_depth = 10000, max_memory_usage = 40000000;

DROP ROW POLICY 05871_p ON t;
DROP TABLE t;
