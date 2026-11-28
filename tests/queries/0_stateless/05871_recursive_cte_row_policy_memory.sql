-- Tags: no-parallel-replicas
-- no-parallel-replicas: every read of `t` becomes a remote read, which needs far more memory than the limit.
-- The row policy filter is rebuilt for every read of `t` on every step of a recursive CTE and must not retain memory per step.
-- Sixteen reads per step keep the number of steps low: the flaky check runs many copies of the test under the thread fuzzer.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1);
CREATE ROW POLICY 05871_p ON t USING x = 1 TO ALL;

WITH RECURSIVE r AS
(
    SELECT toUInt64(0) AS n
    UNION ALL
    SELECT r.n + 1
    FROM r
    CROSS JOIN
    (
        SELECT x FROM
        (
            SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t
            UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t
            UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t
            UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t UNION ALL SELECT x FROM t
        )
        LIMIT 1
    ) AS s
    WHERE r.n < 50
)
SELECT max(n) FROM r
SETTINGS max_memory_usage = 10000000;

DROP ROW POLICY 05871_p ON t;
DROP TABLE t;
