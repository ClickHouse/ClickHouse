-- The read limits of the enclosing query apply to the reads of a `UNION` branch, as they do to a subquery in `FROM`,
-- also when the branch sets its own value. https://github.com/ClickHouse/ClickHouse/issues/133446
-- `WHERE x % 7 = 1` keeps `count()` from being answered without reading.

DROP TABLE IF EXISTS t_union_limits_big;
DROP TABLE IF EXISTS t_union_limits_tiny;

CREATE TABLE t_union_limits_big (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_union_limits_big SELECT number FROM numbers(1000000);
CREATE TABLE t_union_limits_tiny (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_union_limits_tiny VALUES (1);

-- A subquery in FROM with a looser limit of its own.
SELECT count() FROM
(
    SELECT x FROM t_union_limits_big WHERE x % 7 = 1 SETTINGS max_bytes_to_read = 100000000
)
SETTINGS max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }

-- UNION ALL branches with a looser limit of their own.
SELECT count() FROM
(
    (SELECT x FROM t_union_limits_big WHERE x % 7 = 1 SETTINGS max_bytes_to_read = 100000000)
    UNION ALL
    (SELECT x FROM t_union_limits_tiny SETTINGS max_bytes_to_read = 100000000)
)
SETTINGS max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }

-- A nested UNION.
SELECT count() FROM
(
    (SELECT x FROM t_union_limits_tiny)
    UNION ALL
    (
        (SELECT x FROM t_union_limits_big WHERE x % 7 = 1 SETTINGS max_bytes_to_read = 100000000)
        UNION ALL
        (SELECT x FROM t_union_limits_tiny SETTINGS max_bytes_to_read = 100000000)
    )
)
SETTINGS max_bytes_to_read = 1000; -- { serverError TOO_MANY_BYTES }

-- A stricter limit of a branch still applies.
SELECT count() FROM
(
    (SELECT x FROM t_union_limits_big WHERE x % 7 = 1 SETTINGS max_bytes_to_read = 1000)
    UNION ALL
    (SELECT x FROM t_union_limits_tiny)
)
SETTINGS max_bytes_to_read = 100000000; -- { serverError TOO_MANY_BYTES }

-- Within all limits.
SELECT count() FROM
(
    (SELECT x FROM t_union_limits_big WHERE x % 7 = 1 SETTINGS max_bytes_to_read = 100000000)
    UNION ALL
    (SELECT x FROM t_union_limits_tiny SETTINGS max_bytes_to_read = 100000000)
)
SETTINGS max_bytes_to_read = 100000000;

DROP TABLE t_union_limits_big;
DROP TABLE t_union_limits_tiny;
