-- The `conversion_time` mode of windowFunnel returns the tuple (max_level, time_1_to_2, ...).
-- time_i_to_(i+1) is the time between the steps i and i+1 on the fastest path to the step i+1,
-- i.e. the path that minimizes the time between the first step and the step i+1.

DROP TABLE IF EXISTS funnel_conv_time;
CREATE TABLE funnel_conv_time (uid UInt32, ts UInt64, step UInt8) ENGINE = Memory;

-- step 9 is an event that does not match any condition
INSERT INTO funnel_conv_time VALUES
    -- 1: the fastest path starts at the second step 1 event (2 and 8 instead of 100 and 20)
    (1, 0, 1), (1, 100, 2), (1, 110, 1), (1, 112, 2), (1, 120, 3),
    -- 2: a single path
    (2, 0, 1), (2, 10, 2), (2, 50, 3),
    -- 3: stops at the step 2
    (3, 0, 1), (3, 10, 2),
    -- 4: only the step 1
    (4, 0, 1),
    -- 5: the step 3 is out of the window
    (5, 0, 1), (5, 50, 2), (5, 5000, 3),
    -- 6: both paths take 10, the one that finishes first wins
    (6, 0, 1), (6, 4, 2), (6, 10, 3), (6, 20, 1), (6, 28, 2), (6, 30, 3),
    -- 7: paths with the same start, the earliest step 2 is used
    (7, 0, 1), (7, 2, 2), (7, 6, 2), (7, 10, 3),
    -- 8: steps with the same timestamp
    (8, 0, 1), (8, 0, 2), (8, 7, 3),
    -- 9: an unrelated event between the steps
    (9, 0, 1), (9, 10, 2), (9, 15, 9), (9, 20, 3),
    -- 10: the chain never starts
    (10, 5, 2), (10, 9, 3),
    -- 30: a single event that matches two steps
    (30, 0, 1), (30, 5, 3);

SELECT 'type';
SELECT toTypeName(windowFunnel(1000, 'conversion_time')(ts, step = 1, step = 2, step = 3)) FROM funnel_conv_time LIMIT 1;

SELECT 'conversion_time';
SELECT uid, windowFunnel(1000, 'conversion_time')(ts, step = 1, step = 2, step = 3) AS r
FROM funnel_conv_time WHERE uid <= 10 GROUP BY uid ORDER BY uid;

SELECT 'strict_increase';
SELECT uid, windowFunnel(1000, 'strict_increase', 'conversion_time')(ts, step = 1, step = 2, step = 3) AS r
FROM funnel_conv_time WHERE uid <= 10 GROUP BY uid ORDER BY uid;

SELECT 'strict_once';
SELECT uid, windowFunnel(1000, 'strict_once', 'conversion_time')(ts, step = 1, step = 2, step = 3) AS r
FROM funnel_conv_time WHERE uid <= 10 GROUP BY uid ORDER BY uid;

SELECT 'strict_order';
SELECT uid, windowFunnel(1000, 'strict_order', 'conversion_time')(ts, step = 1, step = 2, step = 3) AS r
FROM funnel_conv_time WHERE uid IN (2, 8, 9, 10) GROUP BY uid ORDER BY uid;

SELECT 'one event matching two steps';
SELECT 'default', windowFunnel(1000, 'conversion_time')(ts, step = 1, step IN (2, 3), step = 3) FROM funnel_conv_time WHERE uid = 30;
SELECT 'strict_once', windowFunnel(1000, 'strict_once', 'conversion_time')(ts, step = 1, step IN (2, 3), step = 3) FROM funnel_conv_time WHERE uid = 30;
SELECT 'strict_increase', windowFunnel(1000, 'strict_increase', 'conversion_time')(ts, step = 1, step IN (2, 3), step = 3) FROM funnel_conv_time WHERE uid = 30;

SELECT 'named elements';
SELECT uid, r.max_level, r.time_1_to_2, r.time_2_to_3
FROM (SELECT uid, windowFunnel(1000, 'conversion_time')(ts, step = 1, step = 2, step = 3) AS r FROM funnel_conv_time GROUP BY uid)
WHERE uid IN (1, 3) ORDER BY uid;

SELECT 'max level is the same as without conversion_time';
SELECT count(), countIf(a = b.1) FROM
(
    SELECT
        windowFunnel(1000, 'strict_increase')(ts, step = 1, step = 2, step = 3) AS a,
        windowFunnel(1000, 'strict_increase', 'conversion_time')(ts, step = 1, step = 2, step = 3) AS b
    FROM funnel_conv_time GROUP BY uid
);

SELECT 'no rows';
SELECT windowFunnel(1000, 'conversion_time')(ts, step = 1, step = 2, step = 3) FROM funnel_conv_time WHERE uid = 0;

SELECT 'a single step';
SELECT tupleElement(windowFunnel(1000, 'conversion_time')(ts, step = 1), 'max_level') FROM funnel_conv_time WHERE uid = 2;

DROP TABLE funnel_conv_time;

SELECT 'large DateTime';
SELECT windowFunnel(3600, 'conversion_time')(ts, step = 1, step = 2, step = 3)
FROM values('ts DateTime(\'UTC\'), step UInt8', ('2106-02-01 00:00:00', 1), ('2106-02-01 00:10:00', 2), ('2106-02-01 00:25:00', 3));

SELECT 'large UInt64';
SELECT windowFunnel(1000, 'conversion_time')(ts, step = 1, step = 2, step = 3)
FROM values('ts UInt64, step UInt8', (9223372036854775000, 1), (9223372036854775100, 2), (9223372036854775400, 3));

SELECT 'unsupported combinations';
SELECT windowFunnel(10, 'conversion_time', 'strict_deduplication')(ts, step = 1, step = 2)
FROM values('ts UInt64, step UInt8', (1, 1)); -- { serverError BAD_ARGUMENTS }
SELECT windowFunnel(10, 'strict_order', 'allow_reentry', 'conversion_time')(ts, step = 1, step = 2)
FROM values('ts UInt64, step UInt8', (1, 1)); -- { serverError BAD_ARGUMENTS }
