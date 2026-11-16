-- an OR over the elements of several arrays gives each ARRAY JOIN the part of it that the step can compute

-- a serialized plan does not fuse a filter into ARRAY JOIN
SET serialize_query_plan = 0;
SET query_plan_merge_filters = 1;
SET query_plan_fuse_filter_into_array_join = 1;

DROP TABLE IF EXISTS t_aj_or;
CREATE TABLE t_aj_or (id UInt64, A Array(String), B Array(String), C Array(String)) ENGINE = MergeTree ORDER BY id;

-- the arrays have 0 to 4, 0 to 3 and 0 to 2 elements
INSERT INTO t_aj_or SELECT number,
    arrayMap(x -> concat('a', toString(x)), range(number % 5)),
    arrayMap(x -> concat('b', toString(x)), range(number % 4)),
    arrayMap(x -> concat('c', toString(x)), range(number % 3))
FROM numbers(60);

SELECT '-- function form: an element filter in each step';
SELECT count() FROM (EXPLAIN SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c FROM t_aj_or
    WHERE (a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND b = 'b1' AND c = 'c1'))
WHERE explain LIKE '%Element filter column%';

SELECT '-- clause form';
SELECT count() FROM (EXPLAIN SELECT a, b, c FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN B AS b ARRAY JOIN C AS c
    WHERE (a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND b = 'b1' AND c = 'c1'))
WHERE explain LIKE '%Element filter column%';

SELECT '-- each step gets the filter one time, and no filter stays above';
SELECT count() FROM (EXPLAIN SELECT a, b, c FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN B AS b ARRAY JOIN C AS c
    WHERE (a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND b = 'b1' AND c = 'c1'))
WHERE explain LIKE '%Filter%';

SELECT '-- two arrays';
SELECT count() FROM (EXPLAIN SELECT a, c FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN C AS c
    WHERE (a = 'a1' AND c = 'c0') OR (a = 'a3' AND c = 'c1'))
WHERE explain LIKE '%Element filter column%';

SELECT '-- a branch with no condition on the first array gives nothing to the first step';
SELECT count() FROM (EXPLAIN SELECT a, c FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN C AS c
    WHERE (a = 'a1' AND c = 'c0') OR c = 'c1')
WHERE explain LIKE '%Element filter column%';

SELECT '-- a condition that can throw stays above';
-- C is empty when id % 3 = 0, and without the lift up intDiv runs only above the ARRAY JOIN of C
SELECT count() FROM (EXPLAIN SELECT a, c FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN C AS c
    WHERE (intDiv(6, id % 3) > 0 AND a = 'a1' AND c = 'c0') OR (a = 'a3' AND c = 'c1') SETTINGS query_plan_lift_up_array_join = 0)
WHERE explain LIKE '%Element filter column%' AND explain NOT LIKE '%intDiv%';
SELECT a, c, count() FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN C AS c
    WHERE (intDiv(6, id % 3) > 0 AND a = 'a1' AND c = 'c0') OR (a = 'a3' AND c = 'c1')
GROUP BY a, c ORDER BY a, c SETTINGS query_plan_lift_up_array_join = 0;

SELECT '-- same rows as without the filter push down';
SELECT
    (SELECT groupArray((a, b, c, n)) FROM (SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c, count() AS n FROM t_aj_or
        WHERE (a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND b = 'b1' AND c = 'c1') GROUP BY a, b, c ORDER BY a, b, c))
    = (SELECT groupArray((a, b, c, n)) FROM (SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c, count() AS n FROM t_aj_or
        WHERE (a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND b = 'b1' AND c = 'c1') GROUP BY a, b, c ORDER BY a, b, c SETTINGS query_plan_filter_push_down = 0));
SELECT
    (SELECT groupArray((a, b, c, n)) FROM (SELECT a, b, c, count() AS n FROM t_aj_or LEFT ARRAY JOIN A AS a LEFT ARRAY JOIN B AS b LEFT ARRAY JOIN C AS c
        WHERE (a = 'a1' AND b = 'b2' AND c = '') OR (a = 'a3' AND b = '' AND c = 'c1') OR (a = '' AND b = 'b1') GROUP BY a, b, c ORDER BY a, b, c))
    = (SELECT groupArray((a, b, c, n)) FROM (SELECT a, b, c, count() AS n FROM t_aj_or LEFT ARRAY JOIN A AS a LEFT ARRAY JOIN B AS b LEFT ARRAY JOIN C AS c
        WHERE (a = 'a1' AND b = 'b2' AND c = '') OR (a = 'a3' AND b = '' AND c = 'c1') OR (a = '' AND b = 'b1') GROUP BY a, b, c ORDER BY a, b, c SETTINGS query_plan_filter_push_down = 0));
SELECT
    (SELECT groupArray((a, b, c, n)) FROM (SELECT a, b, c, count() AS n FROM t_aj_or ARRAY JOIN A AS a LEFT ARRAY JOIN B AS b ARRAY JOIN C AS c
        WHERE id < 50 AND ((a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND (b = '' OR b = 'b1') AND c = 'c1')) GROUP BY a, b, c ORDER BY a, b, c))
    = (SELECT groupArray((a, b, c, n)) FROM (SELECT a, b, c, count() AS n FROM t_aj_or ARRAY JOIN A AS a LEFT ARRAY JOIN B AS b ARRAY JOIN C AS c
        WHERE id < 50 AND ((a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND (b = '' OR b = 'b1') AND c = 'c1')) GROUP BY a, b, c ORDER BY a, b, c SETTINGS query_plan_filter_push_down = 0));
SELECT a, b, c, count() FROM t_aj_or ARRAY JOIN A AS a ARRAY JOIN B AS b ARRAY JOIN C AS c
    WHERE (a = 'a1' AND b = 'b2' AND c = 'c0') OR (a = 'a3' AND b = 'b1' AND c = 'c1')
GROUP BY a, b, c ORDER BY a, b, c;

DROP TABLE t_aj_or;
