-- A filter conjunct larger than `max_expanded_ast_elements` written out as a tree stays above the JOIN, the other conjuncts are still pushed.
-- Each level doubles `a`, so `a12` is 8191 nodes written out.

SET enable_join_runtime_filters = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;
SET query_plan_filter_push_down = 1, query_plan_merge_expressions = 1, query_plan_convert_join_to_in = 0, use_join_disjunctions_push_down = 1;
SET read_overflow_mode = 'throw';

-- `id < 10` still reaches both reads: 20 rows read instead of 1010.
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 1000, max_rows_to_read = 100;

WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 500000, max_rows_to_read = 100;

-- Where the `a12 <= 12288` conjunct (the only `<=`) is printed: a Join step exists, above it, below it.
SELECT join_line > 0,
    arrayExists((l, i) -> l LIKE '%<=%' AND i < join_line, lines, arrayEnumerate(lines)),
    arrayExists((l, i) -> l LIKE '%<=%' AND i > join_line, lines, arrayEnumerate(lines))
FROM
(
    SELECT lines, arrayFirstIndex(x -> x LIKE 'Join (%', lines) AS join_line
    FROM (SELECT groupArray(replaceRegexpOne(explain, '^[│├└─ ]+', '')) AS lines FROM (EXPLAIN actions = 1
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 1000))
);

SELECT join_line > 0,
    arrayExists((l, i) -> l LIKE '%<=%' AND i < join_line, lines, arrayEnumerate(lines)),
    arrayExists((l, i) -> l LIKE '%<=%' AND i > join_line, lines, arrayEnumerate(lines))
FROM
(
    SELECT lines, arrayFirstIndex(x -> x LIKE 'Join (%', lines) AS join_line
    FROM (SELECT groupArray(replaceRegexpOne(explain, '^[│├└─ ]+', '')) AS lines FROM (EXPLAIN actions = 1
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 500000))
);

-- The same with a MergeTree table below the JOIN.
DROP TABLE IF EXISTS t_mt;
CREATE TABLE t_mt (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mt SELECT number, number FROM numbers(1000);

WITH
    j AS (SELECT r.id AS id, r.val AS val FROM t_mt AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 1000;

SELECT join_line > 0,
    arrayExists((l, i) -> l LIKE '%<=%' AND i < join_line, lines, arrayEnumerate(lines)),
    arrayExists((l, i) -> l LIKE '%<=%' AND i > join_line, lines, arrayEnumerate(lines))
FROM
(
    SELECT lines, arrayFirstIndex(x -> x LIKE 'Join (%', lines) AS join_line
    FROM (SELECT groupArray(replaceRegexpOne(explain, '^[│├└─ ]+', '')) AS lines FROM (EXPLAIN actions = 1
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM t_mt AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 1000, enable_parallel_replicas = 0))
);

SELECT join_line > 0,
    arrayExists((l, i) -> l LIKE '%<=%' AND i < join_line, lines, arrayEnumerate(lines)),
    arrayExists((l, i) -> l LIKE '%<=%' AND i > join_line, lines, arrayEnumerate(lines))
FROM
(
    SELECT lines, arrayFirstIndex(x -> x LIKE 'Join (%', lines) AS join_line
    FROM (SELECT groupArray(replaceRegexpOne(explain, '^[│├└─ ]+', '')) AS lines FROM (EXPLAIN actions = 1
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM t_mt AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE id < 10 AND a12 <= 12288
SETTINGS max_expanded_ast_elements = 500000, enable_parallel_replicas = 0))
);

-- An implied indexHint holds the composed condition in its own actions, so it counts with them.
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE a12 >= id AND id >= 5
SETTINGS max_expanded_ast_elements = 1000, optimize_and_compare_chain = 1;

SELECT join_line > 0,
    arrayExists((l, i) -> l LIKE '%indexHint%' AND i < join_line, lines, arrayEnumerate(lines)),
    arrayExists((l, i) -> l LIKE '%indexHint%' AND i > join_line, lines, arrayEnumerate(lines))
FROM
(
    SELECT lines, arrayFirstIndex(x -> x LIKE 'Join (%', lines) AS join_line
    FROM (SELECT groupArray(replaceRegexpOne(explain, '^[│├└─ ]+', '')) AS lines FROM (EXPLAIN actions = 1
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE a12 >= id AND id >= 5
SETTINGS max_expanded_ast_elements = 1000, optimize_and_compare_chain = 1))
);

SELECT join_line > 0,
    arrayExists((l, i) -> l LIKE '%indexHint%' AND i < join_line, lines, arrayEnumerate(lines)),
    arrayExists((l, i) -> l LIKE '%indexHint%' AND i > join_line, lines, arrayEnumerate(lines))
FROM
(
    SELECT lines, arrayFirstIndex(x -> x LIKE 'Join (%', lines) AS join_line
    FROM (SELECT groupArray(replaceRegexpOne(explain, '^[│├└─ ]+', '')) AS lines FROM (EXPLAIN actions = 1
WITH
    j AS (SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id),
    t1 AS (SELECT id, val + val AS a1 FROM j),
    t2 AS (SELECT id, a1 + a1 AS a2 FROM t1),
    t3 AS (SELECT id, a2 + a2 AS a3 FROM t2),
    t4 AS (SELECT id, a3 + a3 AS a4 FROM t3),
    t5 AS (SELECT id, a4 + a4 AS a5 FROM t4),
    t6 AS (SELECT id, a5 + a5 AS a6 FROM t5),
    t7 AS (SELECT id, a6 + a6 AS a7 FROM t6),
    t8 AS (SELECT id, a7 + a7 AS a8 FROM t7),
    t9 AS (SELECT id, a8 + a8 AS a9 FROM t8),
    t10 AS (SELECT id, a9 + a9 AS a10 FROM t9),
    t11 AS (SELECT id, a10 + a10 AS a11 FROM t10),
    t12 AS (SELECT id, a11 + a11 AS a12 FROM t11)
SELECT count() FROM t12 WHERE a12 >= id AND id >= 5
SETTINGS max_expanded_ast_elements = 500000, optimize_and_compare_chain = 1))
);

-- `system.numbers` ends only by a pushed bound, so the conjunct is pushed below this JOIN even when too big.
WITH
    j AS (SELECT n.number AS number, n.number < 4 AS c0 FROM system.numbers AS n INNER JOIN (SELECT number AS id FROM numbers(10)) AS t ON n.number = t.id),
    t1 AS (SELECT number, c0 OR c0 AS c1 FROM j),
    t2 AS (SELECT number, c1 OR c1 AS c2 FROM t1),
    t3 AS (SELECT number, c2 OR c2 AS c3 FROM t2),
    t4 AS (SELECT number, c3 OR c3 AS c4 FROM t3),
    t5 AS (SELECT number, c4 OR c4 AS c5 FROM t4),
    t6 AS (SELECT number, c5 OR c5 AS c6 FROM t5),
    t7 AS (SELECT number, c6 OR c6 AS c7 FROM t6),
    t8 AS (SELECT number, c7 OR c7 AS c8 FROM t7),
    t9 AS (SELECT number, c8 OR c8 AS c9 FROM t8),
    t10 AS (SELECT number, c9 OR c9 AS c10 FROM t9),
    t11 AS (SELECT number, c10 OR c10 AS c11 FROM t10),
    t12 AS (SELECT number, c11 OR c11 AS c12 FROM t11)
SELECT count() FROM t12 WHERE c12
SETTINGS max_expanded_ast_elements = 1000, max_rows_to_read = 1000;

DROP TABLE t_mt;
