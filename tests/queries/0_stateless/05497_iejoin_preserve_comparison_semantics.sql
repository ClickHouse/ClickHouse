SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET join_default_strictness = 'ALL';
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 0;
SET enable_join_runtime_filters = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET join_overflow_mode = 'throw';

CREATE TABLE ie_t_l (id UInt32, t Tuple(Nullable(Int64)), f Float64, k Int64, x Int32, y Int32) ENGINE = Memory;
CREATE TABLE ie_t_r AS ie_t_l;
INSERT INTO ie_t_l VALUES (1, tuple(NULL), nan, 1, 1, 30), (2, tuple(NULL), nan, 1, 1, 30), (3, tuple(3), 3, 3, 1, 30);
INSERT INTO ie_t_r VALUES (11, tuple(NULL), nan, 1, 2, 20), (12, tuple(NULL), nan, 1, 2, 20), (13, tuple(3), 3, 3, 2, 20);

SELECT 'tuple', 'ie_join,hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
SELECT 'tuple', 'hash,ie_join', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash,ie_join', query_plan_optimize_join_order_limit = 1;

SELECT 'float', 'ie_join,hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
SELECT 'float', 'hash,ie_join', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash,ie_join', query_plan_optimize_join_order_limit = 1;
SELECT 'tuple', 'ie_join,parallel_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,parallel_hash', query_plan_optimize_join_order_limit = 1;
SELECT 'float', 'ie_join,parallel_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,parallel_hash', query_plan_optimize_join_order_limit = 1;
SELECT 'tuple', 'ie_join,grace_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 10485760, query_plan_optimize_join_order_limit = 1;
SELECT 'float', 'ie_join,grace_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 10485760, query_plan_optimize_join_order_limit = 1;

SELECT 'ordinary', countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1);
SELECT 'tuple-only-ie-result', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join', query_plan_optimize_join_order_limit = 1;
SELECT 'float-only-ie-result', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join', query_plan_optimize_join_order_limit = 1;
SELECT 'grace-hash-unrunnable-ie', countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_limit = 1);
SELECT 'outer-partial-merge-ie', count()
FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,partial_merge', query_plan_optimize_join_order_limit = 1;
SELECT 'outer-auto-ie', count()
FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,auto', query_plan_optimize_join_order_limit = 1;
SELECT 'outer-prefer-ie', count()
FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,prefer_partial_merge', query_plan_optimize_join_order_limit = 1;
SELECT 'inner-auto-hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,auto', query_plan_optimize_join_order_limit = 1;
SELECT 'inner-prefer-hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,prefer_partial_merge', query_plan_optimize_join_order_limit = 1;
SELECT 'fsm-one-sided-ie', count()
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y AND l.x > 0
SETTINGS join_algorithm = 'ie_join,full_sorting_merge', query_plan_split_filter = 0, query_plan_optimize_join_order_limit = 1;
SELECT 'dynamic-disallowed-ie', count()
FROM (SELECT CAST(1, 'Dynamic') AS d, f, x, y FROM ie_t_l) l ALL INNER JOIN (SELECT CAST(1, 'Dynamic') AS d, f, x, y FROM ie_t_r) r ON l.d = r.d AND l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', allow_dynamic_type_in_join_keys = 0, query_plan_optimize_join_order_limit = 1;
SELECT 'nullsafe-no-supertype-ie', count()
FROM (SELECT toUInt64(id) AS u, f, x, y FROM ie_t_l) l ALL INNER JOIN (SELECT toInt64(id) AS i, f, x, y FROM ie_t_r) r ON isNotDistinctFrom(l.u, r.i) AND l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
SELECT 'outer-ie', count(), sum(cityHash64(lid, rid))
FROM (SELECT l.id AS lid, r.id AS rid FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y)
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
SELECT 'outer-hash', count(), sum(cityHash64(lid, rid))
FROM (SELECT l.id AS lid, r.id AS rid FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y)
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_limit = 1;

SELECT 'string-le-where', count()
FROM values('s String, x Int64', ('a\0', 1)) AS l
CROSS JOIN values('s FixedString(1), x Int64', ('a', 2)) AS r
WHERE l.s <= r.s AND l.x < r.x;
SELECT 'string-le-on', count()
FROM values('s String, x Int64', ('a\0', 1)) AS l
JOIN values('s FixedString(1), x Int64', ('a', 2)) AS r ON l.s <= r.s AND l.x < r.x
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
SELECT 'string-gt-where', count()
FROM values('s String, x Int64', ('a\0', 1)) AS l
CROSS JOIN values('s FixedString(1), x Int64', ('a', 2)) AS r
WHERE l.s > r.s AND l.x < r.x;
SELECT 'string-gt-on', count()
FROM values('s String, x Int64', ('a\0', 1)) AS l
JOIN values('s FixedString(1), x Int64', ('a', 2)) AS r ON l.s > r.s AND l.x < r.x
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
SELECT 'string-key-hash', count()
FROM values('s String, x Int64, y Int64', ('a\0', 1, 3)) AS l
JOIN values('s FixedString(1), x Int64, y Int64', ('a', 2, 2)) AS r ON l.s = r.s AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_limit = 1;
SELECT 'string-key-ie', count()
FROM values('s String, x Int64, y Int64', ('a\0', 1, 3)) AS l
JOIN values('s FixedString(1), x Int64, y Int64', ('a', 2, 2)) AS r ON l.s = r.s AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1;
