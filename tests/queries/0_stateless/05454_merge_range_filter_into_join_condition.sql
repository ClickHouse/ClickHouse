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
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'tuple', 'hash,ie_join', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash,ie_join', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'tuple', 'hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;

SELECT 'float', 'ie_join,hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'float', 'hash,ie_join', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash,ie_join', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'float', 'hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'tuple', 'ie_join,parallel_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,parallel_hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'float', 'ie_join,parallel_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,parallel_hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'tuple', 'ie_join,grace_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 10485760, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'float', 'ie_join,grace_hash', count(), groupArraySorted(10)((l.id, r.id))
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 10485760, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;

SELECT 'tuple', countIf(explain LIKE '%IEJoin%') = 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'float', countIf(explain LIKE '%IEJoin%') = 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'ordinary', countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.k = r.k AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'tuple-only-ie', countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'float-only-ie', countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'parallel-hash-no-ie', countIf(explain LIKE '%IEJoin%') = 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.t = r.t AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,parallel_hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'grace-hash-no-ie', countIf(explain LIKE '%IEJoin%') = 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 10485760, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'grace-hash-unrunnable-ie', countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT * FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,grace_hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0);
SELECT 'outer-partial-merge-ie', count()
FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,partial_merge', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'outer-auto-ie', count()
FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,auto', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'fsm-one-sided-ie', count()
FROM ie_t_l l ALL INNER JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y AND l.x > 0
SETTINGS join_algorithm = 'ie_join,full_sorting_merge', query_plan_split_filter = 0, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'dynamic-disallowed-ie', count()
FROM (SELECT CAST(1, 'Dynamic') AS d, f, x, y FROM ie_t_l) l ALL INNER JOIN (SELECT CAST(1, 'Dynamic') AS d, f, x, y FROM ie_t_r) r ON l.d = r.d AND l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', allow_dynamic_type_in_join_keys = 0, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'nullsafe-no-supertype-ie', count()
FROM (SELECT toUInt64(id) AS u, f, x, y FROM ie_t_l) l ALL INNER JOIN (SELECT toInt64(id) AS i, f, x, y FROM ie_t_r) r ON isNotDistinctFrom(l.u, r.i) AND l.f = r.f AND l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;

SELECT 'outer', 0, count(), sum(cityHash64(lid, rid))
FROM (SELECT l.id AS lid, r.id AS rid FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y)
SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'outer', 1, count(), sum(cityHash64(lid, rid))
FROM (SELECT l.id AS lid, r.id AS rid FROM ie_t_l l LEFT JOIN ie_t_r r ON l.f = r.f AND l.x < r.x AND l.y > r.y)
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;

CREATE TABLE rf_l (id UInt32, k Int64, x Int64, y Int64, z Int64, u UInt32, n Nullable(Int64), f Float64, dt Date, n8 UInt8, arr Array(Int64), t Tuple(Nullable(Int64))) ENGINE = Memory;
CREATE TABLE rf_r AS rf_l;
CREATE TABLE rf_lj (k Int64, x Int64) ENGINE = Join(ALL, INNER, k);
INSERT INTO rf_l SELECT number, (number + 1) / 2, number - 1, 20 - number, 8 - number, number, if(number <= 2, NULL, number), if(number = 1, nan, number), toDate('2020-01-01') + number, if(number % 2, 0, 2), [1, 2], tuple(if(number <= 2, NULL, number)) FROM numbers(8);
INSERT INTO rf_r SELECT number + 10, (number + 1) / 2, number + 10, if(number <= 2, 1, 20), number, number, if(number <= 2, NULL, number + 10), if(number = 1, nan, number + 10), toDate('2020-01-10') + number, if(number % 2, 0, 2), [1, 2], tuple(if(number <= 2, NULL, number)) FROM numbers(8);
INSERT INTO rf_lj SELECT number, number FROM numbers(1, 4);

SELECT 'R1', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Filter column: %less(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R1', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R1', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R2-lt-eq', countIf(explain LIKE '%Expression: %lessOrEquals(%') > 0, countIf(explain LIKE '%Filter column: %lessOrEquals(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x <= r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R2-gt', countIf(explain LIKE '%Expression: %greater(%') > 0, countIf(explain LIKE '%Filter column: %greater(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.y > l.x SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R2-gt-eq', countIf(explain LIKE '%Expression: %greaterOrEquals(%') > 0, countIf(explain LIKE '%Filter column: %greaterOrEquals(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.y >= l.x SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R2-lt-eq', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x <= r.y) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R2-lt-eq', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x <= r.y) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R2-gt', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.y > l.x) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R2-gt', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.y > l.x) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R2-gt-eq', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.y >= l.x) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R2-gt-eq', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.y >= l.x) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R3', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Expression: %greater(%') > 0, countIf(explain LIKE '%Expression: %greater(%x, 0%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.z > r.z AND l.x > 0 SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R3', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.z > r.z AND l.x > 0) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R3', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.z > r.z AND l.x > 0) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R4-nullable', countIf(explain LIKE '%Expression: %less(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.n < r.n SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R4-float', countIf(explain LIKE '%Expression: %less(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.f < r.f SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R4-mixed', countIf(explain LIKE '%Expression: %less(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.u < r.x SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R4-nullable', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.n < r.n) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R4-nullable', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.n < r.n) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R4-float', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.f < r.f) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R4-float', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.f < r.f) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R4-mixed', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.u < r.x) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R4-mixed', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.u < r.x) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R5', countIf(explain LIKE '%Expression: %greater(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.dt > l.dt + 5 SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R5', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.dt > l.dt + 5) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R5', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE r.dt > l.dt + 5) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R6', countIf(explain LIKE '%Expression: %less(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x + l.z < r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R6', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x + l.z < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R6', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x + l.z < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R6b-constant', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < 5 SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R6b-same-side', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < l.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R6b-constant', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < 5) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R6b-constant', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < 5) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R6b-same-side', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < l.z) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R6b-same-side', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < l.z) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R7', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x + r.y < r.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R7', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x + r.y < r.z) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R7', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x + r.y < r.z) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R8', countIf(explain LIKE '%Expression: %less(%rand%') = 0, countIf(explain LIKE '%Filter column: %<%rand%') > 0, countIf(explain LIKE '%Expression: %greater(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + rand() % 2 AND l.z > r.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R9', countIf(explain LIKE '%Expression: %less(%sleepEachRow%') = 0, countIf(explain LIKE '%Filter column: %<%sleepEachRow%') > 0, countIf(explain LIKE '%Expression: %greater(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + sleepEachRow(0) AND l.z > r.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R9', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + sleepEachRow(0) AND l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R9', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + sleepEachRow(0) AND l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R10', countIf(explain LIKE '%Expression: %less(%arrayJoin%') = 0, countIf(explain LIKE '%Filter column: %<%arrayJoin%') > 0, countIf(explain LIKE '%Expression: %greater(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + arrayJoin(r.arr) AND l.z > r.z SETTINGS query_plan_lower_array_join_function = 0, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R10', 0, count(), sum(cityHash64(lid, rid, larr, rarr)) FROM (SELECT l.id AS lid, r.id AS rid, l.arr AS larr, r.arr AS rarr FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + arrayJoin(r.arr) AND l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R10', 1, count(), sum(cityHash64(lid, rid, larr, rarr)) FROM (SELECT l.id AS lid, r.id AS rid, l.arr AS larr, r.arr AS rarr FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y + arrayJoin(r.arr) AND l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R11', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Type: INNER%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l, rf_r r WHERE l.x < r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R11', countIf(explain ILIKE '%Type: cross%') > 0 FROM (EXPLAIN PLAN actions = 1 SELECT * FROM rf_l l, rf_r r WHERE l.x < r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R11', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l, rf_r r WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R11', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l, rf_r r WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R11b', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Expression: %greater(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l, rf_r r WHERE l.x < r.y AND l.z > r.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R11b', countIf(explain LIKE '%IEJoin%') > 0 FROM (EXPLAIN PLAN actions = 1 SELECT * FROM rf_l l, rf_r r WHERE l.x < r.y AND l.z > r.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R11b', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l, rf_r r WHERE l.x < r.y AND l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R11b', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l, rf_r r WHERE l.x < r.y AND l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R12', countIf(explain LIKE '%Expression: %less(%') = 0, countIf(explain LIKE '%Filter column: %x < y%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y SETTINGS join_algorithm = 'partial_merge', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R12', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS join_algorithm = 'partial_merge', query_plan_merge_filter_into_join_condition = 0;
SELECT 'R12', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS join_algorithm = 'partial_merge', query_plan_merge_filter_into_join_condition = 1;
SELECT 'R12b', countIf(explain LIKE '%Expression: %equals(%') > 0, countIf(explain LIKE '%Filter column: %<%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.u = r.u AND l.x < r.y SETTINGS join_algorithm = 'partial_merge', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R12b', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.u = r.u AND l.x < r.y) SETTINGS join_algorithm = 'partial_merge', query_plan_merge_filter_into_join_condition = 0;
SELECT 'R12b', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.u = r.u AND l.x < r.y) SETTINGS join_algorithm = 'partial_merge', query_plan_merge_filter_into_join_condition = 1;

SELECT 'R13-any', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l ANY JOIN rf_r r ON l.k = r.k WHERE l.x < r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R13-semi', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l SEMI JOIN rf_r r ON l.k = r.k WHERE l.x < r.y SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R13-any', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l ANY JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R13-any', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l ANY JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R13-semi', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l SEMI JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R13-semi', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l SEMI JOIN rf_r r ON l.k = r.k WHERE l.x < r.y) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R13b', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l LEFT JOIN rf_r r ON l.k = r.k WHERE l.x < r.y OR r.y IS NULL SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R13b', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l LEFT JOIN rf_r r ON l.k = r.k WHERE l.x < r.y OR r.y IS NULL) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R13b', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l LEFT JOIN rf_r r ON l.k = r.k WHERE l.x < r.y OR r.y IS NULL) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R13c', countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_lj r ON l.k = r.k WHERE l.x < r.x SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R13c', 0, count(), sum(cityHash64(lid, rx)) FROM (SELECT l.id AS lid, r.x AS rx FROM rf_l l JOIN rf_lj r ON l.k = r.k WHERE l.x < r.x) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R13c', 1, count(), sum(cityHash64(lid, rx)) FROM (SELECT l.id AS lid, r.x AS rx FROM rf_l l JOIN rf_lj r ON l.k = r.k WHERE l.x < r.x) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R14', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Filter column: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT l.id, r.id, (l.x < r.y) AS c FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE c SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R14', 0, count(), sum(cityHash64(lid, rid, c)) FROM (SELECT l.id AS lid, r.id AS rid, (l.x < r.y) AS c FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE c) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R14', 1, count(), sum(cityHash64(lid, rid, c)) FROM (SELECT l.id AS lid, r.id AS rid, (l.x < r.y) AS c FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE c) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R14-values', min(c), max(c), toTypeName(c) FROM (SELECT (l.x < r.y) AS c FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE c) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R14b', countIf(explain LIKE '%Expression: %less(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.n8 SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R14b', 0, count(), sum(cityHash64(lid, rid, n8)) FROM (SELECT l.id AS lid, r.id AS rid, l.n8 AS n8 FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.n8) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R14b', 1, count(), sum(cityHash64(lid, rid, n8)) FROM (SELECT l.id AS lid, r.id AS rid, l.n8 AS n8 FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.n8) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R14b-values', countIf(n8 != 2) = 0 FROM (SELECT l.n8 AS n8 FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.n8) SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT 'R15', countIf(explain LIKE '%Expression: %or(%') = 0, countIf(explain LIKE '%Expression: %less(%') = 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y OR l.z > r.z SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R15', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y OR l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 0;
SELECT 'R15', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y OR l.z > r.z) SETTINGS query_plan_merge_filter_into_join_condition = 1;
SELECT 'R16', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Expression: %greater(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.f = r.f WHERE l.x < r.y AND l.z > r.z SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R16', countIf(explain LIKE '%IEJoin%') = 0 FROM (EXPLAIN PLAN actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.f = r.f WHERE l.x < r.y AND l.z > r.z SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R16', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.f = r.f WHERE l.x < r.y AND l.z > r.z) SETTINGS join_algorithm = 'ie_join,hash', query_plan_merge_filter_into_join_condition = 0;
SELECT 'R16', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.f = r.f WHERE l.x < r.y AND l.z > r.z) SETTINGS join_algorithm = 'ie_join,hash', query_plan_merge_filter_into_join_condition = 1;
SELECT 'R16b', countIf(explain LIKE '%Expression: %less(%') > 0, countIf(explain LIKE '%Expression: %greater(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.t = r.t WHERE l.x < r.y AND l.z > r.z SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R16b', countIf(explain LIKE '%IEJoin%') = 0 FROM (EXPLAIN PLAN actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.t = r.t WHERE l.x < r.y AND l.z > r.z SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R16b', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.t = r.t WHERE l.x < r.y AND l.z > r.z) SETTINGS join_algorithm = 'ie_join,hash', query_plan_merge_filter_into_join_condition = 0;
SELECT 'R16b', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.t = r.t WHERE l.x < r.y AND l.z > r.z) SETTINGS join_algorithm = 'ie_join,hash', query_plan_merge_filter_into_join_condition = 1;
SELECT 'R17', countIf(explain LIKE '%IEJoin%') > 0 FROM (EXPLAIN PLAN actions = 1 SELECT * FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.z > r.z SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
SELECT 'R17', 0, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.z > r.z) SETTINGS join_algorithm = 'ie_join,hash', query_plan_merge_filter_into_join_condition = 0;
SELECT 'R17', 1, count(), sum(cityHash64(lid, rid)) FROM (SELECT l.id AS lid, r.id AS rid FROM rf_l l JOIN rf_r r ON l.k = r.k WHERE l.x < r.y AND l.z > r.z) SETTINGS join_algorithm = 'ie_join,hash', query_plan_merge_filter_into_join_condition = 1;

CREATE TABLE ro_a (id UInt32, k Int64, x Int64, y Int64) ENGINE = Memory;
CREATE TABLE ro_b (id UInt32, ak Int64, ck Int64) ENGINE = Memory;
CREATE TABLE ro_c (id UInt32, k Int64, x Int64, y Int64) ENGINE = Memory;
INSERT INTO ro_a VALUES (1, 1, 10, 100), (2, 2, 20, 200), (3, 3, 30, 300);
INSERT INTO ro_c VALUES (1, 1, 15, 50), (2, 2, 25, 150), (3, 3, 5, 400);
INSERT INTO ro_b SELECT number, number % 3 + 1, (number * 7) % 3 + 1 FROM numbers(10000);

SELECT 'G1-plan', countIf(explain LIKE '%a%⋈%c%' OR explain LIKE '%c%⋈%a%') > 0, countIf(explain ILIKE '%Type: cross%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, use_hash_table_stats_for_join_reordering = 0, query_plan_merge_filter_into_join_condition = 1);
SELECT 'G1-default', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-default', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G2-plan', countIf(explain LIKE '%a%⋈%c%' OR explain LIKE '%c%⋈%a%') > 0, countIf(explain LIKE '%IEJoin%') > 0
FROM (EXPLAIN PLAN actions = 1 SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x AND a.y > c.y SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, use_hash_table_stats_for_join_reordering = 0, query_plan_merge_filter_into_join_condition = 1);
SELECT 'G2-default', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x AND a.y > c.y) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G2-default', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x AND a.y > c.y) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;

SELECT 'G1-hash', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-hash', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G1-sorting', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-sorting', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G2-ie-first', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x AND a.y > c.y) SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G2-ie-first', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x AND a.y > c.y) SETTINGS join_algorithm = 'ie_join,hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;

SELECT 'G1-parallel-plan', countIf(explain LIKE '%Filter column: %<%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x SETTINGS join_algorithm = 'parallel_hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1);
SELECT 'G1-parallel', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'parallel_hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-parallel', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'parallel_hash', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G1-partial-plan', countIf(explain LIKE '%Filter column: %<%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x SETTINGS join_algorithm = 'partial_merge', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1);
SELECT 'G1-partial', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'partial_merge', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-partial', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'partial_merge', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G1-default-legacy-plan', countIf(explain LIKE '%Expression: %less(%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x SETTINGS join_algorithm = 'default', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1);
SELECT 'G1-default-legacy', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'default', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-default-legacy', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'default', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G1-auto-plan', countIf(explain LIKE '%Filter column: %<%') > 0 FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1 SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x SETTINGS join_algorithm = 'auto', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1);
SELECT 'G1-auto', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'auto', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-auto', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'auto', query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;

SELECT 'G1-greedy', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-greedy', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G1-dpsub-greedy', 0, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub,greedy', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT 'G1-dpsub-greedy', 1, count(), sum(cityHash64(a.id, b.id, c.id)) FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join', query_plan_optimize_join_order_algorithm = 'dpsub,greedy', query_plan_optimize_join_order_limit = 3, allow_block_nested_loop_join = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT 'G1-count', count() = 6667 FROM (SELECT a.id, b.id, c.id FROM ro_a a JOIN ro_b b ON a.k = b.ak JOIN ro_c c ON b.ck = c.k WHERE a.x < c.x) SETTINGS query_plan_merge_filter_into_join_condition = 0, query_plan_optimize_join_order_limit = 1;

-- A later range must not run before the conjunct that would have rejected the row.
SELECT 'R18-short-circuit', 0, count()
FROM values('s String, x Int64', ('bad', 0), ('1', 5)) AS l
CROSS JOIN values('y Int64, z Int64', (1, 3), (1, 3)) AS r
WHERE l.x >= r.y AND toInt64(l.s) < r.z
SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT 'R18-short-circuit', 1, count()
FROM values('s String, x Int64', ('bad', 0), ('1', 5)) AS l
CROSS JOIN values('y Int64, z Int64', (1, 3), (1, 3)) AS r
WHERE l.x >= r.y AND toInt64(l.s) < r.z
SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1;
SELECT 'R18-plan', countIf(explain LIKE '%Expression: %toInt64%') = 0 AND countIf(explain LIKE '%Expression: %greaterOrEquals%') > 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
    SELECT count()
    FROM values('s String, x Int64', ('bad', 0), ('1', 5)) AS l
    CROSS JOIN values('y Int64, z Int64', (1, 3), (1, 3)) AS r
    WHERE l.x >= r.y AND toInt64(l.s) < r.z
    SETTINGS query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);

-- A pure range must not remove rows before a stateful conjunct of the same filter.
SELECT 'R19-stateful', 0, x, y, rn
FROM
(
    SELECT l.x AS x, r.y AS y, rowNumberInAllBlocks() AS rn
    FROM values('x UInt64, k UInt8', (0, 1), (1, 1), (2, 1), (3, 1)) AS l
    JOIN values('y UInt64, k UInt8', (0, 1)) AS r ON l.k = r.k
    WHERE l.x > r.y AND rn % 2 = 0
    ORDER BY x
    SETTINGS max_threads = 1, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 0
);
SELECT 'R19-stateful', 1, x, y, rn
FROM
(
    SELECT l.x AS x, r.y AS y, rowNumberInAllBlocks() AS rn
    FROM values('x UInt64, k UInt8', (0, 1), (1, 1), (2, 1), (3, 1)) AS l
    JOIN values('y UInt64, k UInt8', (0, 1)) AS r ON l.k = r.k
    WHERE l.x > r.y AND rn % 2 = 0
    ORDER BY x
    SETTINGS max_threads = 1, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1
);
SELECT 'R19-plan', countIf(explain LIKE '%Expression: %greater(%') = 0
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
    SELECT l.x, r.y, rowNumberInAllBlocks() AS rn
    FROM values('x UInt64, k UInt8', (0, 1), (1, 1), (2, 1), (3, 1)) AS l
    JOIN values('y UInt64, k UInt8', (0, 1)) AS r ON l.k = r.k
    WHERE l.x > r.y AND rn % 2 = 0
    SETTINGS max_threads = 1, query_plan_optimize_join_order_limit = 1, query_plan_merge_filter_into_join_condition = 1);
