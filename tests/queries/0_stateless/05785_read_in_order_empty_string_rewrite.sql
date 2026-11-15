-- Tags: no-parallel-replicas

-- `s = ''` rewritten to `empty(s)` by `optimize_empty_string_comparisons` fixes `s` to a single
-- value, so `ORDER BY` on the next sorting-key column can read in order, including when `s` is in
-- the sorting key outside a shortened `PRIMARY KEY` and `optimize_functions_to_subcolumns` is on.

SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET optimize_empty_string_comparisons = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET enable_parallel_replicas = 0;
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS t_full_str;
DROP TABLE IF EXISTS t_full_lc;
DROP TABLE IF EXISTS t_full_null;
DROP TABLE IF EXISTS t_short_str;
DROP TABLE IF EXISTS t_short_lc;
DROP TABLE IF EXISTS t_short_null;

SELECT '--- full PK: String';
CREATE TABLE t_full_str (s String, id UInt64) ENGINE = MergeTree ORDER BY (s, id);
SYSTEM STOP MERGES t_full_str;
INSERT INTO t_full_str VALUES ('', 10), ('', 30), ('a', 5), ('b', 1);
INSERT INTO t_full_str VALUES ('', 20), ('', 40), ('a', 15), ('c', 2);

SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_full_str WHERE s = '' ORDER BY id ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_full_str WHERE s = '' ORDER BY id DESC LIMIT 2) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_full_str WHERE s = '' ORDER BY id ASC LIMIT 3;
SELECT id FROM t_full_str WHERE s = '' ORDER BY id DESC LIMIT 2;
SELECT id FROM t_full_str WHERE s = '' ORDER BY id ASC LIMIT 3 SETTINGS read_in_order_two_level_merge_threshold = 1;

SELECT '--- full PK: LowCardinality(String)';
CREATE TABLE t_full_lc (s LowCardinality(String), id UInt64) ENGINE = MergeTree ORDER BY (s, id);
SYSTEM STOP MERGES t_full_lc;
INSERT INTO t_full_lc VALUES ('', 10), ('', 30), ('a', 5), ('b', 1);
INSERT INTO t_full_lc VALUES ('', 20), ('', 40), ('a', 15), ('c', 2);

SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_full_lc WHERE s = '' ORDER BY id ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_full_lc WHERE s = '' ORDER BY id ASC LIMIT 3;
SELECT id FROM t_full_lc WHERE s = '' ORDER BY id DESC LIMIT 2;

SELECT '--- full PK: Nullable(String)';
CREATE TABLE t_full_null (s Nullable(String), id UInt64) ENGINE = MergeTree ORDER BY (s, id) SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES t_full_null;
INSERT INTO t_full_null VALUES ('', 10), (NULL, 1), ('', 30), ('a', 5);
INSERT INTO t_full_null VALUES ('', 20), (NULL, 2), ('', 40), ('b', 6);

SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_full_null WHERE s = '' ORDER BY id ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_full_null WHERE s = '' ORDER BY id ASC LIMIT 3;

SELECT '--- shortened PK: String';
CREATE TABLE t_short_str (tenant UInt64, s String, id UInt64) ENGINE = MergeTree ORDER BY (tenant, s, id) PRIMARY KEY tenant;
SYSTEM STOP MERGES t_short_str;
INSERT INTO t_short_str VALUES (1, '', 10), (1, '', 30), (1, 'a', 2), (2, '', 1);
INSERT INTO t_short_str VALUES (1, '', 20), (1, '', 40), (1, 'b', 3), (2, '', 5);

SELECT extract(explain, 'Read type: [A-Za-z]+') AS e FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 1 SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id LIMIT 2) WHERE e != '';
SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id DESC LIMIT 2) WHERE explain LIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_short_str PREWHERE s = '' WHERE tenant = 1 ORDER BY id ASC LIMIT 2) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3;
SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id DESC LIMIT 2;
SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3 SETTINGS read_in_order_two_level_merge_threshold = 1;

SELECT '--- shortened PK: LowCardinality(String)';
CREATE TABLE t_short_lc (tenant UInt64, s LowCardinality(String), id UInt64) ENGINE = MergeTree ORDER BY (tenant, s, id) PRIMARY KEY tenant;
SYSTEM STOP MERGES t_short_lc;
INSERT INTO t_short_lc VALUES (1, '', 10), (1, '', 30), (1, 'a', 2), (2, '', 1);
INSERT INTO t_short_lc VALUES (1, '', 20), (1, '', 40), (1, 'b', 3), (2, '', 5);

SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_short_lc WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_short_lc WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3;
SELECT id FROM t_short_lc WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3 SETTINGS read_in_order_two_level_merge_threshold = 1;

SELECT '--- shortened PK: Nullable(String)';
CREATE TABLE t_short_null (tenant UInt64, s Nullable(String), id UInt64) ENGINE = MergeTree ORDER BY (tenant, s, id) PRIMARY KEY tenant SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES t_short_null;
INSERT INTO t_short_null VALUES (1, '', 10), (1, NULL, 1), (1, '', 30), (1, 'a', 2);
INSERT INTO t_short_null VALUES (1, '', 20), (1, NULL, 3), (1, '', 40), (2, '', 4);

SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_short_null WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_short_null WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3;
SELECT id FROM t_short_null WHERE tenant = 1 AND s = '' ORDER BY id ASC LIMIT 3 SETTINGS read_in_order_two_level_merge_threshold = 1;

SELECT '--- controls';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT id FROM t_short_str WHERE tenant = 1 AND s = '' ORDER BY id LIMIT 2) WHERE explain LIKE '%s.size%';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT countIf(s = '') FROM t_short_str ORDER BY length(s)) WHERE explain LIKE '%s.size%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT sum(length(s)) FROM t_short_str WHERE tenant = 1 AND s != '') WHERE explain LIKE '%s.size%';
SELECT sum(length(s)) FROM t_short_str WHERE tenant = 1 AND s != '';
SELECT trim(explain) FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0 SELECT id FROM t_short_str WHERE tenant = 1 AND s != '' ORDER BY id ASC LIMIT 2) WHERE explain LIKE '%ReadType%';
SELECT id FROM t_short_str WHERE tenant = 1 AND s != '' ORDER BY id ASC LIMIT 2;

DROP TABLE t_full_str;
DROP TABLE t_full_lc;
DROP TABLE t_full_null;
DROP TABLE t_short_str;
DROP TABLE t_short_lc;
DROP TABLE t_short_null;
