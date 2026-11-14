-- Tags: no-parallel-replicas
-- The reads of subcolumns of columns returned by subqueries are pushed down to ReadFromMergeTree:
-- the first line of each case lists the columns read (except `id`), the next two the result with the optimization and without it.

SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1, query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 1;
SET serialize_query_plan = 0, query_plan_optimize_lazy_materialization = 0, optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1,
    query_plan_merge_filters = 1, query_plan_optimize_join_order_randomize = 0;
SET allow_suspicious_low_cardinality_types = 1, allow_experimental_variant_type = 1, allow_experimental_dynamic_type = 1, enable_json_type = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS tj;
DROP TABLE IF EXISTS tr;
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tn;
DROP TABLE IF EXISTS tk;
DROP TABLE IF EXISTS tm;
DROP TABLE IF EXISTS tl;
DROP TABLE IF EXISTS tf;
DROP TABLE IF EXISTS tmb;
DROP TABLE IF EXISTS tq;
DROP TABLE IF EXISTS tw;
DROP VIEW IF EXISTS v;

CREATE TABLE t (id UInt64, data JSON, tup Tuple(a UInt64, b String), n Nullable(String), s String, arr Array(UInt32), m Map(String, UInt64),
    lc LowCardinality(Nullable(String)), v Variant(UInt64, String), d Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t SELECT number,
    if(number % 4 = 0, '{"a" : {"Int64" : 7}}', toJSONString(map('a', toString(number), 'b', 'x'))),
    (number, toString(number % 5)),
    if(number % 3 = 0, NULL, repeat('x', number % 4)),
    repeat('y', number % 3),
    range(number % 3),
    if(number % 2 = 0, map('k1', 1, 'k2', number), map()),
    if(number % 5 = 0, NULL, toString(number)),
    if(number % 2 = 0, number::Variant(UInt64, String), ('s' || toString(number))::Variant(UInt64, String)),
    if(number % 2 = 0, number::Dynamic, toString(number)::Dynamic)
FROM numbers(12);

CREATE TABLE t2 (id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t2 SELECT number * 2, (number * 100, 'w') FROM numbers(8);

CREATE TABLE tj (id UInt64, j JSON(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tj SELECT number, '{"a" : ' || toString(number) || ', "b" : ' || toString(number * 10) || ', "c" : {"x" : ' || toString(number) || '}}' FROM numbers(4);

CREATE VIEW v AS SELECT * FROM t;

CREATE TABLE tr (id UInt64, tup Tuple(a UInt64, b String), n Nullable(String)) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tr SELECT number % 4, (number, 'r'), if(number % 2 = 0, NULL, 'x') FROM numbers(8);

CREATE TABLE ts (id UInt64, tup Tuple(a UInt64, b UInt64)) ENGINE = SummingMergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO ts SELECT number % 3, (number, 1) FROM numbers(9);

CREATE TABLE ta (id UInt64, tup Tuple(a UInt64, b String), tup2 Tuple(a UInt64, b String) ALIAS tup) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO ta SELECT number, (number, 'a') FROM numbers(4);

CREATE TABLE tn (id UInt64, tup Tuple(a Tuple(b UInt64, c String), d String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tn SELECT number, ((number * 10, 'c'), 'd') FROM numbers(6);

CREATE TABLE tk (id UInt64, s String) ENGINE = MergeTree ORDER BY length(s) SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tk SELECT number, repeat('z', number % 5) FROM numbers(10);

CREATE TABLE tm (id UInt64, m Map(String, UInt64), INDEX ik mapKeys(m) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 2;
INSERT INTO tm SELECT number, if(number < 4, map('k1', 1), map('k2', 2)) FROM numbers(10);

CREATE TABLE tl (id UInt64, lcm Map(LowCardinality(String), UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tl SELECT number, map('k1', number) FROM numbers(3);

CREATE TABLE tf (id UInt64, m2 Map(FixedString(4), UInt64), m3 Map(LowCardinality(FixedString(4)), UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO tf SELECT number, map(toFixedString('ab', 4), number), map(toFixedString('ab', 4), number) FROM numbers(3);

CREATE TABLE tmb (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO tmb SELECT number, map('k1', number) FROM numbers(3);

SELECT 'P1 JSON path';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT data.a AS r FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT data.a AS r FROM (SELECT * FROM t) ORDER BY toString(r)) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT data.a AS r FROM (SELECT * FROM t) ORDER BY toString(r)) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P2 tuple elements';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a, tup.b FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a, tup.b FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a, tup.b FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P3 null map';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT n.null FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT n.null FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT n.null FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P4 subcolumns of every kind';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT arr.size0, m.keys, m.values, v.String, d.UInt64 FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT arr.size0, m.keys, m.values, v.String, d.UInt64 FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT arr.size0, m.keys, m.values, v.String, d.UInt64 FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P4 JSON typed path, type hint, sub-object';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT j.a, j.b.:Int64, toString(j.^c) FROM (SELECT * FROM tj)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT j.a, j.b.:Int64, toString(j.^c) FROM (SELECT * FROM tj) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT j.a, j.b.:Int64, toString(j.^c) FROM (SELECT * FROM tj) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P5 view';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM v) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM v ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM v ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P6 CTE';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN WITH c AS (SELECT * FROM t) SELECT tup.a FROM c) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (WITH c AS (SELECT * FROM t) SELECT tup.a FROM c ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (WITH c AS (SELECT * FROM t) SELECT tup.a FROM c ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P7 nested subqueries';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM (SELECT * FROM t))) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM (SELECT * FROM t)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM (SELECT * FROM t)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P8 WHERE in the subquery';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t WHERE id > 5)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t WHERE id > 5) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t WHERE id > 5) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P9 outer WHERE on another subcolumn';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t) WHERE tup.b != '3') WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t) WHERE tup.b != '3' ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t) WHERE tup.b != '3' ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P10 ORDER BY in the subquery';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t ORDER BY id)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY id) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY id) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P11 ORDER BY LIMIT in the subquery';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t ORDER BY id DESC LIMIT 10)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY id DESC LIMIT 10) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY id DESC LIMIT 10) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P12 LIMIT OFFSET in the subquery';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P13 UNION ALL';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT id, tup FROM t UNION ALL SELECT id, tup FROM t2)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT id, tup FROM t UNION ALL SELECT id, tup FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT id, tup FROM t UNION ALL SELECT id, tup FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P14 INNER JOIN, subquery on the left';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a, t2.id FROM (SELECT * FROM t) AS s INNER JOIN t2 ON s.id = t2.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a, t2.id FROM (SELECT * FROM t) AS s INNER JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT s.tup.a, t2.id FROM (SELECT * FROM t) AS s INNER JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P15 LEFT JOIN, subquery on the preserved left side';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a, s.n.null, t2.tup.b FROM (SELECT * FROM t) AS s LEFT JOIN t2 ON s.id = t2.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a, s.n.null, t2.tup.b FROM (SELECT * FROM t) AS s LEFT JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT s.tup.a, s.n.null, t2.tup.b FROM (SELECT * FROM t) AS s LEFT JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P16 RIGHT JOIN, subquery on the preserved right side';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a FROM t2 RIGHT JOIN (SELECT * FROM t) AS s ON s.id = t2.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a FROM t2 RIGHT JOIN (SELECT * FROM t) AS s ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT s.tup.a FROM t2 RIGHT JOIN (SELECT * FROM t) AS s ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P16 RIGHT JOIN with join_use_nulls';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a, isNull(s.n) FROM t2 RIGHT JOIN (SELECT * FROM t) AS s ON s.id = t2.id SETTINGS join_use_nulls = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a, isNull(s.n) FROM t2 RIGHT JOIN (SELECT * FROM t) AS s ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS join_use_nulls = 1;
SELECT groupArray(r) FROM (SELECT s.tup.a, isNull(s.n) FROM t2 RIGHT JOIN (SELECT * FROM t) AS s ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0, join_use_nulls = 1;

SELECT 'P16 subqueries on both sides';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT a.tup.a, b.tup.b FROM (SELECT * FROM t) AS a INNER JOIN (SELECT * FROM t2) AS b ON a.id = b.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT a.tup.a, b.tup.b FROM (SELECT * FROM t) AS a INNER JOIN (SELECT * FROM t2) AS b ON a.id = b.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT a.tup.a, b.tup.b FROM (SELECT * FROM t) AS a INNER JOIN (SELECT * FROM t2) AS b ON a.id = b.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P16 LEFT ANTI JOIN';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a FROM (SELECT * FROM t) AS s LEFT ANTI JOIN t2 ON s.id = t2.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a FROM (SELECT * FROM t) AS s LEFT ANTI JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT s.tup.a FROM (SELECT * FROM t) AS s LEFT ANTI JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P17 FINAL of ReplacingMergeTree';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM tr FINAL)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tr FINAL) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tr FINAL) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P18 PREWHERE in the subquery';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t PREWHERE id > 5)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t PREWHERE id > 5) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t PREWHERE id > 5) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P19 ALIAS column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup2.a FROM (SELECT tup2 FROM ta)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup2.a FROM (SELECT tup2 FROM ta) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup2.a FROM (SELECT tup2 FROM ta) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P22 tupleElement';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tupleElement(tup, 'a'), tupleElement(tup, 2) FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tupleElement(tup, 'a'), tupleElement(tup, 2) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tupleElement(tup, 'a'), tupleElement(tup, 2) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P23 variantElement';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT variantElement(v, 'String') FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT variantElement(v, 'String') FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT variantElement(v, 'String') FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

CREATE TABLE tq (id UInt64, vec QBit(Float32, 8)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tq SELECT number, arrayMap(i -> toFloat32(if((number + i) % 2 = 0, -1, 1) * (number * 10 + i + 0.5)), range(8)) FROM numbers(6);

SELECT 'P40 tupleElement of QBit';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT hex(tupleElement(vec, 1)), hex(vec.9) FROM (SELECT * FROM tq)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT hex(tupleElement(vec, 1)), hex(vec.9) FROM (SELECT * FROM tq) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT hex(tupleElement(vec, 1)), hex(vec.9) FROM (SELECT * FROM tq) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P24 mapKeys, mapValues';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT mapKeys(m), mapValues(m) FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT mapKeys(m), mapValues(m) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT mapKeys(m), mapValues(m) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P25 length, empty, notEmpty of Array and Map';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(arr), empty(arr), notEmpty(m) FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(arr), empty(arr), notEmpty(m) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT length(arr), empty(arr), notEmpty(m) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P26 length, empty, notEmpty of String';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(s), empty(s), notEmpty(n) FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(s), empty(s), notEmpty(n) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT length(s), empty(s), notEmpty(n) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P27 isNull, isNotNull';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT isNull(n), isNotNull(n) FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT isNull(n), isNotNull(n) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT isNull(n), isNotNull(n) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P28 map key and value lookups';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT mapContainsKey(m, 'k1'), has(m, 'k1'), notHas(m, 'zz'), mapContainsValue(m, 1) FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT mapContainsKey(m, 'k1'), has(m, 'k1'), notHas(m, 'zz'), mapContainsValue(m, 1) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT mapContainsKey(m, 'k1'), has(m, 'k1'), notHas(m, 'zz'), mapContainsValue(m, 1) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P29 map element';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT m['k1'], m['zz'] FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT m['k1'], m['zz'] FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT m['k1'], m['zz'] FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P30 outer WHERE isNotNull';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t) WHERE isNotNull(n)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t) WHERE isNotNull(n) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t) WHERE isNotNull(n) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P32 alias-equivalent exports across ORDER BY';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT x.a, tup.b FROM (SELECT tup AS x, tup, id FROM t ORDER BY id)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT x.a, tup.b FROM (SELECT tup AS x, tup, id FROM t ORDER BY id) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT x.a, tup.b FROM (SELECT tup AS x, tup, id FROM t ORDER BY id) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P33 composed subcolumns';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT * FROM tn))) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT * FROM tn)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT x.b FROM (SELECT tup.a AS x FROM (SELECT * FROM tn)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P33 composed subcolumns across WHERE and ORDER BY';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT x.b FROM (SELECT tup.a AS x, id FROM (SELECT * FROM tn WHERE id > 2) ORDER BY id)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT x.b FROM (SELECT tup.a AS x, id FROM (SELECT * FROM tn WHERE id > 2) ORDER BY id) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT x.b FROM (SELECT tup.a AS x, id FROM (SELECT * FROM tn WHERE id > 2) ORDER BY id) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P34 map element of an indexed map keeps the map';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT id FROM (SELECT * FROM tm) WHERE m['k1'] = 1) WHERE explain LIKE '% Output:%';
SELECT replaceRegexpOne(explain, '^.*Prewhere filter column: *', '') FROM (EXPLAIN SELECT id FROM (SELECT * FROM tm) WHERE m['k1'] = 1) WHERE explain LIKE '%Prewhere filter column:%';
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM tm) WHERE m['k1'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM tm) WHERE m['k1'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N1 whole column also read';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a, tup FROM (SELECT * FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a, tup FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a, tup FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N2 GROUP BY the column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT tup FROM t GROUP BY tup)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT tup FROM t GROUP BY tup) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT tup FROM t GROUP BY tup) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N3 DISTINCT';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT DISTINCT tup FROM t)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT DISTINCT tup FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT DISTINCT tup FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N4 LEFT JOIN, subquery on the padded side';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT t2.id, s.n.null FROM t2 LEFT JOIN (SELECT * FROM t) AS s ON s.id = t2.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT t2.id, s.n.null FROM t2 LEFT JOIN (SELECT * FROM t) AS s ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT t2.id, s.n.null FROM t2 LEFT JOIN (SELECT * FROM t) AS s ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N5 FULL JOIN';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a FROM (SELECT * FROM t) AS s FULL JOIN t2 ON s.id = t2.id) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a FROM (SELECT * FROM t) AS s FULL JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT s.tup.a FROM (SELECT * FROM t) AS s FULL JOIN t2 ON s.id = t2.id ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N33 WHERE on the subcolumn, pushed down below a join';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT s.tup.a, s.n.null FROM (SELECT * FROM t) AS s LEFT JOIN t2 ON s.id = t2.id WHERE s.n.null) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT s.tup.a, s.n.null FROM (SELECT * FROM t) AS s LEFT JOIN t2 ON s.id = t2.id WHERE s.n.null ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT s.tup.a, s.n.null FROM (SELECT * FROM t) AS s LEFT JOIN t2 ON s.id = t2.id WHERE s.n.null ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N34 PREWHERE that reads the subcolumn';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM (SELECT * FROM t PREWHERE tup.a < 3))) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM (SELECT * FROM t PREWHERE tup.a < 3)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM (SELECT * FROM t PREWHERE tup.a < 3)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N35 row policy that reads the subcolumn';
CREATE ROW POLICY p ON t2 USING tup.a < 300 TO ALL;
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t2)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;
DROP ROW POLICY p ON t2;

SELECT 'N6 ORDER BY the column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t ORDER BY tup)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY tup) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t ORDER BY tup) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N7 WITH FILL';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT id, tup.a FROM (SELECT * FROM t ORDER BY id WITH FILL FROM 0 TO 25)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT id, tup.a FROM (SELECT * FROM t ORDER BY id WITH FILL FROM 0 TO 25) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id, tup.a FROM (SELECT * FROM t ORDER BY id WITH FILL FROM 0 TO 25) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N8 FINAL of SummingMergeTree';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM ts FINAL)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM ts FINAL) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM ts FINAL) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N10 WHERE on the whole column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t WHERE cityHash64(tup) % 2 = 0)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t WHERE cityHash64(tup) % 2 = 0) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM t WHERE cityHash64(tup) % 2 = 0) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N16 function forms off';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(arr) FROM (SELECT * FROM t) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(arr) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_functions_to_subcolumns = 0;
SELECT groupArray(r) FROM (SELECT length(arr) FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0, optimize_functions_to_subcolumns = 0;

SELECT 'P35 plain subcolumn with function forms off';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT arr.size0 FROM (SELECT * FROM t) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT arr.size0 FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_functions_to_subcolumns = 0;
SELECT groupArray(r) FROM (SELECT arr.size0 FROM (SELECT * FROM t) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0, optimize_functions_to_subcolumns = 0;

SELECT 'N17 function form under FINAL';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT isNull(n) FROM (SELECT * FROM tr FINAL)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT isNull(n) FROM (SELECT * FROM tr FINAL) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT isNull(n) FROM (SELECT * FROM tr FINAL) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N18 has of a LowCardinality-keyed map';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT has(lcm, 'k1') FROM (SELECT * FROM tl)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT has(lcm, 'k1') FROM (SELECT * FROM tl) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT has(lcm, 'k1') FROM (SELECT * FROM tl) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N19 map element of a FixedString-keyed map';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT m2['ab'] FROM (SELECT * FROM tf)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT m2['ab'] FROM (SELECT * FROM tf) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT m2['ab'] FROM (SELECT * FROM tf) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N19b map element of a LowCardinality(FixedString)-keyed map';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT m3['ab'] FROM (SELECT * FROM tf)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT m3['ab'] FROM (SELECT * FROM tf) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT m3['ab'] FROM (SELECT * FROM tf) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P41 map element of a LowCardinality(FixedString)-keyed map with a full-width key';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT m3[toFixedString('ab', 4)] FROM (SELECT * FROM tf)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT m3[toFixedString('ab', 4)] FROM (SELECT * FROM tf) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT m3[toFixedString('ab', 4)] FROM (SELECT * FROM tf) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N32 map element of a Map without buckets';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT m['k1'] FROM (SELECT * FROM tmb)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT m['k1'] FROM (SELECT * FROM tmb) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT m['k1'] FROM (SELECT * FROM tmb) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N22 a JSON path named like the composed path';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT toString(getSubcolumn(x, 'Int64')) FROM (SELECT data.a AS x FROM (SELECT * FROM t))) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT toString(getSubcolumn(x, 'Int64')) FROM (SELECT data.a AS x FROM (SELECT * FROM t)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT toString(getSubcolumn(x, 'Int64')) FROM (SELECT data.a AS x FROM (SELECT * FROM t)) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N23 function form over a primary key column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT id FROM (SELECT * FROM tk) WHERE length(s) = 3) WHERE explain LIKE '% Output:%';
SELECT replaceRegexpOne(explain, '^.*Prewhere filter column: *', '') FROM (EXPLAIN SELECT id FROM (SELECT * FROM tk) WHERE length(s) = 3) WHERE explain LIKE '%Prewhere filter column:%';
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM tk) WHERE length(s) = 3 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM tk) WHERE length(s) = 3 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N24 function form over a skip index column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT id FROM (SELECT * FROM tm) WHERE has(mapKeys(m), 'k1')) WHERE explain LIKE '% Output:%';
SELECT replaceRegexpOne(explain, '^.*Prewhere filter column: *', '') FROM (EXPLAIN SELECT id FROM (SELECT * FROM tm) WHERE has(mapKeys(m), 'k1')) WHERE explain LIKE '%Prewhere filter column:%';
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM tm) WHERE has(mapKeys(m), 'k1') ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM tm) WHERE has(mapKeys(m), 'k1') ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P31 residual functions stay above the WHERE';
SELECT trimLeft(explain) FROM (
    EXPLAIN compact = 0, actions = 1 SELECT empty(arr), mapContainsValue(m, 1) FROM (SELECT * FROM t WHERE id > 9) SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Expression (%' OR explain LIKE '%Filter (%' OR explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%FUNCTION equals%'
    OR explain LIKE '%FUNCTION has%' OR explain LIKE '% Output:%';
SELECT groupArray((e, c)) FROM (SELECT empty(arr) AS e, mapContainsValue(m, 1) AS c FROM (SELECT * FROM t WHERE id > 9) ORDER BY ALL) SETTINGS optimize_move_to_prewhere = 0;

SELECT 'N23 primary key is used';
SELECT id FROM (SELECT * FROM tk) WHERE length(s) = 3 ORDER BY id SETTINGS force_primary_key = 1;
SELECT 'N24 skip index is used';
SELECT id FROM (SELECT * FROM tm) WHERE has(mapKeys(m), 'k1') ORDER BY id SETTINGS force_data_skipping_indices = 'ik';
SELECT 'N14 optimization off';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t) SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain LIKE '% Output:%';
SELECT 'N15 plan serialized';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM t) SETTINGS serialize_query_plan = 1) WHERE explain LIKE '% Output:%';

SELECT 'N-ADD column added later with a default';
ALTER TABLE t2 ADD COLUMN tup3 Tuple(a UInt64) DEFAULT tuple(7);
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup3.a FROM (SELECT * FROM t2)) WHERE explain LIKE '% Output:%';
SELECT groupArray(tup3.a) FROM (SELECT * FROM t2);
SELECT groupArray(tup3.a) FROM (SELECT * FROM t2) SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N-ADD2 columns added later with DEFAULT and MATERIALIZED expressions';

ALTER TABLE t2 ADD COLUMN tup4 Tuple(a UInt64) DEFAULT tuple(id * 2), ADD COLUMN m4 Tuple(a UInt64) MATERIALIZED tuple(id + 1);

SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup4.a, m4.a FROM (SELECT tup4, m4 FROM t2)) WHERE explain LIKE '% Output:%';

SELECT groupArray(r) FROM (SELECT tup4.a, m4.a FROM (SELECT tup4, m4 FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;

SELECT groupArray(r) FROM (SELECT tup4.a, m4.a FROM (SELECT tup4, m4 FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT groupArray(r) FROM (SELECT tup4.a, m4.a FROM t2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;

SELECT 'P42 the columns of one read are pushed down together';
CREATE TABLE tw (c0 String, c1 String, c2 String, c3 String, c4 String, c5 String, c6 String, c7 String, c8 String, c9 String, c10 String, c11 String, c12 String, c13 String, c14 String, c15 String, c16 String, c17 String, c18 String, c19 String, c20 String, c21 String, c22 String, c23 String, c24 String, c25 String, c26 String, c27 String, c28 String, c29 String, c30 String, c31 String, c32 String, c33 String, c34 String, c35 String, c36 String, c37 String, c38 String, c39 String) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tw SELECT repeat('x', number + 0), repeat('x', number + 1), repeat('x', number + 2), repeat('x', number + 3), repeat('x', number + 4), repeat('x', number + 5), repeat('x', number + 6), repeat('x', number + 7), repeat('x', number + 8), repeat('x', number + 9), repeat('x', number + 10), repeat('x', number + 11), repeat('x', number + 12), repeat('x', number + 13), repeat('x', number + 14), repeat('x', number + 15), repeat('x', number + 16), repeat('x', number + 17), repeat('x', number + 18), repeat('x', number + 19), repeat('x', number + 20), repeat('x', number + 21), repeat('x', number + 22), repeat('x', number + 23), repeat('x', number + 24), repeat('x', number + 25), repeat('x', number + 26), repeat('x', number + 27), repeat('x', number + 28), repeat('x', number + 29), repeat('x', number + 30), repeat('x', number + 31), repeat('x', number + 32), repeat('x', number + 33), repeat('x', number + 34), repeat('x', number + 35), repeat('x', number + 36), repeat('x', number + 37), repeat('x', number + 38), repeat('x', number + 39) FROM numbers(1, 3);
SELECT max(countSubstrings(explain, '.size')) FROM (EXPLAIN SELECT * APPLY length APPLY sum FROM (SELECT * FROM tw) SETTINGS query_plan_max_optimizations_to_apply = 10) WHERE explain LIKE '% Output:%';
SELECT * APPLY length APPLY sum FROM (SELECT * FROM tw) SETTINGS query_plan_max_optimizations_to_apply = 10;
SELECT * APPLY length APPLY sum FROM (SELECT * FROM tw) SETTINGS query_plan_push_down_volume_reducing_functions = 0;

DROP VIEW v;
DROP TABLE t;
DROP TABLE t2;
DROP TABLE tj;
DROP TABLE tr;
DROP TABLE ts;
DROP TABLE ta;
DROP TABLE tn;
DROP TABLE tk;
DROP TABLE tm;
DROP TABLE tl;
DROP TABLE tf;
DROP TABLE tmb;
DROP TABLE tq;
DROP TABLE tw;
