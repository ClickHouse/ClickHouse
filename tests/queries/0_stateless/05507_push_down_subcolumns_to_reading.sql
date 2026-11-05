-- Tags: no-parallel-replicas
-- The reads of subcolumns of columns returned by subqueries are pushed down to ReadFromMergeTree:
-- the first line of each case lists the columns read (except `id`), the next two the result with the optimization and without it.
-- A case name followed by a number is 1 if the index, projection or read in order used without the optimization is still used with it.

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
DROP TABLE IF EXISTS tq;
DROP VIEW IF EXISTS v;
DROP ROW POLICY IF EXISTS rp_05507 ON trp;
DROP TABLE IF EXISTS trp;
DROP TABLE IF EXISTS tjh;
DROP TABLE IF EXISTS tmu;
DROP TABLE IF EXISTS tmu2;
DROP TABLE IF EXISTS tmat;
DROP TABLE IF EXISTS tlw;
DROP TABLE IF EXISTS tcp;
DROP TABLE IF EXISTS tpn;
DROP TABLE IF EXISTS tjp;
DROP TABLE IF EXISTS tpa;
DROP TABLE IF EXISTS tpf;
DROP TABLE IF EXISTS tsk;
DROP TABLE IF EXISTS tsm;
DROP TABLE IF EXISTS k1m;
DROP TABLE IF EXISTS k2t;
DROP TABLE IF EXISTS k3;
DROP TABLE IF EXISTS k3w;
DROP TABLE IF EXISTS k4;
DROP TABLE IF EXISTS k5;
DROP TABLE IF EXISTS k5s;
DROP TABLE IF EXISTS k5w;
DROP TABLE IF EXISTS k6s;
DROP TABLE IF EXISTS k6m;
DROP TABLE IF EXISTS k6t;
DROP TABLE IF EXISTS k7m;
DROP TABLE IF EXISTS k8;
DROP VIEW IF EXISTS vtab;
DROP VIEW IF EXISTS vtpk;
DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tpk;

CREATE TABLE t (id UInt64, data JSON, tup Tuple(a UInt64, b String), n Nullable(String), s String, arr Array(UInt32), m Map(String, UInt64),
    lc LowCardinality(Nullable(String)), v Variant(UInt64, String), d Dynamic)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
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

CREATE TABLE tf (id UInt64, m2 Map(FixedString(4), UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tf SELECT number, map(toFixedString('ab', 4), number) FROM numbers(3);

CREATE TABLE trp (id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO trp SELECT number, (number, toString(number)) FROM numbers(10);
CREATE ROW POLICY rp_05507 ON trp USING cityHash64(tup) % 2 = 0 TO ALL;

CREATE TABLE tjh (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES tjh;
INSERT INTO tjh SELECT number, toJSONString(map('a', number)) FROM numbers(4);
ALTER TABLE tjh MODIFY COLUMN data JSON(a UInt32) SETTINGS enable_json_lazy_type_hints = 1;
INSERT INTO tjh SELECT number + 4, toJSONString(map('a', number + 4)) FROM numbers(4);

CREATE TABLE tmu (k UInt64, id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES tmu;
INSERT INTO tmu SELECT number, number, (number, 'x') FROM numbers(4);
ALTER TABLE tmu UPDATE tup = (tup.a * 10, 'u') WHERE 1 SETTINGS mutations_sync = 0;

CREATE TABLE tmu2 (k UInt64, id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES tmu2;
INSERT INTO tmu2 SELECT number, number, (number, 'x') FROM numbers(4);
ALTER TABLE tmu2 UPDATE id = id + 1 WHERE 1 SETTINGS mutations_sync = 0;

CREATE TABLE tmat (x UInt64, m Tuple(a UInt64, b String) MATERIALIZED tuple(x, 'z'), al UInt64 ALIAS x + 1, m1 UInt64 MATERIALIZED al * 2,
    m2 Tuple(a UInt64) MATERIALIZED tuple(m1)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES tmat;
INSERT INTO tmat (x) VALUES (1), (2);
ALTER TABLE tmat UPDATE x = 42 WHERE 1 SETTINGS mutations_sync = 0;

CREATE TABLE tlw (x UInt64, m Tuple(a UInt64, b String) MATERIALIZED tuple(x, 'z')) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES tlw;
INSERT INTO tlw (x) VALUES (1), (2);
UPDATE tlw SET x = 42 WHERE 1;

CREATE TABLE tcp (id UInt64, tup Tuple(a UInt64, b String), n Nullable(String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 0;
INSERT INTO tcp SELECT number, (number, 'c'), if(number % 2 = 0, NULL, 'x') FROM numbers(6);

CREATE TABLE tpn (id UInt64, k UInt64, tup Tuple(a UInt64, b String), PROJECTION p (SELECT id, tup ORDER BY id)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO tpn SELECT number, number % 3, (number, toString(number)) FROM numbers(64);

CREATE TABLE tjp (id UInt64, k UInt64, data JSON, PROJECTION p (SELECT * ORDER BY id)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES tjp;
INSERT INTO tjp SELECT number, number % 3, toJSONString(map('a', number)) FROM numbers(64);
ALTER TABLE tjp MODIFY COLUMN data JSON(a UInt32) SETTINGS enable_json_lazy_type_hints = 1;

CREATE TABLE tpa (s String, PROJECTION p (SELECT length(s), count() GROUP BY length(s))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tpa SELECT repeat('a', number % 10) FROM numbers(1000);

CREATE TABLE tpf (id UInt64, s String, PROJECTION p (SELECT id, s WHERE length(s) > 98 ORDER BY id)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
INSERT INTO tpf SELECT number, repeat('a', number % 100) FROM numbers(10000);

CREATE TABLE tsk (id UInt64, s String) ENGINE = MergeTree ORDER BY (id, length(s)) PRIMARY KEY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tsk SELECT number % 3, repeat('x', number % 5) FROM numbers(64);

CREATE TABLE tsm (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY (id, m['k']) PRIMARY KEY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO tsm SELECT number % 3, map('k', number % 8) FROM numbers(64);

CREATE TABLE k1m (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY m['k']
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k1m SELECT number, map('k', number % 8) FROM numbers(64);

CREATE TABLE k2t (id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY (id, tup.a) PRIMARY KEY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO k2t SELECT number % 3, (number % 7, 'b') FROM numbers(64);

CREATE TABLE k3 (id UInt64, s String, tup Tuple(a UInt64, b String), m Map(String, UInt64)) ENGINE = MergeTree
PARTITION BY (length(s), m['k'], tup.a) ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO k3 SELECT number, repeat('x', number % 2), (intDiv(number, 2) % 2, 'b'), map('k', intDiv(number, 4) % 2) FROM numbers(16);

CREATE TABLE k3w (id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree PARTITION BY tup ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO k3w SELECT number, (intDiv(number, 32), 'b') FROM numbers(64);

CREATE TABLE k4 (id UInt64, s String, tup Tuple(a UInt64, b String), m Map(String, UInt64),
    INDEX i_len length(s) TYPE minmax GRANULARITY 1, INDEX i_mk m['k'] TYPE minmax GRANULARITY 1, INDEX i_ta tup.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k4 SELECT number, repeat('x', intDiv(number, 8)), (intDiv(number, 8), 'b'), map('k', intDiv(number, 8)) FROM numbers(32);

CREATE TABLE k5 (id UInt64, s String, tup Tuple(a UInt64, b String), m Map(String, UInt64),
    INDEX b_len length(s) TYPE bloom_filter GRANULARITY 1, INDEX b_mk mapKeys(m) TYPE bloom_filter GRANULARITY 1,
    INDEX b_me m['k'] TYPE bloom_filter GRANULARITY 1, INDEX b_ta tup.a TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k5 SELECT number, repeat('x', intDiv(number, 8)), (intDiv(number, 8), 'b'), if(number < 8, map('k', 1), map('z', 2)) FROM numbers(32);

CREATE TABLE k5s (id UInt64, m Map(String, String),
    INDEX x_set m['k'] TYPE set(0) GRANULARITY 1, INDEX x_txt m['k'] TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k5s SELECT number, if(number < 8, map('k', 'needle hay'), map('z', 'other')) FROM numbers(32);

CREATE TABLE k5w (id UInt64, tup Tuple(a UInt64, b String), n Nullable(String), INDEX w_set tup TYPE set(0) GRANULARITY 1, INDEX n_set n TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k5w SELECT number, (intDiv(number, 8), 'b'), if(number < 8, NULL, 'x') FROM numbers(64);

CREATE TABLE k6s (id UInt64, s String, PROJECTION p (SELECT * ORDER BY length(s))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k6s SELECT number, repeat('x', number % 5) FROM numbers(64);

CREATE TABLE k6m (id UInt64, m Map(String, UInt64), PROJECTION p (SELECT * ORDER BY m['k'])) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k6m SELECT number, map('k', number % 8) FROM numbers(64);

CREATE TABLE k6t (id UInt64, tup Tuple(a UInt64, b String), PROJECTION p (SELECT * ORDER BY tup.a)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO k6t SELECT number, (number % 7, 'b') FROM numbers(64);

CREATE TABLE k7m (id UInt64, m Map(String, UInt64), PROJECTION p (SELECT m['k'], count() GROUP BY m['k'])) ENGINE = MergeTree ORDER BY id;
INSERT INTO k7m SELECT number, map('k', number % 8) FROM numbers(64);

CREATE TABLE k8 (id UInt64, tup Tuple(a UInt64, b String), PROJECTION p (SELECT tup, count() GROUP BY tup)) ENGINE = MergeTree ORDER BY id;
INSERT INTO k8 SELECT number, (number % 7, toString(number % 2)) FROM numbers(64);

CREATE TABLE tab (id UInt64, turns Array(Tuple(role String, text String)), tup Tuple(a UInt64, text String),
    INDEX idx_text turns.text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_arr_bf turns.text TYPE bloom_filter GRANULARITY 1,
    INDEX idx_tup_bf tup.text TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1024;
INSERT INTO tab SELECT number, [('user', concat('lorem ipsum filler words row ', toString(number)))], (number, concat('t', toString(number))) FROM numbers(30000);
INSERT INTO tab VALUES (30000, [('user', 'this row contains the needle token')], (30000, 'needle'));
CREATE VIEW vtab AS SELECT * FROM tab;

CREATE TABLE tpk (id UInt64, t Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY t.a SETTINGS index_granularity = 1024;
INSERT INTO tpk SELECT number, (number, toString(number)) FROM numbers(30000);
CREATE VIEW vtpk AS SELECT * FROM tpk;

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

-- r8: on-the-fly changes, keys, indexes and projections.

SELECT 'N9 row policy on the whole column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM trp)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM trp) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM trp) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N11 JSON part written before a lazy type hint';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT data.a FROM (SELECT * FROM tjh)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT data.a FROM (SELECT * FROM tjh) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT data.a FROM (SELECT * FROM tjh) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N12 pending mutation of the column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM tmu) SETTINGS apply_mutations_on_fly = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tmu) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tmu) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N25b pending mutation of another column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM tmu2) SETTINGS apply_mutations_on_fly = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tmu2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tmu2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N25 MATERIALIZED columns of a column mutated on the fly';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT sum(m.a), sum(m2.a) FROM (SELECT m, m2 FROM tmat) SETTINGS apply_mutations_on_fly = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT sum(m.a), sum(m2.a) FROM (SELECT m, m2 FROM tmat) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1;
SELECT groupArray(r) FROM (SELECT sum(m.a), sum(m2.a) FROM (SELECT m, m2 FROM tmat) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N25 two subquery levels';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT sum(m.a), sum(m2.a) FROM (SELECT * FROM (SELECT m, m2 FROM tmat)) SETTINGS apply_mutations_on_fly = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT sum(m.a), sum(m2.a) FROM (SELECT * FROM (SELECT m, m2 FROM tmat)) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1;
SELECT groupArray(r) FROM (SELECT sum(m.a), sum(m2.a) FROM (SELECT * FROM (SELECT m, m2 FROM tmat)) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N25 tupleElement';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT sum(tupleElement(m, 'a')) FROM (SELECT m FROM tmat) SETTINGS apply_mutations_on_fly = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT sum(tupleElement(m, 'a')) FROM (SELECT m FROM tmat) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1;
SELECT groupArray(r) FROM (SELECT sum(tupleElement(m, 'a')) FROM (SELECT m FROM tmat) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS apply_mutations_on_fly = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N26 patch part of a lightweight update';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT sum(m.a), sum(x) FROM (SELECT m, x FROM tlw)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT sum(m.a), sum(x) FROM (SELECT m, x FROM tlw) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT sum(m.a), sum(x) FROM (SELECT m, x FROM tlw) ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N13 compact part without substream marks';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a, n.null FROM (SELECT * FROM tcp)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a, n.null FROM (SELECT * FROM tcp) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a, n.null FROM (SELECT * FROM tcp) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'P20 normal projection';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM tpn) WHERE id = 3 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tpn) WHERE id = 3 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM tpn) WHERE id = 3 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N20 normal projection, part written before a lazy type hint';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT data.a FROM (SELECT * FROM tjp) WHERE id = 1 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT data.a FROM (SELECT * FROM tjp) WHERE id = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT data.a FROM (SELECT * FROM tjp) WHERE id = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N27 aggregate projection over length(s)';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(s), count() FROM (SELECT s FROM tpa) GROUP BY length(s) SETTINGS force_optimize_projection = 1, optimize_use_projections = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(s), count() FROM (SELECT s FROM tpa) GROUP BY length(s) ORDER BY ALL LIMIT 3) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT length(s), count() FROM (SELECT s FROM tpa) GROUP BY length(s) ORDER BY ALL LIMIT 3) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N28 filtered projection over length(s)';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT count() FROM (SELECT s FROM tpf) WHERE length(s) > 98 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT s FROM tpf) WHERE length(s) > 98 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT s FROM tpf) WHERE length(s) > 98 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N29 aggregate projection, two subquery levels';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(s), count() FROM (SELECT * FROM (SELECT s FROM tpa)) GROUP BY length(s) SETTINGS force_optimize_projection = 1, optimize_use_projections = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(s), count() FROM (SELECT * FROM (SELECT s FROM tpa)) GROUP BY length(s) ORDER BY ALL LIMIT 3) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT length(s), count() FROM (SELECT * FROM (SELECT s FROM tpa)) GROUP BY length(s) ORDER BY ALL LIMIT 3) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N29 filtered projection, two subquery levels';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT count() FROM (SELECT * FROM (SELECT s FROM tpf)) WHERE length(s) > 98 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM (SELECT s FROM tpf)) WHERE length(s) > 98 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM (SELECT s FROM tpf)) WHERE length(s) > 98 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N30 sorting key suffix length(s)';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 SETTINGS optimize_read_in_order = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1;
SELECT groupArray(r) FROM (SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N30 read in order kept', (SELECT count() FROM (EXPLAIN actions = 1 SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Read type: InOrder%') <= (SELECT count() FROM (EXPLAIN actions = 1 SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InOrder%');
SELECT groupArray(r) FROM (SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1;
SELECT groupArray(r) FROM (SELECT length(s) FROM (SELECT s, id FROM tsk) WHERE id = 0 ORDER BY length(s) LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N31 sorting key suffix m[\'k\']';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 SETTINGS optimize_read_in_order = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1;
SELECT groupArray(r) FROM (SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N31 read in order kept', (SELECT count() FROM (EXPLAIN actions = 1 SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Read type: InOrder%') <= (SELECT count() FROM (EXPLAIN actions = 1 SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InOrder%');
SELECT groupArray(r) FROM (SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1;
SELECT groupArray(r) FROM (SELECT m['k'] FROM (SELECT m, id FROM tsm) WHERE id = 0 ORDER BY m['k'] LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K1 primary key m[\'k\']', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k1m) WHERE m['k'] = 3 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Condition: (%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k1m) WHERE m['k'] = 3) WHERE explain ILIKE '%Condition: (%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k1m) WHERE m['k'] = 3 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k1m) WHERE m['k'] = 3 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K2 sorting key suffix tup.a', (SELECT count() FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup, id FROM k2t) WHERE id = 0 ORDER BY tup.a LIMIT 3 SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Read type: InOrder%') <= (SELECT count() FROM (EXPLAIN actions = 1 SELECT tup.a FROM (SELECT tup, id FROM k2t) WHERE id = 0 ORDER BY tup.a LIMIT 3 SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InOrder%');
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT tup, id FROM k2t) WHERE id = 0 ORDER BY tup.a LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT tup, id FROM k2t) WHERE id = 0 ORDER BY tup.a LIMIT 3 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_read_in_order = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K3 partition key length(s)', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3) WHERE length(s) = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Condition: (%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3) WHERE length(s) = 1) WHERE explain ILIKE '%Condition: (%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3) WHERE length(s) = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3) WHERE length(s) = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K3 partition key m[\'k\']', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3) WHERE m['k'] = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Condition: (%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3) WHERE m['k'] = 1) WHERE explain ILIKE '%Condition: (%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3) WHERE m['k'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3) WHERE m['k'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K3 partition key tup.a', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3) WHERE tup.a = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Condition: (%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3) WHERE tup.a = 1) WHERE explain ILIKE '%Condition: (%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3) WHERE tup.a = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3) WHERE tup.a = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K3 partition key over the whole tuple', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3w) WHERE tup.a = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Condition: (%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k3w) WHERE tup.a = 1) WHERE explain ILIKE '%Condition: (%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3w) WHERE tup.a = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k3w) WHERE tup.a = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K4 minmax length(s)', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k4) WHERE length(s) = 2 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: i_len%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k4) WHERE length(s) = 2) WHERE explain ILIKE '%Name: i_len%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k4) WHERE length(s) = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k4) WHERE length(s) = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K4 minmax m[\'k\']', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k4) WHERE m['k'] = 2 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: i_mk%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k4) WHERE m['k'] = 2) WHERE explain ILIKE '%Name: i_mk%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k4) WHERE m['k'] = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k4) WHERE m['k'] = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K4 minmax tup.a', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k4) WHERE tup.a = 2 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: i_ta%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k4) WHERE tup.a = 2) WHERE explain ILIKE '%Name: i_ta%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k4) WHERE tup.a = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k4) WHERE tup.a = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 bloom filter length(s)', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE length(s) = 2 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: b_len%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE length(s) = 2) WHERE explain ILIKE '%Name: b_len%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE length(s) = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE length(s) = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 bloom filter mapKeys(m)', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: b_mk%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1) WHERE explain ILIKE '%Name: b_mk%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 bloom filter m[\'k\']', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: b_me%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1) WHERE explain ILIKE '%Name: b_me%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE m['k'] = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 bloom filter tup.a', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE tup.a = 2 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: b_ta%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5) WHERE tup.a = 2) WHERE explain ILIKE '%Name: b_ta%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE tup.a = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5) WHERE tup.a = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 set m[\'k\']', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5s) WHERE m['k'] = 'needle hay' SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: x_set%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5s) WHERE m['k'] = 'needle hay') WHERE explain ILIKE '%Name: x_set%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5s) WHERE m['k'] = 'needle hay' ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5s) WHERE m['k'] = 'needle hay' ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 text m[\'k\']', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5s) WHERE hasAnyTokens(m['k'], ['needle']) SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: x_txt%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5s) WHERE hasAnyTokens(m['k'], ['needle'])) WHERE explain ILIKE '%Name: x_txt%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5s) WHERE hasAnyTokens(m['k'], ['needle']) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5s) WHERE hasAnyTokens(m['k'], ['needle']) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 set over the whole tuple', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5w) WHERE tup.a = 2 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: w_set%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5w) WHERE tup.a = 2) WHERE explain ILIKE '%Name: w_set%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5w) WHERE tup.a = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5w) WHERE tup.a = 2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K5 set over the whole Nullable', (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5w) WHERE n.null = 1 SETTINGS query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%Name: n_set%') <= (SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM (SELECT * FROM k5w) WHERE n.null = 1) WHERE explain ILIKE '%Name: n_set%');
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5w) WHERE n.null = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT id FROM (SELECT * FROM k5w) WHERE n.null = 1 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K6 normal projection length(s)', (SELECT count() FROM (EXPLAIN SELECT count() FROM (SELECT * FROM k6s) WHERE length(s) = 2 SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%ReadFromMergeTree (p)%') <= (SELECT count() FROM (EXPLAIN SELECT count() FROM (SELECT * FROM k6s) WHERE length(s) = 2 SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%');
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM k6s) WHERE length(s) = 2 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM k6s) WHERE length(s) = 2 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K6 normal projection m[\'k\']', (SELECT count() FROM (EXPLAIN SELECT count() FROM (SELECT * FROM k6m) WHERE m['k'] = 2 SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%ReadFromMergeTree (p)%') <= (SELECT count() FROM (EXPLAIN SELECT count() FROM (SELECT * FROM k6m) WHERE m['k'] = 2 SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%');
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM k6m) WHERE m['k'] = 2 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM k6m) WHERE m['k'] = 2 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K6 normal projection tup.a', (SELECT count() FROM (EXPLAIN SELECT count() FROM (SELECT * FROM k6t) WHERE tup.a = 2 SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%ReadFromMergeTree (p)%') <= (SELECT count() FROM (EXPLAIN SELECT count() FROM (SELECT * FROM k6t) WHERE tup.a = 2 SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%');
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM k6t) WHERE tup.a = 2 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT count() FROM (SELECT * FROM k6t) WHERE tup.a = 2 ) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K7 aggregate projection m[\'k\']', (SELECT count() FROM (EXPLAIN SELECT m['k'] AS x, count() FROM (SELECT * FROM k7m) GROUP BY x SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%ReadFromMergeTree (p)%') <= (SELECT count() FROM (EXPLAIN SELECT m['k'] AS x, count() FROM (SELECT * FROM k7m) GROUP BY x SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%');
SELECT groupArray(r) FROM (SELECT m['k'] AS x, count() FROM (SELECT * FROM k7m) GROUP BY x ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT m['k'] AS x, count() FROM (SELECT * FROM k7m) GROUP BY x ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K8 aggregate projection over the whole tuple', (SELECT count() FROM (EXPLAIN SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0) WHERE explain ILIKE '%ReadFromMergeTree (p)%') <= (SELECT count() FROM (EXPLAIN SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x SETTINGS optimize_use_projections = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%');
SELECT groupArray(r) FROM (SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K8 plan';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x SETTINGS optimize_use_projections = 1) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1;
SELECT groupArray(r) FROM (SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS optimize_use_projections = 1, query_plan_push_down_volume_reducing_functions = 0;

SELECT 'K3 plan, partition key over the whole tuple';
SELECT replaceRegexpOne(explain, '^.*Prewhere filter column: *', '') FROM (EXPLAIN SELECT id FROM (SELECT * FROM k3w) WHERE tup.a = 1) WHERE explain LIKE '%Prewhere filter column:%';

SELECT 'K5 plan, set index over the whole tuple and Nullable';
SELECT replaceRegexpOne(explain, '^.*Prewhere filter column: *', '') FROM (EXPLAIN SELECT id FROM (SELECT * FROM k5w) WHERE tup.a = 2) WHERE explain LIKE '%Prewhere filter column:%';
SELECT replaceRegexpOne(explain, '^.*Prewhere filter column: *', '') FROM (EXPLAIN SELECT id FROM (SELECT * FROM k5w) WHERE n.null = 1) WHERE explain LIKE '%Prewhere filter column:%';

SELECT 'N27-N29 projections are used';

SELECT length(s), count() FROM (SELECT s FROM tpa) GROUP BY length(s) ORDER BY ALL LIMIT 3 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

SELECT count() FROM (SELECT s FROM tpf) WHERE length(s) > 98 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1;

SELECT tup.a AS x, count() FROM (SELECT * FROM k8) GROUP BY x ORDER BY ALL LIMIT 3 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

SELECT 'K1 K3 K4 K5 indexes are used';

SELECT count() FROM (SELECT * FROM k1m) WHERE m['k'] = 3 SETTINGS force_primary_key = 1;

SELECT count() FROM (SELECT * FROM k4) WHERE length(s) = 2 SETTINGS force_data_skipping_indices = 'i_len';

SELECT count() FROM (SELECT * FROM k4) WHERE m['k'] = 2 SETTINGS force_data_skipping_indices = 'i_mk';

SELECT count() FROM (SELECT * FROM k4) WHERE tup.a = 2 SETTINGS force_data_skipping_indices = 'i_ta';

SELECT count() FROM (SELECT * FROM k5) WHERE length(s) = 2 SETTINGS force_data_skipping_indices = 'b_len';

SELECT count() FROM (SELECT * FROM k5) WHERE m['k'] = 1 SETTINGS force_data_skipping_indices = 'b_mk';

SELECT count() FROM (SELECT * FROM k5) WHERE m['k'] = 1 SETTINGS force_data_skipping_indices = 'b_me';

SELECT count() FROM (SELECT * FROM k5) WHERE tup.a = 2 SETTINGS force_data_skipping_indices = 'b_ta';

SELECT count() FROM (SELECT * FROM k3) WHERE length(s) = 1 SETTINGS force_index_by_date = 1;

SELECT count() FROM (SELECT * FROM k3) WHERE m['k'] = 1 SETTINGS force_index_by_date = 1;

SELECT count() FROM (SELECT * FROM k3) WHERE tup.a = 1 SETTINGS force_index_by_date = 1;

SELECT count() FROM (SELECT * FROM k3w) WHERE tup.a = 1 SETTINGS force_index_by_date = 1;

SELECT count() FROM (SELECT * FROM k5s) WHERE m['k'] = 'needle hay' SETTINGS force_data_skipping_indices = 'x_set';

SELECT count() FROM (SELECT * FROM k5s) WHERE hasAnyTokens(m['k'], ['needle']) SETTINGS force_data_skipping_indices = 'x_txt';

SELECT count() FROM (SELECT * FROM k5w) WHERE tup.a = 2 SETTINGS force_data_skipping_indices = 'w_set';

SELECT count() FROM (SELECT * FROM k5w) WHERE n.null = 1 SETTINGS force_data_skipping_indices = 'n_set';

SELECT 'N-ADD2 columns added later with DEFAULT and MATERIALIZED expressions';

ALTER TABLE t2 ADD COLUMN tup4 Tuple(a UInt64) DEFAULT tuple(id * 2), ADD COLUMN m4 Tuple(a UInt64) MATERIALIZED tuple(id + 1);

SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup4.a, m4.a FROM (SELECT tup4, m4 FROM t2)) WHERE explain LIKE '% Output:%';

SELECT groupArray(r) FROM (SELECT tup4.a, m4.a FROM (SELECT tup4, m4 FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;

SELECT groupArray(r) FROM (SELECT tup4.a, m4.a FROM (SELECT tup4, m4 FROM t2) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT groupArray(r) FROM (SELECT tup4.a, m4.a FROM t2 ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;

SELECT 'P36-P39 indexes over subcolumns read through a view';

SELECT id FROM vtab WHERE hasAnyTokens(turns.text, ['needle']) SETTINGS force_data_skipping_indices = 'idx_text';

SELECT id FROM vtab WHERE has(turns.text, 'this row contains the needle token') SETTINGS force_data_skipping_indices = 'idx_arr_bf';

SELECT id FROM vtab WHERE tup.text = 'needle' SETTINGS force_data_skipping_indices = 'idx_tup_bf';

SELECT id FROM vtpk WHERE t.a = 777 SETTINGS force_primary_key = 1;

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
DROP TABLE tq;
DROP ROW POLICY rp_05507 ON trp;
DROP VIEW vtab;
DROP VIEW vtpk;
DROP TABLE trp;
DROP TABLE tjh;
DROP TABLE tmu;
DROP TABLE tmu2;
DROP TABLE tmat;
DROP TABLE tlw;
DROP TABLE tcp;
DROP TABLE tpn;
DROP TABLE tjp;
DROP TABLE tpa;
DROP TABLE tpf;
DROP TABLE tsk;
DROP TABLE tsm;
DROP TABLE k1m;
DROP TABLE k2t;
DROP TABLE k3;
DROP TABLE k3w;
DROP TABLE k4;
DROP TABLE k5;
DROP TABLE k5s;
DROP TABLE k5w;
DROP TABLE k6s;
DROP TABLE k6m;
DROP TABLE k6t;
DROP TABLE k7m;
DROP TABLE k8;
DROP TABLE tab;
DROP TABLE tpk;
