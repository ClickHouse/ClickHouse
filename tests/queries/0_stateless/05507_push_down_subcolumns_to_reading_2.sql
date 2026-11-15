-- Tags: no-parallel-replicas
-- The reads of subcolumns of columns returned by subqueries are pushed down to ReadFromMergeTree:
-- the first line of each case lists the columns read (except `id`), the next two the result with the optimization and without it.
-- A case name followed by a number is 1 if the index, projection or read in order used without the optimization is still used with it.

SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1, query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 1;
SET serialize_query_plan = 0, query_plan_optimize_lazy_materialization = 0, optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1,
    query_plan_merge_filters = 1, query_plan_optimize_join_order_randomize = 0;
SET allow_suspicious_low_cardinality_types = 1, allow_experimental_variant_type = 1, allow_experimental_dynamic_type = 1, enable_json_type = 1;

DROP ROW POLICY IF EXISTS rp_05507 ON trp;
DROP TABLE IF EXISTS trp;
DROP TABLE IF EXISTS tjh;
DROP TABLE IF EXISTS tjo;
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

CREATE TABLE trp (id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO trp SELECT number, (number, toString(number)) FROM numbers(10);
CREATE ROW POLICY rp_05507 ON trp USING cityHash64(tup) % 2 = 0 TO ALL;

CREATE TABLE tjh (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES tjh;
INSERT INTO tjh SELECT number, toJSONString(map('a', number)) FROM numbers(4);
ALTER TABLE tjh MODIFY COLUMN data JSON(a UInt32) SETTINGS enable_json_lazy_type_hints = 1;
INSERT INTO tjh SELECT number + 4, toJSONString(map('a', number + 4)) FROM numbers(4);

CREATE TABLE tjo (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES tjo;
INSERT INTO tjo VALUES (1, '{}'), (2, '{}');
ALTER TABLE tjo MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;

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

SELECT 'N9 row policy on the whole column';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT tup.a FROM (SELECT * FROM trp)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM trp) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT tup.a FROM (SELECT * FROM trp) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N11 JSON part written before a lazy type hint';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT data.a FROM (SELECT * FROM tjh)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT data.a FROM (SELECT * FROM tjh) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT data.a FROM (SELECT * FROM tjh) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

SELECT 'N11b JSON column omitted from a part before a lazy type hint';
SELECT arrayStringConcat(arraySort(arrayFilter(x -> x != '', groupArray(arrayStringConcat(arraySort(arrayFilter(x -> x != 'id', splitByString(', ', replaceRegexpOne(explain, '^.*Output: ', '')))), ', ')))), ' | ') FROM (EXPLAIN SELECT length(data.a) FROM (SELECT * FROM tjo)) WHERE explain LIKE '% Output:%';
SELECT groupArray(r) FROM (SELECT length(data.a) FROM (SELECT * FROM tjo) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r;
SELECT groupArray(r) FROM (SELECT length(data.a) FROM (SELECT * FROM tjo) ORDER BY ALL) AS r_subquery ARRAY JOIN [tuple(*)] AS r SETTINGS query_plan_push_down_volume_reducing_functions = 0;

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

SELECT 'N27-N29 projections are used';

SELECT length(s), count() FROM (SELECT s FROM tpa) GROUP BY length(s) ORDER BY ALL LIMIT 3 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

SELECT count() FROM (SELECT s FROM tpf) WHERE length(s) > 98 SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1;

DROP ROW POLICY rp_05507 ON trp;
DROP TABLE trp;
DROP TABLE tjh;
DROP TABLE tjo;
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
