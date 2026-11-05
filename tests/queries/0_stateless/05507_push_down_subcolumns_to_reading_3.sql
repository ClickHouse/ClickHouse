-- Tags: no-parallel-replicas
-- The reads of subcolumns of columns returned by subqueries are pushed down to ReadFromMergeTree:
-- the first line of each case lists the columns read (except `id`), the next two the result with the optimization and without it.
-- A case name followed by a number is 1 if the index, projection or read in order used without the optimization is still used with it.

SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 1, query_plan_push_down_volume_reducing_functions = 1, optimize_functions_to_subcolumns = 1;
SET serialize_query_plan = 0, query_plan_optimize_lazy_materialization = 0, optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1,
    query_plan_merge_filters = 1, query_plan_optimize_join_order_randomize = 0;
SET allow_suspicious_low_cardinality_types = 1, allow_experimental_variant_type = 1, allow_experimental_dynamic_type = 1, enable_json_type = 1;

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

SELECT 'K8 projection is used';

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

SELECT 'P36-P39 indexes over subcolumns read through a view';

SELECT id FROM vtab WHERE hasAnyTokens(turns.text, ['needle']) SETTINGS force_data_skipping_indices = 'idx_text';

SELECT id FROM vtab WHERE has(turns.text, 'this row contains the needle token') SETTINGS force_data_skipping_indices = 'idx_arr_bf';

SELECT id FROM vtab WHERE tup.text = 'needle' SETTINGS force_data_skipping_indices = 'idx_tup_bf';

SELECT id FROM vtpk WHERE t.a = 777 SETTINGS force_primary_key = 1;

DROP VIEW vtab;
DROP VIEW vtpk;
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
