-- A condition on a primary key column that gives no key range (e.g. `k LIKE '%X%'`, without a fixed prefix) is not
-- a primary key condition, so it is not moved to the end of PREWHERE ahead of more expensive conditions (#131237).

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 1;
SET move_primary_key_columns_to_end_of_prewhere = 1, use_statistics = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_like_pk;
DROP TABLE IF EXISTS t_like_pk_build;

-- PREWHERE conditions are ordered by column size on disk: `k` is small, `v` is large.
CREATE TABLE t_like_pk (k String, d DateTime('UTC'), v String) ENGINE = MergeTree ORDER BY (k, d)
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '', default_compression_codec = 'LZ4';
INSERT INTO t_like_pk SELECT concat('K', toString(number % 50), if(number % 10 = 0, 'X', '')), toDateTime('2020-01-01 00:00:00', 'UTC') + number, concat(hex(sipHash128(number)), hex(sipHash128(toString(number)))) FROM numbers(10000);

CREATE TABLE t_like_pk_build (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_like_pk_build SELECT v FROM t_like_pk WHERE d < toDateTime('2020-01-01 00:01:40', 'UTC');

SELECT '-- LIKE without a fixed prefix on a key column runs first';
SELECT replaceRegexpAll(explain, '__table1\.|_String|_UInt8', '') FROM (EXPLAIN actions = 1 SELECT count() FROM t_like_pk WHERE k LIKE '%X%' AND v LIKE '%AB%') WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM t_like_pk WHERE k LIKE '%X%' AND v LIKE '%AB%';

SELECT '-- LIKE with a fixed prefix is a key condition and stays last';
SELECT replaceRegexpAll(explain, '__table1\.|_String|_UInt8', '') FROM (EXPLAIN actions = 1 SELECT count() FROM t_like_pk WHERE k LIKE 'K1%X' AND v LIKE '%AB%') WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM t_like_pk WHERE k LIKE 'K1%X' AND v LIKE '%AB%';

SELECT '-- The next key column is not moved to the end either, as the first key column has no key condition';
SELECT replaceRegexpAll(explain, '__table1\.|_String|_UInt8', '') FROM (EXPLAIN actions = 1 SELECT count() FROM t_like_pk WHERE k LIKE '%X%' AND d >= toDateTime('2020-01-01 00:00:00', 'UTC') AND v LIKE '%AB%') WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM t_like_pk WHERE k LIKE '%X%' AND d >= toDateTime('2020-01-01 00:00:00', 'UTC') AND v LIKE '%AB%';

SELECT '-- match without a fixed prefix runs first, with one it stays last';
SELECT replaceRegexpAll(explain, '__table1\.|_String|_UInt8', '') FROM (EXPLAIN actions = 1 SELECT count() FROM t_like_pk WHERE match(k, 'X') AND v LIKE '%AB%') WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM t_like_pk WHERE match(k, 'X') AND v LIKE '%AB%';
SELECT replaceRegexpAll(explain, '__table1\.|_String|_UInt8', '') FROM (EXPLAIN actions = 1 SELECT count() FROM t_like_pk WHERE match(k, '^K1') AND v LIKE '%AB%') WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM t_like_pk WHERE match(k, '^K1') AND v LIKE '%AB%';

SELECT '-- The join runtime filter on the large column runs after the LIKE';
SELECT replaceRegexpAll(replaceRegexpAll(explain, '_runtime_filter_[0-9]+', '_runtime_filter'), '__table[0-9]+\.|_String|_UInt8', '') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_like_pk AS t INNER JOIN t_like_pk_build AS b ON t.v = b.v WHERE t.k LIKE '%X%'
    SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 0, join_runtime_filter_min_probe_rows = 0,
        query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0)
WHERE explain LIKE '%Prewhere filter column%';
SELECT count() FROM t_like_pk AS t INNER JOIN t_like_pk_build AS b ON t.v = b.v WHERE t.k LIKE '%X%'
SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 0, join_runtime_filter_min_probe_rows = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;

DROP TABLE t_like_pk;
DROP TABLE t_like_pk_build;
