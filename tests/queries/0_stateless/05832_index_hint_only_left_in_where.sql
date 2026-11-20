-- Tags: no-parallel-replicas
-- A filter left in WHERE only for implied `indexHint` conjuncts (the ones `optimize_and_compare_chain` derives) is not kept
-- after PREWHERE: such a hint is constant true when evaluated, and the read has already analyzed its indexes with it.

SET optimize_move_to_prewhere = 1, optimize_and_compare_chain = 1, optimize_use_projections = 1, explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_index_hint_left;
DROP TABLE IF EXISTS t_index_hint_left_proj;
DROP TABLE IF EXISTS t_index_hint_left_whatif;

CREATE TABLE t_index_hint_left (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';

INSERT INTO t_index_hint_left SELECT number, number + number % 3, number % 1000 + 100 FROM numbers(10000);

-- `k < a AND a < 500` derives `indexHint(k < 500)`, the two conditions move to PREWHERE (`b` is read after it).
SELECT count() FROM (EXPLAIN SELECT sum(b) FROM t_index_hint_left WHERE k < a AND a < 500) WHERE trimLeft(explain) LIKE 'Filter%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(b) FROM t_index_hint_left WHERE k < a AND a < 500) WHERE explain LIKE '%Prewhere filter column%';

-- The derived hint still prunes by the primary key.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT sum(b) FROM t_index_hint_left WHERE k < a AND a < 500)
    WHERE explain LIKE '%Condition: (k in (-Inf, 499])%';

-- The same rows as without the derived conjuncts.
SELECT count(), sum(k), sum(a), sum(b) FROM t_index_hint_left WHERE k < a AND a < 500;
SELECT count(), sum(k), sum(a), sum(b) FROM t_index_hint_left WHERE k < a AND a < 500 SETTINGS optimize_and_compare_chain = 0;
SELECT k, a, b FROM t_index_hint_left WHERE k < a AND a < b AND b < 120 ORDER BY k;
SELECT k, a, b FROM t_index_hint_left WHERE k < a AND a < b AND b < 120 ORDER BY k SETTINGS optimize_and_compare_chain = 0;

-- An `indexHint` written in the query selects granules and is not implied by the other conditions: the filter is kept.
SELECT count() FROM (EXPLAIN SELECT sum(a) FROM t_index_hint_left WHERE indexHint(k < 640) AND b < 150) WHERE trimLeft(explain) LIKE 'Filter%';
SELECT count(), sum(a), sum(b) FROM t_index_hint_left WHERE indexHint(k < 640) AND b < 150;

-- The filter column is part of the result: the filter is kept.
SELECT count() FROM (EXPLAIN SELECT k < a AND a < 500 AS c FROM t_index_hint_left WHERE c) WHERE trimLeft(explain) LIKE 'Filter%';
SELECT count(), sum(c) FROM (SELECT k < a AND a < 500 AS c FROM t_index_hint_left WHERE c);

-- `a < b` reads every queried column and stays in WHERE next to the derived `indexHint(a < 500)`: the filter is kept.
SELECT count() FROM (EXPLAIN SELECT sum(b) FROM t_index_hint_left WHERE a < b AND b < 500) WHERE trimLeft(explain) LIKE 'Filter%';
SELECT count(), sum(b) FROM t_index_hint_left WHERE a < b AND b < 500;
SELECT count(), sum(b) FROM t_index_hint_left WHERE a < b AND b < 500 SETTINGS optimize_and_compare_chain = 0;

-- An additional table filter is one more condition moved to PREWHERE.
SELECT count() FROM (EXPLAIN SELECT count(), sum(k), sum(a), sum(b) FROM t_index_hint_left WHERE k < a AND a < 500
    SETTINGS additional_table_filters = {'t_index_hint_left': 'b < 300'}, query_plan_merge_filters = 1) WHERE trimLeft(explain) LIKE 'Filter%';
SELECT count(), sum(k), sum(a), sum(b) FROM t_index_hint_left WHERE k < a AND a < 500
    SETTINGS additional_table_filters = {'t_index_hint_left': 'b < 300'};
SELECT count(), sum(k), sum(a), sum(b) FROM t_index_hint_left WHERE k < a AND a < 500
    SETTINGS additional_table_filters = {'t_index_hint_left': 'b < 300'}, optimize_and_compare_chain = 0;

-- The same rows when the plan is serialized and executed by the shards.
SELECT count(), sum(k), sum(a), sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_index_hint_left) WHERE k < a AND a < 500
    SETTINGS serialize_query_plan = 1;
SELECT count(), sum(k), sum(a), sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_index_hint_left) WHERE k < a AND a < 500
    SETTINGS serialize_query_plan = 1, optimize_and_compare_chain = 0;

-- `optimize_or_like_chain` keeps the original `OR` of `LIKE` as an implied `indexHint` next to `multiSearchAny`.
SELECT count() FROM (EXPLAIN SELECT sum(b) FROM t_index_hint_left
    WHERE toString(a) LIKE '%11%' OR toString(a) LIKE '%22%' OR toString(a) LIKE '%33%' OR toString(a) LIKE '%44%'
    SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_substrings = 4) WHERE trimLeft(explain) LIKE 'Filter%';
SELECT count(), sum(b) FROM t_index_hint_left
    WHERE toString(a) LIKE '%11%' OR toString(a) LIKE '%22%' OR toString(a) LIKE '%33%' OR toString(a) LIKE '%44%'
    SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_substrings = 4;
SELECT count(), sum(b) FROM t_index_hint_left
    WHERE toString(a) LIKE '%11%' OR toString(a) LIKE '%22%' OR toString(a) LIKE '%33%' OR toString(a) LIKE '%44%'
    SETTINGS optimize_or_like_chain = 0;

-- Projections are analyzed after PREWHERE, from the filters of the plan steps. On a table with projections the filter
-- that keeps the derived `indexHint(a < 500)` stays, so projection `p` is used and reads only the granules it selects.
CREATE TABLE t_index_hint_left_proj (k UInt64, a UInt64, b UInt64, c UInt64, PROJECTION p (SELECT * ORDER BY a))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';

INSERT INTO t_index_hint_left_proj SELECT number, number % 1024, number % 1024 + 1, number FROM numbers(10240);
OPTIMIZE TABLE t_index_hint_left_proj FINAL;

SELECT countIf(explain LIKE '%ReadFromMergeTree (p)%'), countIf(explain LIKE '%Granules: 79/160%')
FROM (EXPLAIN indexes = 1 SELECT sum(c) FROM t_index_hint_left_proj WHERE a < b AND b < 500);
SELECT sum(c), count() FROM t_index_hint_left_proj WHERE a < b AND b < 500;
SELECT sum(c), count() FROM t_index_hint_left_proj WHERE a < b AND b < 500 SETTINGS optimize_and_compare_chain = 0;

-- Without projection optimization nothing analyzes the filter later, and it is not kept.
SELECT count() FROM (EXPLAIN SELECT sum(c) FROM t_index_hint_left_proj WHERE a < b AND b < 500 SETTINGS optimize_use_projections = 0)
    WHERE trimLeft(explain) LIKE 'Filter%';

-- A written hint stays visible to projections too: `p` reads the 8 granules (512 rows) that `indexHint(a < 50)` selects.
SELECT countIf(explain LIKE '%ReadFromMergeTree (p)%')
FROM (EXPLAIN SELECT count(), sum(c % 1024) FROM t_index_hint_left_proj WHERE indexHint(a < 50) AND b < 300);
SELECT count(), sum(c % 1024) FROM t_index_hint_left_proj WHERE indexHint(a < 50) AND b < 300;

-- `EXPLAIN WHATIF` weighs a hypothetical projection the same way: the derived bound selects as many marks as the
-- same bound written in the query.
CREATE TABLE t_index_hint_left_whatif (k UInt64, a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';

INSERT INTO t_index_hint_left_whatif SELECT * FROM t_index_hint_left_proj;
OPTIMIZE TABLE t_index_hint_left_whatif FINAL;

SELECT count() FROM (EXPLAIN SELECT sum(c) FROM t_index_hint_left_whatif WHERE a < b AND b < 500) WHERE trimLeft(explain) LIKE 'Filter%';

CREATE HYPOTHETICAL PROJECTION p_h ON t_index_hint_left_whatif (SELECT * ORDER BY a);

SELECT derived[2] = written[2], derived[2] < derived[1]
FROM
(
    SELECT
        (SELECT groupArray(toUInt64(extract(trim(explain), '\\d+')))
         FROM (EXPLAIN WHATIF SELECT sum(c) FROM t_index_hint_left_whatif WHERE a < b AND b < 500)
         WHERE startsWith(trim(explain), 'marks:')) AS derived,
        (SELECT groupArray(toUInt64(extract(trim(explain), '\\d+')))
         FROM (EXPLAIN WHATIF SELECT sum(c) FROM t_index_hint_left_whatif WHERE a < 500 AND a < b AND b < 500)
         WHERE startsWith(trim(explain), 'marks:')) AS written
);

DROP HYPOTHETICAL PROJECTION p_h ON t_index_hint_left_whatif;

DROP TABLE t_index_hint_left;
DROP TABLE t_index_hint_left_proj;
DROP TABLE t_index_hint_left_whatif;
