-- Combined skip indexes (`use_skip_indexes_for_disjunctions`) must not drop granules matching a filter whose
-- negated comparison folds into a condition the key analysis rewrites. https://github.com/ClickHouse/ClickHouse/issues/128360

SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET allow_key_condition_coalesce_rewrite = 1;
-- Each query must run the index analysis instead of reusing the marks found by a previous one.
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_minmax;
DROP TABLE IF EXISTS t_set;

CREATE TABLE t_minmax
(
    id UInt32,
    b UInt32,
    c Nullable(UInt32),
    x UInt32,
    INDEX ib b TYPE minmax GRANULARITY 1,
    INDEX ic c TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_minmax SELECT number, 1, 500, number FROM numbers(16);

-- Only the row with x = 9 matches each filter below.
SELECT count() FROM t_minmax WHERE NOT (coalesce(c, b) > 10) OR x = 9 SETTINGS use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_minmax WHERE NOT (coalesce(c, b) > 10) OR x = 9 SETTINGS use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_minmax WHERE (NOT (coalesce(c, b) > 10) OR x = 9) AND x + 0 < 2000 AND x + 1 < 2000 AND x + 2 < 2000 AND x + 3 < 2000 AND x + 4 < 2000 AND x + 5 < 2000 AND x + 6 < 2000 AND x + 7 < 2000 AND x + 8 < 2000 AND x + 9 < 2000 AND x + 10 < 2000 AND x + 11 < 2000 AND x + 12 < 2000 AND x + 13 < 2000 AND x + 14 < 2000
SETTINGS use_skip_indexes_for_disjunctions_max_rpn_elements = 128;
SELECT count() FROM t_minmax WHERE NOT (ifNull(c, b) > 10) OR x = 9;
SELECT count() FROM t_minmax WHERE NOT (coalesce(c, b) != 7) OR x = 9;
SELECT count() FROM t_minmax WHERE NOT (ifNull(c < 10 OR b > 100, 0) = 0) OR x = 9;
SELECT count() FROM t_minmax WHERE NOT ((c < 10 OR b > 100) NOT IN (true)) OR x = 9;
SELECT count() FROM t_minmax WHERE NOT (nullIf(coalesce(c, b), toUInt32(0)) != toUInt32(7)) OR x = 9;
-- A comparison whose coalesce operand is wrapped in a function the key analysis strips is not negated, but folds the same way.
SELECT count() FROM t_minmax WHERE materialize(coalesce(c, b)) <= 10 OR x = 9;
SELECT count() FROM t_minmax WHERE CAST(coalesce(c, b), 'UInt32') <= 10 OR x = 9;
SELECT count() FROM t_minmax WHERE materialize(coalesce(c, b)) <=> 10 OR x = 9;
SELECT count() FROM t_minmax WHERE ifNull(c < 10 OR b > 100, materialize(0)) OR x = 9;
-- A comparison that a rewrite builds (a nullIf operand of coalesce, a nested nullIf) must get the same rewrites.
SELECT count() FROM t_minmax WHERE coalesce(c, nullIf(coalesce(c, b), toUInt32(0))) = toUInt32(7) OR x = 9;
SELECT count() FROM t_minmax WHERE nullIf(nullIf(coalesce(c, toNullable(b)), toNullable(toUInt32(0))), toNullable(toUInt32(1))) = toNullable(toUInt32(7)) OR x = 9;

CREATE TABLE t_set
(
    id UInt32,
    b UInt32,
    c Nullable(UInt32),
    x UInt32,
    INDEX ib b TYPE minmax GRANULARITY 1,
    INDEX ic c TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_set SELECT number, 1, 500, number FROM numbers(16);

SELECT count() FROM t_set WHERE NOT (coalesce(c, b) > 10) OR x = 9;

-- A subquery set must get the same rewrites in every pass, even after a set index condition has built it.
DROP TABLE IF EXISTS t_ready;
CREATE TABLE t_ready
(
    id UInt32,
    b UInt32,
    x UInt32,
    INDEX i_ready (b < 10 OR b > 100) TYPE set(0) GRANULARITY 1,
    INDEX i_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_ready SELECT number, 50, number FROM numbers(16);

SELECT count() FROM t_ready WHERE (b < 10 OR b > 100) IN (SELECT true) OR x = 9;
SELECT count() FROM t_ready WHERE NOT ((b < 10 OR b > 100) NOT IN (SELECT true)) OR x = 9;

-- Per-part constant folding can turn an operand into a constant; the folded filter must keep one RPN layout too.
DROP TABLE IF EXISTS t_fold;
CREATE TABLE t_fold
(
    p UInt32,
    id UInt32,
    b UInt32,
    c Nullable(UInt32),
    x UInt32,
    INDEX ib b TYPE minmax GRANULARITY 1,
    INDEX ic c TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_fold SELECT 10, number, 1, 500, number FROM numbers(16);

SELECT count() FROM t_fold WHERE (coalesce(c, b) < p OR x = 9) AND c > 0 AND b > 0 SETTINGS use_constant_folding_in_index_analysis = 1;
SELECT count() FROM t_fold WHERE (NOT (coalesce(c, b) >= p) OR x = 9) AND c > 0 AND b > 0 SETTINGS use_constant_folding_in_index_analysis = 1;

-- With the rewrite off, a negated ifNull range still reaches the monotonic chain over a Nullable partition key; both counts must match.
DROP TABLE IF EXISTS t_chain_part;
DROP TABLE IF EXISTS t_chain_flat;
CREATE TABLE t_chain_part (c0 Nullable(Int)) ENGINE = MergeTree PARTITION BY (c0) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
CREATE TABLE t_chain_flat (c0 Nullable(Int)) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_chain_part VALUES (NULL), (0), (1), (5), (NULL);
INSERT INTO t_chain_flat VALUES (NULL), (0), (1), (5), (NULL);
SELECT (SELECT count() FROM t_chain_flat WHERE NOT (ifNull(c0, 0) >= 7) SETTINGS allow_key_condition_coalesce_rewrite = 0),
       (SELECT count() FROM t_chain_part WHERE NOT (ifNull(c0, 0) >= 7) SETTINGS allow_key_condition_coalesce_rewrite = 0);

DROP TABLE t_minmax;
DROP TABLE t_set;
DROP TABLE t_ready;
DROP TABLE t_fold;
DROP TABLE t_chain_part;
DROP TABLE t_chain_flat;
