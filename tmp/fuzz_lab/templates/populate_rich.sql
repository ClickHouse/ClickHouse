-- Rich seed schema for the AST-fuzzer oracle. Targets the code paths that
-- produced 6 of the 8 campaign bugs but were absent from the plain-MergeTree
-- _s1.._s4 base tables: skip indexes, projections, aggregating/replacing/
-- collapsing engines + FINAL, the storage-Join engine, and edge-case data
-- (NaN/Inf/NULL/duplicate keys/binary FixedString). Recreated every batch
-- (deterministic), so the curated oracle-shaped queries below and every
-- fuzzed mutant of them exercise these paths on every instance.
--
-- Naming convention: rich tables are prefixed `_r_` so they never collide
-- with the `_s1.._s4` base set or stateless-test table names.

-- ============================================================
-- Skip indexes over numeric data including NaN / Inf / NULL.
-- (bug #6 class: minmax pruning under negated float ranges)
-- ============================================================
DROP TABLE IF EXISTS _r_minmax;
CREATE TABLE _r_minmax
(
    id UInt64,
    f Float64,
    fn Nullable(Float64),
    i Int64,
    INDEX idx_f  f  TYPE minmax GRANULARITY 1,
    INDEX idx_fn fn TYPE minmax GRANULARITY 1,
    INDEX idx_i  i  TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO _r_minmax SELECT number, number * 0.5 - 25, if(number % 4 = 0, NULL, number * 0.5 - 25), number - 100 FROM numbers(120);
INSERT INTO _r_minmax VALUES (1000, nan, nan, 0), (1001, nan, NULL, 0), (1002, inf, inf, 9223372036854775807), (1003, -inf, -inf, -9223372036854775807), (1004, 0, -0.0, 0);

-- ============================================================
-- set / bloom_filter / tokenbf / ngrambf skip indexes over strings + arrays.
-- (bug #5 class: index-vs-runtime equality disagreements)
-- ============================================================
DROP TABLE IF EXISTS _r_skip_str;
CREATE TABLE _r_skip_str
(
    id UInt64,
    s  String,
    sn Nullable(String),
    fs FixedString(8),
    arr Array(UInt32),
    INDEX idx_set  s  TYPE set(50)              GRANULARITY 1,
    INDEX idx_bf   s  TYPE bloom_filter          GRANULARITY 1,
    INDEX idx_tok  s  TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1,
    INDEX idx_ngr  s  TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1,
    INDEX idx_arr  arr TYPE bloom_filter         GRANULARITY 1
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO _r_skip_str SELECT
    number,
    'tok' || toString(number % 13) || ' word' || toString(number % 7),
    if(number % 5 = 0, NULL, 'n' || toString(number % 9)),
    toFixedString(toString(number % 1000), 8),
    range(number % 6)
FROM numbers(80);
INSERT INTO _r_skip_str VALUES (900, '', NULL, toFixedString('', 8), []);

-- ============================================================
-- Map with bloom_filter on mapKeys/mapValues (bug #5 exact class).
-- ============================================================
DROP TABLE IF EXISTS _r_map;
CREATE TABLE _r_map
(
    id UInt64,
    m  Map(String, String),
    mi Map(String, UInt32),
    INDEX idx_keys mapKeys(m)   TYPE bloom_filter GRANULARITY 1,
    INDEX idx_vals mapValues(m) TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO _r_map SELECT
    number,
    map('k' || toString(number % 4), 'v' || toString(number % 3)),
    map('a', number, 'b', number * 2)
FROM numbers(40);

-- ============================================================
-- text index on Nullable(String) (bug #3 class: NULL semantics under
-- direct read, hasToken / hasPhrase / hasAllTokens / hasAnyTokens).
-- ============================================================
DROP TABLE IF EXISTS _r_text;
CREATE TABLE _r_text
(
    id UInt64,
    t  Nullable(String),
    INDEX idx_t t TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO _r_text VALUES
    (1, 'hello world foo'), (2, NULL), (3, 'foo bar baz'), (4, NULL),
    (5, 'hello there world'), (6, 'baz qux'), (7, NULL), (8, 'world hello');

-- ============================================================
-- AggregatingMergeTree + SimpleAggregateFunction, dup keys → FINAL.
-- (bug #2 / distinct-cube + the FINAL skip-index gate class)
-- ============================================================
DROP TABLE IF EXISTS _r_agg;
CREATE TABLE _r_agg
(
    key UInt32,
    mx SimpleAggregateFunction(max, Int64),
    mn SimpleAggregateFunction(min, Int64),
    INDEX idx_mx mx TYPE minmax GRANULARITY 1
) ENGINE = AggregatingMergeTree ORDER BY key PARTITION BY key % 4;
INSERT INTO _r_agg SELECT number % 10, number, -number FROM numbers(50);
INSERT INTO _r_agg SELECT number % 10, number + 1, -(number + 1) FROM numbers(50);

-- ============================================================
-- ReplacingMergeTree with a version column, duplicate keys → FINAL dedup.
-- ============================================================
DROP TABLE IF EXISTS _r_repl;
CREATE TABLE _r_repl (key UInt32, v UInt32, val Nullable(String))
ENGINE = ReplacingMergeTree(v) ORDER BY key;
INSERT INTO _r_repl SELECT number % 8, 1, toString(number) FROM numbers(20);
INSERT INTO _r_repl SELECT number % 8, 2, if(number % 3 = 0, NULL, toString(number * 10)) FROM numbers(20);

-- ============================================================
-- SummingMergeTree and CollapsingMergeTree (sign-based) → FINAL.
-- ============================================================
DROP TABLE IF EXISTS _r_sum;
CREATE TABLE _r_sum (key UInt32, s UInt64) ENGINE = SummingMergeTree ORDER BY key;
INSERT INTO _r_sum SELECT number % 6, number FROM numbers(60);

DROP TABLE IF EXISTS _r_collapse;
CREATE TABLE _r_collapse (key UInt32, v UInt32, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY key;
INSERT INTO _r_collapse SELECT number % 5, number, 1 FROM numbers(30);
INSERT INTO _r_collapse SELECT number % 5, number, -1 FROM numbers(15);

-- ============================================================
-- Storage Join engine (bug #7 class: RIGHT JOIN + QUALIFY).
-- ============================================================
DROP TABLE IF EXISTS _r_join;
CREATE TABLE _r_join (k1 Int64, k2 UInt64, a UInt64, b Nullable(UInt64))
ENGINE = Join(ALL, RIGHT, k1, k2);
INSERT INTO _r_join VALUES (-1, 1, 10, 100), (-2, 2, 20, NULL), (-3, 3, 30, 300), (-6, 6, 60, 600);

DROP TABLE IF EXISTS _r_left;
CREATE TABLE _r_left (k1 Int64, k2 UInt64, val UInt64) ENGINE = MergeTree ORDER BY k1;
INSERT INTO _r_left VALUES (-1, 1, 11), (-2, 2, 22), (-3, 3, 33), (-4, 4, 44);

-- ============================================================
-- Table with a projection (projection-vs-base consistency).
-- ============================================================
DROP TABLE IF EXISTS _r_proj;
CREATE TABLE _r_proj
(
    id UInt64,
    g  UInt8,
    v  Int64,
    PROJECTION p_by_g (SELECT g, sum(v), count() GROUP BY g)
) ENGINE = MergeTree ORDER BY id;
INSERT INTO _r_proj SELECT number, number % 7, number * 3 - 100 FROM numbers(100);
