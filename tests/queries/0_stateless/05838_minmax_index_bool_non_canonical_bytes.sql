-- A `Bool` column can store a byte other than 0 and 1, e.g. after `INSERT ... SELECT toUInt8(2)`. Some functions see
-- that byte (`toUInt8(b) = 2`, `b = true`), others see it converted to 1 (`b IN (true)`). A min-max index of the column,
-- implicit or explicit, must keep every granule that one of them matches.

SET use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_saf;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_multi_mark;
DROP TABLE IF EXISTS t_multi_mark_middle;

CREATE TABLE t_bool (k UInt8, b Bool, nb Nullable(Bool), INDEX i_nb nb TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_bool SELECT 1, toUInt8(2), toUInt8(2);
INSERT INTO t_bool VALUES (2, false, false), (3, true, true);

SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_bool' ORDER BY name;

SELECT 'stored byte';
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE toUInt8(b) = 2 ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE toUInt8(b) = 2 ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE b = true ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE b = true ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE toUInt8(nb) = 2 ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE toUInt8(nb) = 2 ORDER BY k) SETTINGS use_skip_indexes = 0;

SELECT 'converted';
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE b IN (true) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE b IN (true) ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE b ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE b ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE nb IN (true) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_bool WHERE nb IN (true) ORDER BY k) SETTINGS use_skip_indexes = 0;

SELECT 'top-k';
SELECT k FROM t_bool ORDER BY b DESC, k LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT k FROM t_bool ORDER BY b DESC, k LIMIT 1 SETTINGS use_skip_indexes = 0;

CREATE TABLE t_saf (k UInt8, m SimpleAggregateFunction(max, Bool))
ENGINE = MergeTree ORDER BY k SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_saf SELECT 1, toUInt8(2);
INSERT INTO t_saf VALUES (2, false);

SELECT 'SimpleAggregateFunction';
SELECT groupArray(k) FROM (SELECT k FROM t_saf WHERE toUInt8(m) = 2 ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_saf WHERE toUInt8(m) = 2 ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_saf WHERE m IN (true) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_saf WHERE m IN (true) ORDER BY k) SETTINGS use_skip_indexes = 0;

-- A `Bool` inside a `Tuple`, an `Array` or a `Map` stores the same bytes.
CREATE TABLE t_nested
(
    k UInt8,
    t Tuple(Bool),
    a Array(Bool),
    m Map(String, Bool),
    n Tuple(x Nullable(Bool), y UInt8),
    INDEX i_t t TYPE minmax GRANULARITY 1,
    INDEX i_a a TYPE minmax GRANULARITY 1,
    INDEX i_m m TYPE minmax GRANULARITY 1,
    INDEX i_n n TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, auto_statistics_types = '';
-- The `Tuple` for `n` has the names and the `Nullable` of the column: otherwise it is converted element by element,
-- which turns the byte into 1.
INSERT INTO t_nested SELECT 1, tuple(toUInt8(2)), [toUInt8(2)], map('k', toUInt8(2)),
    CAST((toNullable(toUInt8(2)), toUInt8(5)), 'Tuple(x Nullable(UInt8), y UInt8)');
INSERT INTO t_nested VALUES (2, (false), [false], {'k': false}, (false, 5)), (3, (true), [true], {'k': true}, (true, 0));

SELECT 'nested, stored byte';
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE t = tuple(toUInt8(2)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE t = tuple(toUInt8(2)) ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE t > tuple(true) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE t > tuple(true) ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE a = [toUInt8(2)] ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE a = [toUInt8(2)] ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE m = map('k', toUInt8(2)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE m = map('k', toUInt8(2)) ORDER BY k) SETTINGS use_skip_indexes = 0;

SELECT 'nested, bytes';
SELECT k, toUInt8(t.1), arrayMap(x -> toUInt8(x), a), toUInt8(m['k']), toUInt8(n.x) FROM t_nested ORDER BY k;

SELECT 'nested, converted';
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE n IN ((true, 5), (true, 0)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE n IN ((true, 5), (true, 0)) ORDER BY k) SETTINGS use_skip_indexes = 0;

SELECT 'nested, canonical';
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE t = tuple(true) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE t = tuple(true) ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE a = [true] ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE a = [true] ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE m = map('k', true) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_nested WHERE m = map('k', true) ORDER BY k) SETTINGS use_skip_indexes = 0;

SELECT 'nested, top-k';
SELECT k FROM t_nested ORDER BY t DESC, k LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT k FROM t_nested ORDER BY t DESC, k LIMIT 1 SETTINGS use_skip_indexes = 0;

-- A granule of the index can span several marks. Its bounds are then merged from the bounds of the marks, so the
-- row with the byte need not be the lower bound: `(1, 5)` and `(2, 0)` give `[(1, 5), (2, 0)]`, and `(2, 0)`
-- converted is `(1, 0)`, below `(1, 5)`. In `t_multi_mark_middle` neither bound has the byte: the bounds are
-- `[(0, 1, 5), (3, 0, 0)]`, and `(0, 2, 0)` converted is `(0, 1, 0)`.
CREATE TABLE t_multi_mark (k UInt8, n Tuple(Nullable(Bool), UInt8), INDEX i_n n TYPE minmax GRANULARITY 2)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_multi_mark
SELECT number + 1, tuple(toNullable(toUInt8(number + 1)), toUInt8(if(number = 0, 5, 0))) FROM numbers(2);

CREATE TABLE t_multi_mark_middle (k UInt8, n Tuple(UInt8, Bool, UInt8), INDEX i_n n TYPE minmax GRANULARITY 3)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_multi_mark_middle
SELECT number + 1, tuple(toUInt8([0, 0, 3][number + 1]), toUInt8([1, 2, 0][number + 1]), toUInt8([5, 0, 0][number + 1])) FROM numbers(3);

SELECT 'several marks, bytes';
SELECT k, toUInt8(n.1), n.2 FROM t_multi_mark ORDER BY k;
SELECT k, n.1, toUInt8(n.2), n.3 FROM t_multi_mark_middle ORDER BY k;

SELECT 'several marks, converted';
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark WHERE n IN ((true, 0), (false, 0)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark WHERE n IN ((true, 0), (false, 0)) ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark_middle WHERE n IN ((0, true, 0), (0, false, 0)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark_middle WHERE n IN ((0, true, 0), (0, false, 0)) ORDER BY k) SETTINGS use_skip_indexes = 0;

SELECT 'several marks, stored byte';
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark WHERE n = tuple(toNullable(toUInt8(2)), toUInt8(0)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark WHERE n = tuple(toNullable(toUInt8(2)), toUInt8(0)) ORDER BY k) SETTINGS use_skip_indexes = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark_middle WHERE n = tuple(toUInt8(0), toUInt8(2), toUInt8(0)) ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_multi_mark_middle WHERE n = tuple(toUInt8(0), toUInt8(2), toUInt8(0)) ORDER BY k) SETTINGS use_skip_indexes = 0;

-- The upper bound is not changed, so values above it are still skipped.
SELECT 'several marks, skipped';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM t_multi_mark_middle WHERE n IN ((4, true, 0), (5, false, 0))) WHERE explain LIKE '%Granules: _/_%';

DROP TABLE t_bool;
DROP TABLE t_saf;
DROP TABLE t_nested;
DROP TABLE t_multi_mark;
DROP TABLE t_multi_mark_middle;
