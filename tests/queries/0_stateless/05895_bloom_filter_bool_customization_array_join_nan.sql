-- A `bloom_filter` index must not drop rows that the same filter selects without the index.
-- Each query is run with `use_skip_indexes = 0` (the oracle) and then with the index.

SET optimize_functions_to_subcolumns = 1;

SELECT 'A set of SimpleAggregateFunction(any, Bool) over an integer index';
-- `Set` converts the left-hand side to the type of the set, which collapses every non-zero number
-- to `true` for this customization of `UInt8` just as for `Bool`, so both rows match.

DROP TABLE IF EXISTS t_bf_saf_bool_set;
CREATE TABLE t_bf_saf_bool_set
(
    v UInt16,
    a Array(UInt16),
    m Map(String, UInt16),
    INDEX bf v TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX bf_a a TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX bf_m mapValues(m) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_saf_bool_set VALUES (1, [1], {'k': 1}), (2, [2], {'k': 2});

SELECT count() FROM t_bf_saf_bool_set WHERE v IN (SELECT CAST(true AS SimpleAggregateFunction(any, Bool))) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_saf_bool_set WHERE v IN (SELECT CAST(true AS SimpleAggregateFunction(any, Bool)));

SELECT count() FROM t_bf_saf_bool_set WHERE m['k'] IN (SELECT CAST(true AS SimpleAggregateFunction(any, Bool))) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_saf_bool_set WHERE m['k'] IN (SELECT CAST(true AS SimpleAggregateFunction(any, Bool)));

SELECT count() FROM t_bf_saf_bool_set WHERE arrayJoin(a) IN (SELECT CAST(true AS SimpleAggregateFunction(any, Bool))) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_saf_bool_set WHERE arrayJoin(a) IN (SELECT CAST(true AS SimpleAggregateFunction(any, Bool)));

DROP TABLE t_bf_saf_bool_set;

SELECT 'An integer constant with an index of SimpleAggregateFunction(any, Bool)';
-- The column can hold a byte other than 0 and 1, and `equals` compares it numerically.

DROP TABLE IF EXISTS t_bf_saf_bool;
CREATE TABLE t_bf_saf_bool
(
    k UInt8,
    b SimpleAggregateFunction(any, Bool),
    INDEX bf b TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = AggregatingMergeTree ORDER BY k SETTINGS index_granularity = 1;

INSERT INTO t_bf_saf_bool SELECT number, reinterpret(toUInt8(number), 'Bool') FROM numbers(1, 2);

SELECT count() FROM t_bf_saf_bool WHERE b = toInt64(2) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_saf_bool WHERE b = toInt64(2);
SELECT count() FROM t_bf_saf_bool WHERE b = 300 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_saf_bool WHERE b = 300;

DROP TABLE t_bf_saf_bool;

SELECT 'arrayJoin of an Array(Bool) with an integer constant';

DROP TABLE IF EXISTS t_bf_bool_array_join;
CREATE TABLE t_bf_bool_array_join
(
    a Array(Bool),
    INDEX bf a TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_bool_array_join SELECT [reinterpret(toUInt8(2), 'Bool')];
INSERT INTO t_bf_bool_array_join SELECT [true];

SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = toInt64(2) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = toInt64(2);
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = 1;
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = true SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = true;
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = 300 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_bool_array_join WHERE arrayJoin(a) = 300;

DROP TABLE t_bf_bool_array_join;

SELECT 'DateTime with a non-finite Float64 constant';
-- No stored value equals it, and it must not be converted to an integer.

DROP TABLE IF EXISTS t_bf_dt_nan;
CREATE TABLE t_bf_dt_nan
(
    d DateTime('UTC'),
    m Map(String, DateTime('UTC')),
    INDEX bf d TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX bf_m mapValues(m) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_dt_nan SELECT toDateTime(number, 'UTC'), map('k', toDateTime(number, 'UTC')) FROM numbers(3);

SELECT count() FROM t_bf_dt_nan WHERE d = nan SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt_nan WHERE d = nan;
SELECT count() FROM t_bf_dt_nan WHERE d = inf SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt_nan WHERE d = inf;
SELECT count() FROM t_bf_dt_nan WHERE d = -inf SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt_nan WHERE d = -inf;
SELECT count() FROM t_bf_dt_nan WHERE m['k'] = nan SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt_nan WHERE m['k'] = nan;
SELECT count() FROM t_bf_dt_nan WHERE m['k'] = nan SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_dt_nan WHERE m['absent'] = nan SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt_nan WHERE m['absent'] = nan;
SELECT count() FROM t_bf_dt_nan WHERE d = toFloat64(2) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_dt_nan WHERE d = toFloat64(2);

DROP TABLE t_bf_dt_nan;
