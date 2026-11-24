-- Tags: no-replicated-database
-- no-replicated-database: a refreshable view with a non-replicated target is refused on a Replicated database

-- A `quantile(0.9)` state stored in or cast to a declared `quantile(0.5)` type finalizes as the median, 2.5, not 70.9.

DROP TABLE IF EXISTS t_top;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_array_tuple;
DROP TABLE IF EXISTS t_quantiles;
DROP TABLE IF EXISTS t_states;
DROP TABLE IF EXISTS t_merge_tree;

CREATE TABLE t_top (s AggregateFunction(quantile(0.5), UInt8)) ENGINE = Memory;
CREATE TABLE t_tuple (s Tuple(AggregateFunction(quantile(0.5), UInt8))) ENGINE = Memory;
CREATE TABLE t_array (s Array(AggregateFunction(quantile(0.5), UInt8))) ENGINE = Memory;
CREATE TABLE t_map (s Map(String, AggregateFunction(quantile(0.5), UInt8))) ENGINE = Memory;
CREATE TABLE t_array_tuple (s Array(Tuple(AggregateFunction(quantile(0.5), UInt8)))) ENGINE = Memory;
CREATE TABLE t_quantiles (s AggregateFunction(quantile, UInt8)) ENGINE = Memory;
CREATE TABLE t_states (s AggregateFunction(groupArray, AggregateFunction(quantile(0.5), UInt8))) ENGINE = Memory;
CREATE TABLE t_merge_tree (s AggregateFunction(quantile(0.5), UInt8)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_top SELECT arrayReduce('quantileState(0.9)', [1, 2, 3, 100]);
INSERT INTO t_tuple SELECT tuple(arrayReduce('quantileState(0.9)', [1, 2, 3, 100]));
INSERT INTO t_array SELECT [arrayReduce('quantileState(0.9)', [1, 2, 3, 100])];
INSERT INTO t_map SELECT map('a', arrayReduce('quantileState(0.9)', [1, 2, 3, 100]));
INSERT INTO t_array_tuple SELECT [tuple(arrayReduce('quantileState(0.9)', [1, 2, 3, 100]))];
INSERT INTO t_quantiles SELECT arrayReduce('quantilesState(0.9, 0.1)', [1, 2, 3, 100]);
INSERT INTO t_states SELECT arrayReduce('groupArrayState', [arrayReduce('quantileState(0.9)', [1, 2, 3, 100])]);
INSERT INTO t_merge_tree SELECT arrayReduce('quantileState(0.9)', [1, 2, 3, 100]);

SELECT 'insert', finalizeAggregation(s) FROM t_top;
SELECT 'insert tuple', finalizeAggregation(tupleElement(s, 1)) FROM t_tuple;
SELECT 'insert array', finalizeAggregation(s[1]) FROM t_array;
SELECT 'insert map', finalizeAggregation(s['a']) FROM t_map;
SELECT 'insert array of tuple', finalizeAggregation(tupleElement(s[1], 1)) FROM t_array_tuple;
SELECT 'insert quantiles state', finalizeAggregation(s) FROM t_quantiles;
SELECT 'insert state of states', arrayMap(x -> finalizeAggregation(x), finalizeAggregation(s)) FROM t_states;
SELECT 'insert merge tree', finalizeAggregation(s) FROM t_merge_tree;
SELECT 'merge with the source level', quantileMerge(0.9)(s) FROM t_top;

WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast', finalizeAggregation(CAST(state, 'AggregateFunction(quantile(0.5), UInt8)'));
WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast tuple', finalizeAggregation(tupleElement(CAST(tuple(state), 'Tuple(AggregateFunction(quantile(0.5), UInt8))'), 1));
WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast named tuple', finalizeAggregation(tupleElement(
    CAST(tuple(state, 1)::Tuple(a AggregateFunction(quantile(0.9), UInt8), b UInt8), 'Tuple(a AggregateFunction(quantile(0.5), UInt8), b UInt8)'), 'a'));
WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast array', finalizeAggregation(CAST([state], 'Array(AggregateFunction(quantile(0.5), UInt8))')[1]);
WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast map', finalizeAggregation(CAST(map('a', state), 'Map(String, AggregateFunction(quantile(0.5), UInt8))')['a']);
WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast array of tuple', finalizeAggregation(tupleElement(CAST([tuple(state)], 'Array(Tuple(AggregateFunction(quantile(0.5), UInt8)))')[1], 1));
WITH arrayReduce('quantileState(0.9)', [1, 2, 3, 100]) AS state
SELECT 'cast state of states', arrayMap(x -> finalizeAggregation(x),
    finalizeAggregation(CAST(arrayReduce('groupArrayState', [state]), 'AggregateFunction(groupArray, AggregateFunction(quantile(0.5), UInt8))')));

-- A nested state of another version keeps its own function: its bytes were written with that version.
SELECT 'nested state of another version', uniqMerge(y) FROM (
    SELECT arrayJoin(finalizeAggregation(CAST(g, 'AggregateFunction(groupArray, AggregateFunction(1, uniq, UInt64))'))) AS y
    FROM (SELECT groupArrayState(u) AS g FROM (SELECT uniqState(number)::AggregateFunction(0, uniq, UInt64) AS u FROM numbers(100000) GROUP BY number % 3)));

DROP TABLE IF EXISTS t_variant_src;
DROP TABLE IF EXISTS t_variant;
CREATE TABLE t_variant_src (v Variant(AggregateFunction(quantile(0.9), UInt8), String)) ENGINE = Memory;
CREATE TABLE t_variant (v Variant(AggregateFunction(quantile(0.5), UInt8), String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_src SELECT arrayReduce('quantileState(0.9)', [1, 2, 3, 100])::Variant(AggregateFunction(quantile(0.9), UInt8), String);
-- A `Variant` holding such a state is kept as it is: `CAST` matches its alternatives by name.
INSERT INTO t_variant SELECT v FROM t_variant_src;
SELECT 'insert variant merge tree', finalizeAggregation(variantElement(v, 'AggregateFunction(quantile(0.5), UInt8)')) FROM t_variant;
SELECT 'cast tuple of variant', variantType(tupleElement(CAST(tuple(v), 'Tuple(Variant(AggregateFunction(quantile(0.5), UInt8), String))'), 1)) FROM t_variant_src;

DROP TABLE IF EXISTS t_version;
CREATE TABLE t_version (s AggregateFunction(1, uniq, UInt64)) ENGINE = Memory;
INSERT INTO t_version SELECT uniqState(number)::AggregateFunction(0, uniq, UInt64) FROM numbers(10);
SELECT 'insert state of another version', uniqMerge(y) FROM (SELECT arrayJoin(groupArray(s)) AS y FROM t_version);

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS rmv;
DROP TABLE IF EXISTS mv_push;
DROP TABLE IF EXISTS rmv_push;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_values;
DROP TABLE IF EXISTS t_mv;
DROP TABLE IF EXISTS t_rmv;
DROP TABLE IF EXISTS t_push_src;
DROP TABLE IF EXISTS t_push_dst;
DROP TABLE IF EXISTS mv_direct;
DROP TABLE IF EXISTS t_mv_direct;

CREATE TABLE t_null (x UInt8) ENGINE = Null;
CREATE TABLE t_values (x UInt8) ENGINE = Memory;
CREATE TABLE t_mv (s AggregateFunction(quantile(0.5), UInt8)) ENGINE = Memory;
CREATE TABLE t_rmv (s AggregateFunction(quantile(0.5), UInt8)) ENGINE = Memory;
CREATE TABLE t_push_src (x UInt8) ENGINE = Null;
CREATE TABLE t_push_dst (s AggregateFunction(quantile(0.5), UInt8)) ENGINE = Memory;
CREATE TABLE t_mv_direct (s AggregateFunction(quantile(0.5), UInt8)) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv TO t_mv AS SELECT quantileState(0.9)(x) AS s FROM t_null;
CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR APPEND TO t_rmv EMPTY AS SELECT quantileState(0.9)(x) AS s FROM t_values;
CREATE MATERIALIZED VIEW mv_push TO t_push_dst AS SELECT quantileState(0.9)(x) AS s FROM t_push_src;
CREATE MATERIALIZED VIEW rmv_push REFRESH EVERY 1 YEAR APPEND TO t_push_src EMPTY AS SELECT x FROM t_values;

INSERT INTO t_null VALUES (1), (2), (3), (100);
INSERT INTO t_values VALUES (1), (2), (3), (100);
SYSTEM REFRESH VIEW rmv;
SYSTEM WAIT VIEW rmv;
SYSTEM REFRESH VIEW rmv_push;
SYSTEM WAIT VIEW rmv_push;

CREATE MATERIALIZED VIEW mv_direct TO t_mv_direct AS SELECT quantileState(0.9)(x) AS s FROM t_null;
INSERT INTO mv_direct SELECT arrayReduce('quantileState(0.9)', [1, 2, 3, 100]);

SELECT 'materialized view', finalizeAggregation(s) FROM t_mv;
SELECT 'refreshable materialized view', finalizeAggregation(s) FROM t_rmv;
SELECT 'refreshable view into the source of a materialized view', finalizeAggregation(s) FROM t_push_dst;
SELECT 'insert into a materialized view', finalizeAggregation(s) FROM t_mv_direct;

DROP TABLE mv_direct;
DROP TABLE rmv_push;
DROP TABLE mv_push;
DROP TABLE rmv;
DROP TABLE mv;
DROP TABLE t_mv_direct;
DROP TABLE t_push_dst;
DROP TABLE t_push_src;
DROP TABLE t_rmv;
DROP TABLE t_mv;
DROP TABLE t_values;
DROP TABLE t_null;
DROP TABLE t_version;
DROP TABLE t_variant;
DROP TABLE t_variant_src;
DROP TABLE t_merge_tree;
DROP TABLE t_states;
DROP TABLE t_quantiles;
DROP TABLE t_array_tuple;
DROP TABLE t_map;
DROP TABLE t_array;
DROP TABLE t_tuple;
DROP TABLE t_top;
