-- `-Tuple` and `-Distinct` over `Variant` arguments that the nested function does not support natively.
-- The results must be the same as for the arguments explicitly cast to `Nullable` of the common supertype.

SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_tuple_distinct;
CREATE TABLE t_variant_tuple_distinct (n UInt64, v Variant(UInt8, UInt32), w Variant(Float64, Int32)) ENGINE = Memory;
INSERT INTO t_variant_tuple_distinct SELECT number, if(number % 3 = 0, NULL, if(number % 2 = 0, (number % 7)::UInt8, (number % 11)::UInt32)), if(number % 5 = 0, number::Int32, number / 4) FROM numbers(100);

SELECT 'Tuple';
SELECT sumTuple(tuple(v, w)), sumTuple(tuple(v::Nullable(UInt32), w::Nullable(Float64))) FROM t_variant_tuple_distinct;
SELECT toTypeName(sumTuple(tuple(v, w))) FROM t_variant_tuple_distinct;
SELECT sumTuple(tuple(v AS a, n AS b)) FROM t_variant_tuple_distinct;
SELECT corrTuple(tuple(v, n), tuple(w, w)) = corrTuple(tuple(v::Nullable(UInt32), n), tuple(w::Nullable(Float64), w::Nullable(Float64))) FROM t_variant_tuple_distinct;
SELECT sumTupleIf(tuple(v), n % 2 = 0), sumTupleIf(tuple(v::Nullable(UInt32)), n % 2 = 0) FROM t_variant_tuple_distinct;
SELECT n % 3 AS k, sumTuple(tuple(v)), sumTuple(tuple(v::Nullable(UInt32))) FROM t_variant_tuple_distinct GROUP BY k ORDER BY k;
SELECT finalizeAggregation(sumTupleState(tuple(v))) FROM t_variant_tuple_distinct;

SELECT 'Distinct';
SELECT sumDistinct(v), sumDistinct(v::Nullable(UInt32)), uniqExact(v) FROM t_variant_tuple_distinct;
SELECT sumDistinctIf(v, n % 2 = 0), sumDistinctIf(v::Nullable(UInt32), n % 2 = 0) FROM t_variant_tuple_distinct;
SELECT avgWeightedDistinct(v, w), avgWeightedDistinct(v::Nullable(UInt32), w::Nullable(Float64)) FROM t_variant_tuple_distinct;
SELECT n % 4 AS k, sumDistinct(v), sumDistinct(v::Nullable(UInt32)) FROM t_variant_tuple_distinct GROUP BY k ORDER BY k;
-- Every value is new, the rows are passed to the nested function as is.
SELECT sumDistinct(number::Variant(UInt64)), sumDistinct(number) FROM numbers(1000);
-- Equal values of different alternatives are distinct.
SELECT sumDistinct(v) FROM (SELECT arrayJoin([1::UInt8::Variant(UInt8, UInt16), 1::UInt16::Variant(UInt8, UInt16), 1::UInt8::Variant(UInt8, UInt16)]) AS v);

DROP TABLE t_variant_tuple_distinct;
