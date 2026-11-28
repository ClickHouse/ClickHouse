-- `-Distinct` nested under `-Tuple` over `Variant` arguments that the nested function does not support natively.
-- `-Tuple` passes the whole batch to the nested functions, so the new distinct rows are converted together.
-- The results must be the same as for the arguments explicitly cast to `Nullable` of the common supertype.

SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_distinct_tuple;
CREATE TABLE t_variant_distinct_tuple (n UInt64, v Variant(UInt8, UInt32), w Variant(Float64, Int32)) ENGINE = Memory;
INSERT INTO t_variant_distinct_tuple SELECT number, if(number % 3 = 0, NULL, if(number % 2 = 0, (number % 7)::UInt8, (number % 11)::UInt32)), if(number % 5 = 0, number::Int32, number / 4) FROM numbers(100);

SELECT sumDistinctTuple(tuple(v, w)), sumDistinctTuple(tuple(v::Nullable(UInt32), w::Nullable(Float64))) FROM t_variant_distinct_tuple;
SELECT sumDistinctTupleIf(tuple(v), n % 2 = 0), sumDistinctTupleIf(tuple(v::Nullable(UInt32)), n % 2 = 0) FROM t_variant_distinct_tuple;
SELECT sumDistinctTuple(if(n % 4 = 0, NULL, tuple(v))), sumDistinctTuple(if(n % 4 = 0, NULL, tuple(v::Nullable(UInt32)))) FROM t_variant_distinct_tuple;

SELECT 'Grouped, UInt8 key';
SELECT (n % 4)::UInt8 AS k, sumDistinctTuple(tuple(v, w)), sumDistinctTuple(tuple(v::Nullable(UInt32), w::Nullable(Float64))) FROM t_variant_distinct_tuple GROUP BY k ORDER BY k;
SELECT 'Grouped, UInt64 key';
SELECT n % 4 AS k, sumDistinctTuple(tuple(v, w)), sumDistinctTuple(tuple(v::Nullable(UInt32), w::Nullable(Float64))) FROM t_variant_distinct_tuple GROUP BY k ORDER BY k;

SELECT 'Large';
SELECT sumDistinctTuple(tuple(number::Variant(UInt64))) = sumDistinctTuple(tuple(number::Variant(UInt64)::Nullable(UInt64))) FROM numbers(100000);
SELECT sum(t.1) FROM (SELECT sumDistinctTuple(tuple(number::Variant(UInt64))) AS t FROM numbers(100000) GROUP BY number::UInt8);

DROP TABLE t_variant_distinct_tuple;

SELECT 'Sparse';
DROP TABLE IF EXISTS t_tuple_sparse;
CREATE TABLE t_tuple_sparse (k UInt8, t Tuple(a UInt64, b UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_tuple_sparse SELECT number % 3, (if(number % 10 = 0, number, 0), number) FROM numbers(1000);
SELECT sumTuple(t), sumDistinctTuple(t) FROM t_tuple_sparse;
SELECT k, sumTuple(t), sumDistinctTuple(t) FROM t_tuple_sparse GROUP BY k ORDER BY k;
DROP TABLE t_tuple_sparse;
