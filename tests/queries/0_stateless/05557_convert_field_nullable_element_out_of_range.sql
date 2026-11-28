-- A value that does not fit a `Nullable` element of an `Array`, `Tuple` or `Map` is rejected instead of becoming NULL.

DROP TABLE IF EXISTS t_drop_partition_nullable_element;
CREATE TABLE t_drop_partition_nullable_element (a Array(Nullable(UInt8)), v String)
ENGINE = MergeTree PARTITION BY a ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_drop_partition_nullable_element VALUES ([NULL], 'null'), ([44], 'p44'), ([1], 'p1');
ALTER TABLE t_drop_partition_nullable_element DROP PARTITION [300]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT a, v FROM t_drop_partition_nullable_element ORDER BY v;
ALTER TABLE t_drop_partition_nullable_element DROP PARTITION [NULL];
SELECT a, v FROM t_drop_partition_nullable_element ORDER BY v;
DROP TABLE t_drop_partition_nullable_element;

SET allow_suspicious_low_cardinality_types = 1;
SELECT * FROM values('a Array(Nullable(UInt8))', [300]); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Array(LowCardinality(Nullable(UInt8)))', [300]); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Tuple(Nullable(UInt8), String)', (300, 'x')); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Map(String, Nullable(UInt8))', map('k', 300)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Array(Array(Nullable(UInt8)))', [[1, 300]]); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Array(Nullable(Tuple(UInt8, String)))', [(300, 'x')]); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('a Variant(Array(Nullable(UInt8)), UInt64)', [300]); -- { serverError TYPE_MISMATCH }
SELECT * FROM values('a Array(Nullable(Nothing))', ['NULL']); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT groupArrayInsertAt([300]::Array(UInt16), 3)(x, 0) FROM (SELECT [1]::Array(Nullable(UInt8)) AS x); -- { serverError CANNOT_CONVERT_TYPE }

-- A NULL of the source stays NULL, and so does a string that a `Variant` reads as NULL.
SELECT * FROM values('a Array(Nullable(UInt8))', [NULL, 1]);
SELECT * FROM values('a Tuple(Nullable(UInt8), String)', (NULL, 'x'));
SELECT * FROM values('a Map(String, Nullable(UInt8))', map('k', NULL));
SELECT * FROM values('a Map(String, Variant(Array(UInt64), UInt64))', map('a', 'NULL'));
