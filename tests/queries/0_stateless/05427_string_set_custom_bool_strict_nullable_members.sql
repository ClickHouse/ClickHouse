-- A `Bool` wrapped in `SimpleAggregateFunction` keeps its custom serialization, so `CAST` to `String`
-- honors `bool_true_representation`; a set and `values` must produce the same text.
SET bool_true_representation = 'yes';
WITH CAST(true, 'SimpleAggregateFunction(any, Bool)') AS b
SELECT CAST(b, 'String'), 'yes' IN (b), 'true' IN (b);
SELECT x FROM values('x String', CAST(true, 'SimpleAggregateFunction(any, Bool)'));
WITH CAST([true], 'Array(SimpleAggregateFunction(any, Bool))') AS b
SELECT CAST(b, 'Array(String)'), ['yes'] IN (b), ['true'] IN (b);

-- A non-`NULL` member that is not representable in a `Nullable` target is not a `NULL` member.
SELECT CAST([NULL], 'Array(Nullable(UInt8))') IN ([256]);
SELECT CAST([NULL], 'Array(Nullable(UInt8))') IN (CAST([256], 'Array(Dynamic)'));
SELECT CAST([NULL], 'Array(Nullable(UInt8))') IN (CAST([256], 'Array(Variant(UInt16, String))'));
SELECT CAST(tuple(NULL), 'Tuple(Nullable(UInt8))') IN (CAST(tuple(256), 'Tuple(Dynamic)'));
SELECT CAST(map(1, NULL), 'Map(UInt8, Nullable(UInt8))') IN (CAST(map(1, 256), 'Map(UInt8, Dynamic)'));

-- A genuine `NULL` member remains representable.
SELECT CAST([NULL], 'Array(Nullable(UInt8))') IN (CAST([NULL], 'Array(Dynamic)'));
SELECT CAST([1], 'Array(Nullable(UInt8))') IN (CAST([1], 'Array(Dynamic)'));

DROP TABLE IF EXISTS t_strict_nullable_members;
CREATE TABLE t_strict_nullable_members (a Array(Nullable(UInt8))) ENGINE = Memory;
INSERT INTO t_strict_nullable_members VALUES ([NULL]), ([1]);
SELECT a FROM t_strict_nullable_members WHERE a IN (CAST([256], 'Array(Dynamic)')) ORDER BY a;
SELECT '--';
SELECT a FROM t_strict_nullable_members WHERE a NOT IN (CAST([256], 'Array(Dynamic)')) ORDER BY a;
DROP TABLE t_strict_nullable_members;
