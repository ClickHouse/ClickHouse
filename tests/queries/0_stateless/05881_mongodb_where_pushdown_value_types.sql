-- Tags: no-fasttest
-- Tag no-fasttest: the MongoDB storage is not built in the Fast test build.

-- WHERE pushdown turns every constant of a comparison into a BSON value while the query is still being
-- built, before the collection is contacted, so a dummy host is enough.
-- Expected behaviour: the "Supported clauses" section of the MongoDB engine documentation block in
-- src/Storages/StorageMongoDB.cpp -- a simple `field = <constant>` comparison is translated, anything
-- else makes the storage refuse the query with NOT_IMPLEMENTED.
-- A conjunction is translated argument by argument and stops at the first argument it cannot take, so a
-- trailing `toString(x) = 'a'` makes the statement fail only after every earlier constant was converted.

-- One conjunct per integer / float / date / tuple constant type. The casts keep each constant at its
-- column's own type, otherwise the comparison is rewritten before the storage sees it.
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', 'c',
    'x Int32, u16 UInt16, u32 UInt32, u64 UInt64, i8 Int8, i16 Int16, i64 Int64, f32 Float32, d32 Date32, t Tuple(Int32, String)')
WHERE u16 = 7::UInt16 AND u32 = 8::UInt32 AND u64 = 9::UInt64
  AND i8 = -1::Int8 AND i16 = -2::Int16 AND i64 = -3::Int64
  AND f32 = 1.5::Float32 AND d32 = '2020-01-01'::Date32 AND t = (1, 'a')
  AND toString(x) = 'a'
SETTINGS mongodb_throw_on_unsupported_query = 1; -- { serverError NOT_IMPLEMENTED }

-- A NULL constant becomes the BSON null value, both on its own and as the member of an IN list.
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', 'c', 'x Int32, n Nullable(Int32)')
WHERE n = CAST(NULL, 'Nullable(Int32)') AND n IN (CAST(NULL, 'Nullable(Int32)'))
  AND toString(x) = 'a'
SETTINGS mongodb_throw_on_unsupported_query = 1; -- { serverError NOT_IMPLEMENTED }

-- A constant whose type has no BSON counterpart is refused by the conversion itself, so it is reported
-- even when the restriction on unsupported queries is switched off.
SELECT * FROM mongodb('mongodb://some-cluster:27017/db', 'c', 'dec Decimal32(2)')
WHERE dec = 1.5::Decimal32(2)
SETTINGS mongodb_throw_on_unsupported_query = 1; -- { serverError NOT_IMPLEMENTED }

SELECT * FROM mongodb('mongodb://some-cluster:27017/db', 'c', 'dec Decimal32(2)')
WHERE dec = 1.5::Decimal32(2)
SETTINGS mongodb_throw_on_unsupported_query = 0; -- { serverError NOT_IMPLEMENTED }
