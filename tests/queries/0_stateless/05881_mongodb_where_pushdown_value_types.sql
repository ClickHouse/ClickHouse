-- Tags: no-fasttest
-- Tag no-fasttest: the MongoDB storage is not built in the Fast test build.

-- WHERE pushdown turns every constant of a comparison into a BSON value while the query is still being
-- built, before the collection is contacted.
-- Expected behaviour: the "Supported clauses" section of the MongoDB engine documentation block in
-- src/Storages/StorageMongoDB.cpp -- a simple `field = <constant>` comparison is translated, anything
-- else makes the storage refuse the query with NOT_IMPLEMENTED.
-- The two outcomes have different error codes, so a conversion that stopped working cannot pass for one
-- that worked:
--   * every constant converted: the storage builds the MongoDB query and only then tries to reach the
--     unreachable host 127.0.0.1:1, which ends the statement in STD_EXCEPTION;
--   * a constant of a type that has no BSON counterpart: the conversion itself refuses the value with
--     NOT_IMPLEMENTED, with or without `mongodb_throw_on_unsupported_query`, and no query is built.
-- The translated query is logged, so the BSON value of every constant is asserted too, not only the fact
-- that a conversion happened. The column names carry the test number, so the logged query is found
-- whatever else runs at the same time.

-- One conjunct per integer / float / date / tuple / null constant type. The casts keep each constant at
-- its column's own type, otherwise the comparison is rewritten before the storage sees it. The two
-- values outside the Int32 range pin the width of the BSON number as well as its value.
-- The two columns named `...lossy_05881` carry the types whose conversion is lossy today: UInt64 and
-- Float32 both become a BSON double, which need not match the BSON number a stored document of the same
-- value holds. The pushed-down number of those two conjuncts is therefore replaced before the comparison
-- below instead of being recorded as expected: only the fact that both constants were converted and
-- pushed down is asserted, and that stays true once either arm is corrected.
SELECT * FROM mongodb('mongodb://127.0.0.1:1/db', 'c',
    'u16_05881 UInt16, u32_05881 UInt32, i8_05881 Int8, i16_05881 Int16, i64_05881 Int64,
     u64lossy_05881 UInt64, f32lossy_05881 Float32, d32_05881 Date32,
     t_05881 Tuple(Int32, String), n_05881 Nullable(Int32)')
WHERE u16_05881 = 7::UInt16 AND u32_05881 = 4000000000::UInt32
  AND i8_05881 = -1::Int8 AND i16_05881 = -2::Int16 AND i64_05881 = -3000000000::Int64
  AND u64lossy_05881 = 9::UInt64 AND f32lossy_05881 = 1.5::Float32
  AND d32_05881 = '2020-01-01'::Date32 AND t_05881 = (1, 'a')
  AND n_05881 IN (CAST(NULL, 'Nullable(Int32)'), 1::Nullable(Int32))
SETTINGS mongodb_throw_on_unsupported_query = 1; -- { serverError STD_EXCEPTION }

-- The MongoDB query the statement above produced: a 32 bit integer for UInt16 / Int8 / Int16, a 64 bit
-- integer for UInt32 / Int64, a date for Date32, an array for the tuple and the null value for the NULL
-- member of the IN list.
SYSTEM FLUSH LOGS text_log;
SELECT DISTINCT replaceRegexpAll(extract(message, '\\{.*\\}'),
    '("[a-z0-9]+lossy_05881" : \\{ "\\$eq" : )[-0-9.e+]+', '\\1<pushed down>')
FROM system.text_log
WHERE event_time > now() - INTERVAL 10 MINUTE AND message LIKE 'MongoDB query has built%u16_05881%'
SETTINGS max_rows_to_read = 0;

-- A constant whose type has no BSON counterpart is refused by the conversion itself, so it is reported
-- even when the restriction on unsupported queries is switched off.
SELECT * FROM mongodb('mongodb://127.0.0.1:1/db', 'c', 'dec_05881 Decimal32(2)')
WHERE dec_05881 = 1.5::Decimal32(2)
SETTINGS mongodb_throw_on_unsupported_query = 1; -- { serverError NOT_IMPLEMENTED }

SELECT * FROM mongodb('mongodb://127.0.0.1:1/db', 'c', 'dec_05881 Decimal32(2)')
WHERE dec_05881 = 1.5::Decimal32(2)
SETTINGS mongodb_throw_on_unsupported_query = 0; -- { serverError NOT_IMPLEMENTED }
