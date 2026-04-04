-- Tags: no-parallel
SET max_execution_time=30;

-- Bug: isBool(type) returns false for Nullable(Bool) in fieldAsBSONValue
-- In BSONCXXHelper.h:55, isBool(type) is called with the original Nullable(Bool) type
-- instead of the unwrapped nested type, so Nullable(Bool) is not recognized as Bool
-- and gets serialized as int32 instead of BSON bool (used by StorageMongoDB).
--
-- The BSONEachRow output format correctly unwraps Nullable before calling isBool,
-- so we test through that format to exercise and verify the Nullable(Bool) BSON path.

-- Test 1: Verify BSON type byte for Bool vs Nullable(Bool)
-- BSON document: <doc_size:4 bytes><type_byte:1><field_name><\0><value>...<\0 end>
-- For Bool: type byte = 0x08, value = 1 byte
-- For Int32: type byte = 0x10, value = 4 bytes
-- hex positions: 8 hex chars (4 bytes size) + 2 hex chars (type byte) = chars 9-10
SELECT 'bool_type_byte' AS test,
       substring(hex(formatRow('BSONEachRow', true::Bool AS a)), 9, 2) AS type_hex;

SELECT 'nullable_bool_type_byte' AS test,
       substring(hex(formatRow('BSONEachRow', CAST(true AS Nullable(Bool)) AS a)), 9, 2) AS type_hex;

-- Test 2: Both Bool and Nullable(Bool) should produce identical BSON when non-null
-- If the bug affected BSONEachRow, Nullable(Bool) would be 3 bytes larger (int32 vs bool)
SELECT 'size_match' AS test,
       length(formatRow('BSONEachRow', true::Bool AS a)) AS bool_size,
       length(formatRow('BSONEachRow', CAST(true AS Nullable(Bool)) AS a)) AS nullable_bool_size,
       length(formatRow('BSONEachRow', true::Bool AS a))
           = length(formatRow('BSONEachRow', CAST(true AS Nullable(Bool)) AS a)) AS same_size;

-- Test 3: Round-trip Bool through BSONEachRow
SELECT 'bool_roundtrip_true' AS test, a FROM format('BSONEachRow', 'a Bool', formatRow('BSONEachRow', true::Bool AS a));
SELECT 'bool_roundtrip_false' AS test, a FROM format('BSONEachRow', 'a Bool', formatRow('BSONEachRow', false::Bool AS a));

-- Test 4: Round-trip Nullable(Bool) through BSONEachRow
SELECT 'nullable_bool_true' AS test, a FROM format('BSONEachRow', 'a Nullable(Bool)', formatRow('BSONEachRow', CAST(true AS Nullable(Bool)) AS a));
SELECT 'nullable_bool_false' AS test, a FROM format('BSONEachRow', 'a Nullable(Bool)', formatRow('BSONEachRow', CAST(false AS Nullable(Bool)) AS a));
SELECT 'nullable_bool_null' AS test, a FROM format('BSONEachRow', 'a Nullable(Bool)', formatRow('BSONEachRow', CAST(NULL AS Nullable(Bool)) AS a));

-- Test 5: Contrast with UInt8 (not Bool) - should produce int32 BSON type (0x10)
SELECT 'uint8_type_byte' AS test,
       substring(hex(formatRow('BSONEachRow', toUInt8(1) AS a)), 9, 2) AS type_hex;
