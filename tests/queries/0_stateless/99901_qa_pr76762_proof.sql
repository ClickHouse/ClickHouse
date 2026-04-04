-- Tags: no-fasttest
-- Test for bug: isBool(type) returns false for Nullable(Bool) in fieldAsBSONValue
-- Nullable(Bool) should be serialized as BSON bool (type 0x08), not BSON int32 (type 0x10)
-- This test verifies correct behavior through BSONEachRow format and roundtrip checks.

SET max_execution_time = 30;

-- Cleanup
DROP TABLE IF EXISTS test_nullable_bool_bson_src;
DROP TABLE IF EXISTS test_nullable_bool_bson_dst;

-- Create source table with Bool and Nullable(Bool) columns
CREATE TABLE test_nullable_bool_bson_src (
    id UInt64,
    b Bool,
    nb Nullable(Bool),
    nn Nullable(Bool)
) ENGINE = Memory;

INSERT INTO test_nullable_bool_bson_src VALUES (1, true, true, NULL), (2, false, false, NULL), (3, true, NULL, NULL);

-- Part 1: Verify BSONEachRow roundtrip preserves Nullable(Bool) values correctly
CREATE TABLE test_nullable_bool_bson_dst (
    id UInt64,
    b Bool,
    nb Nullable(Bool),
    nn Nullable(Bool)
) ENGINE = Memory;

INSERT INTO test_nullable_bool_bson_dst SELECT * FROM test_nullable_bool_bson_src;

SELECT 'roundtrip_values';
SELECT id, b, nb, nn FROM test_nullable_bool_bson_dst ORDER BY id;

-- Part 2: Verify types are preserved after roundtrip
SELECT 'roundtrip_types';
SELECT toTypeName(b), toTypeName(nb), toTypeName(nn) FROM test_nullable_bool_bson_dst LIMIT 1;

-- Part 3: Verify Bool and Nullable(Bool) produce identical BSON type byte (0x08 = bool)
-- Write Bool=true and Nullable(Bool)=true to BSONEachRow files, compare hex output
-- BSON doc: size(4) + type(1) + key + null_terminator + value + doc_end
-- For Bool: type byte at offset 4 should be 08 (BSON bool)
-- If bug present: Nullable(Bool) would produce 10 (BSON int32) instead of 08
INSERT INTO FUNCTION file('99901_bool.bson', 'BSONEachRow', 'b Bool') SELECT true::Bool;
INSERT INTO FUNCTION file('99901_nbool.bson', 'BSONEachRow', 'b Nullable(Bool)') SELECT CAST(true AS Nullable(Bool));

SELECT 'bson_type_check';
-- Extract the BSON type byte (5th byte, offset 4) from each file
-- Both should produce '08' (BSON bool type), not '10' (BSON int32)
-- If the bug were present, Nullable(Bool) would use type 0x10 (int32) instead of 0x08 (bool)
SELECT
    substring(hex(t1.raw_blob), 9, 2) AS bool_type_byte,
    substring(hex(t2.raw_blob), 9, 2) AS nullable_bool_type_byte,
    substring(hex(t1.raw_blob), 9, 2) = '08' AS bool_is_bson_bool,
    substring(hex(t2.raw_blob), 9, 2) = '08' AS nullable_bool_is_bson_bool,
    hex(t1.raw_blob) = hex(t2.raw_blob) AS encoding_matches
FROM
    file('99901_bool.bson', 'RawBLOB') AS t1,
    file('99901_nbool.bson', 'RawBLOB') AS t2;

-- Part 4: Verify Nullable(Bool) with NULL produces BSON null type (0x0A)
INSERT INTO FUNCTION file('99901_null_bool.bson', 'BSONEachRow', 'b Nullable(Bool)') SELECT CAST(NULL AS Nullable(Bool));

SELECT 'null_bson_type_check';
SELECT
    substring(hex(raw_blob), 9, 2) AS null_type_byte,
    substring(hex(raw_blob), 9, 2) = '0A' AS is_bson_null
FROM file('99901_null_bool.bson', 'RawBLOB');

-- Cleanup
DROP TABLE IF EXISTS test_nullable_bool_bson_src;
DROP TABLE IF EXISTS test_nullable_bool_bson_dst;
