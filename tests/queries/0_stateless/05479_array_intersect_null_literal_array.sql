-- The elements of a literal `[NULL]` have type `Nullable(Nothing)`. `arrayIntersect` used it as the
-- common element type and failed to cast the other arguments to it, or found no common subtype at all.

SELECT arrayIntersect([1, NULL], [NULL]);
SELECT arrayIntersect([NULL], [1, NULL]);
SELECT arrayIntersect(['a', NULL], [NULL], [NULL, 'b']);
SELECT arrayIntersect([toNullable(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')), NULL], [NULL]);
SELECT arrayIntersect(CAST([1.5, NULL] AS Array(Nullable(Decimal32(2)))), [NULL]);
SELECT arrayIntersect(CAST(['a', NULL] AS Array(LowCardinality(Nullable(String)))), [NULL]);
SELECT arrayIntersect(CAST([1, NULL] AS Array(Dynamic)), [NULL]);
SELECT arrayIntersect(CAST([1, NULL] AS Array(Variant(UInt8, String))), [NULL]);
SELECT toTypeName(arrayIntersect([1, NULL], [NULL]));

SELECT 'no NULL in the other array';
SELECT arrayIntersect(CAST([1, 2] AS Array(Nullable(UInt8))), [NULL]);
SELECT arrayIntersect(CAST(['a'] AS Array(LowCardinality(Nullable(String)))), [NULL]);
SELECT toTypeName(arrayIntersect(CAST([1, 2] AS Array(Nullable(UInt8))), [NULL]));

SELECT 'the other array is not Nullable';
SELECT arrayIntersect([1, 2], [NULL]);
SELECT arrayIntersect([NULL], [1, 2]);
SELECT arrayIntersect([[1]], [NULL]);
SELECT toTypeName(arrayIntersect([1, 2], [NULL]));

SELECT 'non-const arguments';
SELECT a, arrayIntersect(a, [NULL]), arrayIntersect(materialize([NULL]), a)
FROM values('a Array(Nullable(UInt8))', ([1, NULL]), ([2]), ([]), ([NULL, NULL]));

SELECT 'only NULL arrays';
SELECT arrayIntersect([NULL], [NULL]);
SELECT toTypeName(arrayIntersect([NULL], [NULL]));
