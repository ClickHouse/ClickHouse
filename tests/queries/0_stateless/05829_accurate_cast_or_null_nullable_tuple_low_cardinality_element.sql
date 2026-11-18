-- A failed conversion of a non-nullable LowCardinality element makes the tuple NULL.
SELECT accurateCastOrNull(tuple(materialize('abc')), 'Nullable(Tuple(LowCardinality(FixedString(2))))');
SELECT accurateCastOrDefault(tuple(materialize('abc')), 'Nullable(Tuple(LowCardinality(FixedString(2))))');
SELECT accurateCastOrNull(tuple(materialize(300)), 'Nullable(Tuple(LowCardinality(UInt8)))') SETTINGS allow_suspicious_low_cardinality_types = 1;

-- So does a NULL of the source element.
SELECT accurateCastOrNull(tuple(materialize(NULL::Nullable(String))), 'Nullable(Tuple(LowCardinality(String)))');
SELECT accurateCastOrNull(tuple(materialize(NULL)), 'Nullable(Tuple(LowCardinality(String)))');

-- LowCardinality sources.
SELECT accurateCastOrNull(tuple(toLowCardinality(materialize(toNullable('ab')))), 'Nullable(Tuple(LowCardinality(String)))');
SELECT accurateCastOrNull(tuple(toLowCardinality(materialize('abc'))), 'Nullable(Tuple(LowCardinality(FixedString(2))))');

-- Rows, a second element, and the declared non-nullable dictionary of the result.
SELECT x, accurateCastOrNull(tuple(x, x), 'Nullable(Tuple(LowCardinality(FixedString(2)), LowCardinality(String)))')
FROM (SELECT arrayJoin(['ab', 'abc', NULL, 'cd'])::LowCardinality(Nullable(String)) AS x);
SELECT s, accurateCastOrNull(tuple(s, toLowCardinality(s), s), 'Nullable(Tuple(LowCardinality(String), LowCardinality(String), LowCardinality(FixedString(2))))')
FROM (SELECT arrayJoin(['ab', 'abc']) AS s);
SELECT accurateCastOrNull(tuple('ab'), 'Nullable(Tuple(LowCardinality(FixedString(2))))');
SELECT dumpColumnStructure(accurateCastOrNull(tuple(materialize('ab')), 'Nullable(Tuple(LowCardinality(FixedString(2))))'));

-- Variant, Dynamic and nested tuple sources.
SELECT accurateCastOrNull(tuple(materialize('abc')::Variant(String, UInt64)), 'Nullable(Tuple(LowCardinality(FixedString(2))))');
SELECT accurateCastOrNull(tuple(materialize(NULL)::Variant(String, UInt64)), 'Nullable(Tuple(LowCardinality(String)))') SETTINGS cast_keep_nullable = 1;
SELECT accurateCastOrNull(tuple(materialize('abc')::Dynamic), 'Nullable(Tuple(LowCardinality(FixedString(2))))');
SELECT accurateCastOrNull(tuple(tuple(materialize('abc'))), 'Nullable(Tuple(Tuple(LowCardinality(FixedString(2)))))');

-- A nullable dictionary keeps a NULL of the source element as a value.
SELECT accurateCastOrNull(tuple(materialize(NULL::Nullable(String))), 'Nullable(Tuple(LowCardinality(Nullable(String))))');

-- The NULL rows of the source tuple hold placeholder values, which are not converted.
SELECT accurateCastOrNull(t, 'Nullable(Tuple(LowCardinality(String)))'), accurateCastOrNull(t, 'Nullable(Tuple(LowCardinality(FixedString(1))))')
FROM (SELECT arrayJoin([tuple('a'::Enum8('a' = 1)), arrayElementOrNull([tuple('a'::Enum8('a' = 1))], 2)]) AS t);
SELECT accurateCastOrDefault(arrayElementOrNull([tuple('a'::Enum16('a' = 1000))], 2), 'Nullable(Tuple(LowCardinality(String)))');
SELECT CAST(arrayElementOrNull([tuple('a'::Enum8('a' = 1))], 2), 'Nullable(Tuple(Nullable(String)))');

-- So are those under a nullable element, whose own null map does not cover them.
SELECT accurateCastOrNull(t, 'Nullable(Tuple(LowCardinality(String)))'), CAST(t, 'Nullable(Tuple(String))'), CAST(t, 'Nullable(Tuple(Nullable(String)))')
FROM (SELECT CAST(arrayJoin([tuple('a'::Enum8('a' = 1)), arrayElementOrNull([tuple('a'::Enum8('a' = 1))], 2)]), 'Nullable(Tuple(Nullable(Enum8(\'a\' = 1))))') AS t);
-- A LowCardinality element converts its dictionary, which is smaller than the tuple.
SELECT accurateCastOrNull(t, 'Nullable(Tuple(LowCardinality(String)))'), CAST(t, 'Nullable(Tuple(String))')
FROM (SELECT CAST(arrayElementOrNull([tuple(materialize('a'))], number % 3), 'Nullable(Tuple(LowCardinality(Nullable(String))))') AS t FROM numbers(6));
