-- A Variant or Dynamic member that cannot be converted to a tuple element makes the tuple NULL
-- (accurateCastOrNull) or the default (accurateCastOrDefault), also when the element type cannot contain NULL.

SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Variant(UInt16))), 'Nullable(Tuple(FixedString(2)))');
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Dynamic)), 'Nullable(Tuple(FixedString(2)))');
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Dynamic(max_types = 0))), 'Nullable(Tuple(FixedString(2)))');
SELECT accurateCastOrDefault(tuple(materialize(toUInt16(3)::Variant(UInt16))), 'Nullable(Tuple(FixedString(2)))', CAST(tuple('zz'), 'Nullable(Tuple(FixedString(2)))'));
SELECT accurateCastOrDefault(tuple(materialize(toUInt16(3)::Dynamic)), 'Nullable(Tuple(FixedString(2)))', CAST(tuple('zz'), 'Nullable(Tuple(FixedString(2)))'));
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Variant(UInt16))), 'Tuple(FixedString(2))');
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Variant(UInt16)), materialize(7)), 'Nullable(Tuple(FixedString(2), UInt8))');
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Variant(UInt16))), 'Nullable(Tuple(Tuple(UInt8, UInt8)))');
SELECT accurateCastOrNull(tuple(tuple(materialize(toUInt16(3)::Variant(UInt16)))), 'Nullable(Tuple(Tuple(FixedString(2))))');
SELECT accurateCastOrNull(tuple(v), 'Nullable(Tuple(FixedString(2)))') FROM (SELECT arrayJoin([3::UInt16::Variant(UInt16), NULL]) AS v);

-- Already NULL before: another member converts into a Nullable column, a Nullable element, a top-level cast, a NULL row.
SELECT accurateCastOrNull(tuple(v), 'Nullable(Tuple(FixedString(2)))') FROM (SELECT arrayJoin([3::UInt16::Variant(UInt16, String), 'ab'::Variant(UInt16, String), NULL, 'abc'::Variant(UInt16, String)]) AS v);
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Variant(UInt16))), 'Nullable(Tuple(Nullable(FixedString(2))))');
SELECT accurateCastOrNull(materialize(toUInt16(3)::Variant(UInt16)), 'FixedString(2)');
SELECT accurateCastOrNull(tuple(materialize(NULL::Variant(String))), 'Nullable(Tuple(Array(UInt8)))');

-- An Array or Map element cannot be NULL, so the member converts its values as accurateCast does.
SELECT accurateCastOrNull(tuple(materialize('[1]'::Variant(String))), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrNull(tuple(materialize('[1]'::Dynamic)), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrNull(tuple(materialize('[2]'::Variant(UInt16, String))), 'Nullable(Tuple(Array(UInt8)))');
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Variant(UInt16))), 'Nullable(Tuple(Array(UInt8)))'); -- { serverError TYPE_MISMATCH }
SELECT accurateCastOrNull(tuple(materialize(toUInt16(3)::Dynamic)), 'Nullable(Tuple(Map(String, UInt8)))'); -- { serverError TYPE_MISMATCH }

-- The values under a NULL tuple are not converted.
SELECT accurateCastOrNull(if(number = 0, NULL, tuple(v)), 'Nullable(Tuple(Array(UInt8)))') FROM (SELECT number, if(number = 0, 3::UInt16::Variant(UInt16, String), '[1]'::Variant(UInt16, String)) AS v FROM numbers(2));
SELECT accurateCastOrNull(if(number = 0, NULL, tuple(v)), 'Nullable(Tuple(Array(UInt8)))') FROM (SELECT number, if(number = 0, 3::UInt16::Dynamic, '[1]'::Dynamic) AS v FROM numbers(2));
SELECT accurateCastOrNull(if(number = 0, NULL, tuple(v)), 'Nullable(Tuple(Array(UInt8)))') FROM (SELECT number, if(number = 0, 3::UInt16::Dynamic(max_types = 0), '[1]'::Dynamic(max_types = 0)) AS v FROM numbers(2));
SELECT accurateCastOrDefault(if(number = 0, NULL, tuple(v)), 'Nullable(Tuple(Array(UInt8)))', CAST(tuple([9]), 'Nullable(Tuple(Array(UInt8)))')) FROM (SELECT number, if(number = 0, 3::UInt16::Variant(UInt16, String), '[1]'::Variant(UInt16, String)) AS v FROM numbers(2));
SELECT accurateCastOrNull(tuple(if(number = 0, NULL, tuple(v))), 'Nullable(Tuple(Tuple(Array(UInt8))))') FROM (SELECT number, if(number = 0, 3::UInt16::Variant(UInt16, String), '[1]'::Variant(UInt16, String)) AS v FROM numbers(2));
SELECT accurateCastOrNull(if(number = 0, NULL, tuple(if(number = 1, NULL, tuple(v)))), 'Nullable(Tuple(Nullable(Tuple(Array(UInt8)))))') FROM (SELECT number, if(number = 2, '[1]'::Variant(UInt16, String), 3::UInt16::Variant(UInt16, String)) AS v FROM numbers(3));
