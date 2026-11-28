-- Monotonicity of negate() during MergeTree primary key analysis.
-- negate(x) returns -x and the result is always signed, see the section "negate"
-- of docs/reference/functions/regular-functions/arithmetic-functions.mdx.
-- Primary key analysis asks negate() whether it is monotonic on a key range
-- whenever a predicate wraps a key column, or whenever negate() has to be
-- inverted out of a key expression. It is not monotonic on a range that may
-- contain NULL, the minimum of a signed integer type (negate overflows back
-- onto it) or a NaN, and it is not applicable to a type that is not
-- represented by a number. Each case below puts negate() on a key so that one
-- of those decisions is taken, and asserts how many rows match. Every count is
-- the plain semantics of the predicate, so it is the same no matter how many
-- granules the index lets the query read.

-- A signed integer key whose left bound is the type minimum: negate(-128) is
-- -128 again. 9 rows, and only a = 0 gives negate(a) = 0.
DROP TABLE IF EXISTS negate_int8;
CREATE TABLE negate_int8 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2;
INSERT INTO negate_int8 SELECT number - 128 FROM numbers(8);
INSERT INTO negate_int8 VALUES (0);
SELECT 'int8', count() FROM negate_int8 WHERE negate(a) != 0;

-- The same check for the wide signed integers, whose bounds reach the key
-- condition as Int128 and Int256 fields. Keys are -4 .. 3, so negate(a) > 1
-- holds for -4, -3 and -2.
DROP TABLE IF EXISTS negate_wide;
CREATE TABLE negate_wide (a Int128, b Int256) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 2;
INSERT INTO negate_wide SELECT number - 4, number - 4 FROM numbers(8);
SELECT 'int128', count() FROM negate_wide WHERE negate(a) > 1;
SELECT 'int256', count() FROM negate_wide WHERE negate(b) > 1;

-- A Nullable key: the NULL sorts first, so the left bound of the first granule
-- is NULL and negate() is not monotonic there. negate(a) > 1 holds for -4, -3
-- and -2; the NULL row does not match because negate(NULL) is NULL.
DROP TABLE IF EXISTS negate_nullable;
CREATE TABLE negate_nullable (a Nullable(Int32)) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO negate_nullable SELECT if(number = 3, NULL, number - 4) FROM numbers(8);
SELECT 'nullable', count() FROM negate_nullable WHERE negate(a) > 1;

-- negate() inside the key expression over a Nullable column: a predicate on the
-- bare column has to be transformed through negate(). Keys are -4 .. 3, so four
-- rows are negative.
DROP TABLE IF EXISTS negate_keyexpr;
CREATE TABLE negate_keyexpr (k Nullable(Int32)) ENGINE = MergeTree ORDER BY negate(k)
SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO negate_keyexpr SELECT number - 4 FROM numbers(8);
SELECT 'keyexpr', count() FROM negate_keyexpr WHERE k < 0;

-- A Tuple key is not represented by a number, so there is no monotonicity to
-- report. negate() on a tuple negates every element, and only (1, 1) becomes
-- (-1, -1).
DROP TABLE IF EXISTS negate_tuple;
CREATE TABLE negate_tuple (t Tuple(Int32, Int32)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 2;
INSERT INTO negate_tuple SELECT (number, number) FROM numbers(8);
SELECT 'tuple', count() FROM negate_tuple WHERE negate(t) = (-1, -1);

-- The first key column is unique per row, so the granule bounds leave the
-- trailing key columns the whole universe of their type, i.e. an infinite
-- bound. b is -4 .. 3 and negate(b) > 1 holds for -4, -3 and -2; c is
-- 0, 0.5, .. 3.5 and negate(c) > -2 means c < 2, which holds for 0, 0.5, 1, 1.5.
DROP TABLE IF EXISTS negate_suffix;
CREATE TABLE negate_suffix (a UInt32, b Int32, c Float64) ENGINE = MergeTree ORDER BY (a, b, c)
SETTINGS index_granularity = 2;
INSERT INTO negate_suffix SELECT number, number - 4, number / 2 FROM numbers(8);
SELECT 'suffix_int', count() FROM negate_suffix WHERE negate(b) > 1;
SELECT 'suffix_float', count() FROM negate_suffix WHERE negate(c) > -2;

-- A floating point key that contains a NaN: negate(NaN) is NaN, so the order is
-- not reversed and negate() is not monotonic on a range that holds it. The keys
-- are 0, 0.5, .. 3 and one NaN, all in one part with one row per granule, so the
-- NaN sits at a normal index mark and the granule that holds the key 3 has the
-- mixed range [3, NaN] while the last granule has the left bound NaN. A
-- comparison with NaN is false, so negate(a) > -2 means a < 2 and negate(a) < -1
-- means a > 1; negate(NaN) stays NaN. The row 3 matches negate(a) < -1 and lives
-- in the mixed granule, so dropping the NaN rejection would lose it.
DROP TABLE IF EXISTS negate_float;
CREATE TABLE negate_float (a Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO negate_float SELECT if(number = 7, nan, number / 2) FROM numbers(8);
SELECT 'float_gt', count() FROM negate_float WHERE negate(a) > -2;
SELECT 'float_lt', count() FROM negate_float WHERE negate(a) < -1;
SELECT 'float_nan', count() FROM negate_float WHERE isNaN(negate(a));

DROP TABLE negate_int8;
DROP TABLE negate_wide;
DROP TABLE negate_nullable;
DROP TABLE negate_keyexpr;
DROP TABLE negate_tuple;
DROP TABLE negate_suffix;
DROP TABLE negate_float;
