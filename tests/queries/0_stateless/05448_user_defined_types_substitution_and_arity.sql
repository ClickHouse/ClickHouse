-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS SaNested;
DROP TYPE IF EXISTS SaShadow;
DROP TYPE IF EXISTS SaJSON;
DROP TYPE IF EXISTS SaMoney;
DROP TYPE IF EXISTS SaBroken;
DROP TYPE IF EXISTS SaWide;

-- The actual arguments are substituted into the element types of `Nested` and the typed paths of `JSON`.
CREATE TYPE SaNested(T) AS Nested(x T, y String);
SELECT toTypeName(CAST([(1, 'a')], 'SaNested(UInt16)'));

-- A parameter may shadow a built-in type: the argument, not the built-in type, is used.
CREATE TYPE SaShadow(UInt8) AS Nested(x UInt8);
SELECT toTypeName(CAST([tuple('a')], 'SaShadow(String)'));

CREATE TYPE SaJSON(T) AS JSON(a.b T, SKIP c);
SELECT toTypeName(CAST('{"a" : {"b" : 42}}', 'SaJSON(UInt32)'));

-- A definition is not rejected because a few sample arguments do not make it valid.
CREATE TYPE SaMoney(P) AS Decimal(P, 2);
SELECT toTypeName(CAST(1, 'SaMoney(10)'));
SELECT CAST(1, 'SaMoney(1)'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A type family used with a number of arguments it never accepts is rejected whatever the number of parameters.
CREATE TYPE SaBroken(A, B, C, D, E) AS Tuple(A, B, C, D, Map(E)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE SaBroken(A, B, C, D, E, F) AS Tuple(A, B, C, D, Nested(x Array(E, F))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SHOW TYPE SaBroken; -- { serverError UNKNOWN_TYPE }

CREATE TYPE SaWide(A, B, C, D, E) AS Tuple(A, B, C, D, Map(String, E));
SELECT toTypeName(CAST((1, 2, 3, 4, map('a', 5)), 'SaWide(UInt8, UInt16, UInt32, UInt64, Int8)'));

DROP TYPE SaNested;
DROP TYPE SaShadow;
DROP TYPE SaJSON;
DROP TYPE SaMoney;
DROP TYPE SaWide;
