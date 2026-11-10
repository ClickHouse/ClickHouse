-- Return-type resolution of `transform`. Documented signature, from the
-- getSignatureString()/FunctionDocumentation block in src/Functions/transform.cpp:
--   transform(T, Array(T), Array(U), U) -> U;  or  transform(T, Array(T), Array(T)) -> T
-- The result type is the least common supertype of the destination array's element
-- type and (for the 4-argument form) the default value's type. Argument counts other
-- than 3 and 4 are rejected, and so is a pair that has no such supertype.

-- Wrong number of arguments: only 3 and 4 are accepted.
SELECT transform(1, [1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT transform(1, [1], [2], 3, 4); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 3-argument form: source and destination are both non-numeric and non-String, so
-- they pass the number/String symmetry check, but they have no common supertype.
SELECT transform([1], [[1]], [(1, 2)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT transform([1], [[1]], [map(1, 2)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- 4-argument form: no common supertype of the destination element type and the default.
SELECT transform(1, [1], ['x'], 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT transform(1, [1], [toUUID('00000000-0000-0000-0000-000000000000')], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The accepted forms still resolve and return the mapped value, resp. the default.
SELECT transform(2, [1, 2], [10, 20]);
SELECT transform(2, [1, 2], ['a', 'b'], 'z'), transform(9, [1, 2], ['a', 'b'], 'z');
