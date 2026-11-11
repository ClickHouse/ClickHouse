-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

-- A parameterized definition that is not a valid data type for any actual arguments is rejected at definition time.

DROP TYPE IF EXISTS PvNested;
DROP TYPE IF EXISTS PvBroken;
DROP TYPE IF EXISTS PvFixed;
DROP TYPE IF EXISTS PvDateTime;
DROP TYPE IF EXISTS PvDecimal;
DROP TYPE IF EXISTS PvMap;
DROP TYPE IF EXISTS PvAggregate;
DROP TYPE IF EXISTS PvVariant;

CREATE TYPE PvBroken(T) AS Map(String); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE PvBroken(T) AS Map(T); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE PvBroken(K, V) AS Map(K, V, String); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE PvBroken(T) AS Array(Map(T)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SHOW TYPE PvBroken; -- { serverError UNKNOWN_TYPE }

-- The parameters can stand for types and values.
CREATE TYPE PvFixed(N) AS FixedString(N);
CREATE TYPE PvDateTime(P, TZ) AS DateTime64(P, TZ);
CREATE TYPE PvDecimal(P, S) AS Decimal(P, S);
CREATE TYPE PvMap(K, V) AS Map(K, Array(V));
CREATE TYPE PvAggregate(T) AS AggregateFunction(sum, T);
CREATE TYPE PvVariant(T, U) AS Variant(T, U);
CREATE TYPE PvNested(T) AS PvMap(String, PvFixed(T));

SELECT toTypeName(CAST('a', 'PvFixed(3)'));
SELECT toTypeName(CAST(0, 'PvDateTime(3, \'UTC\')'));
SELECT toTypeName(CAST(1, 'PvDecimal(10, 2)'));
SELECT toTypeName(CAST(map('a', [1]), 'PvMap(String, UInt8)'));
SELECT toTypeName(CAST(NULL, 'PvVariant(String, UInt64)'));
SELECT toTypeName(CAST(map('a', ['b']), 'PvNested(2)'));
SELECT toTypeName(sumState(number)::PvAggregate(UInt64)) FROM numbers(3);

DROP TYPE PvNested;
DROP TYPE PvFixed;
DROP TYPE PvDateTime;
DROP TYPE PvDecimal;
DROP TYPE PvMap;
DROP TYPE PvAggregate;
DROP TYPE PvVariant;
