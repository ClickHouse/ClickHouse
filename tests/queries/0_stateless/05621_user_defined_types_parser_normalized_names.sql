-- `ParserDataType` parses every spelling of `Array` and `Map` followed by arguments as the built-in type
-- (`array(UInt8)` is `Array(UInt8)`), so a user-defined type with such a name could be created but would
-- never be used. These names are rejected like the built-in names themselves.
CREATE TYPE array(T) AS Tuple(T); -- { serverError BAD_ARGUMENTS }
CREATE TYPE ARRAY(T) AS Tuple(T); -- { serverError BAD_ARGUMENTS }
CREATE TYPE map(K, V) AS Tuple(K, V); -- { serverError BAD_ARGUMENTS }
CREATE TYPE mAp(K, V) AS Tuple(K, V); -- { serverError BAD_ARGUMENTS }
CREATE TYPE array AS String; -- { serverError BAD_ARGUMENTS }

-- The parser also recognizes the enum and integer names in any case: `enum8('a' = 1)` is parsed as an enum,
-- and `uint8(1)` as an integer with a display width. Every spelling of a built-in name is rejected.
CREATE TYPE enum8(T) AS String; -- { serverError BAD_ARGUMENTS }
CREATE TYPE ENUM16(T) AS String; -- { serverError BAD_ARGUMENTS }
CREATE TYPE enum AS String; -- { serverError BAD_ARGUMENTS }
CREATE TYPE uint8(T) AS Tuple(T); -- { serverError BAD_ARGUMENTS }
CREATE TYPE INT64 AS String; -- { serverError BAD_ARGUMENTS }
CREATE TYPE string AS UInt8; -- { serverError BAD_ARGUMENTS }
CREATE TYPE tuple(T) AS Array(T); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.user_defined_types WHERE lower(name) IN ('array', 'map', 'enum8', 'enum16', 'enum', 'uint8', 'int64', 'string', 'tuple');

DROP TABLE IF EXISTS t_udt_normalized_names;
CREATE TABLE t_udt_normalized_names (x array(UInt8), m map(String, UInt8), e enum8('a' = 1)) ENGINE = Memory;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_udt_normalized_names' ORDER BY name;
DROP TABLE t_udt_normalized_names;
