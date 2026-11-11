-- `ParserDataType` parses every spelling of `Array` and `Map` followed by arguments as the built-in type
-- (`array(UInt8)` is `Array(UInt8)`), so a user-defined type with such a name could be created but would
-- never be used. These names are rejected like the built-in names themselves.
CREATE TYPE array(T) AS Tuple(T); -- { serverError BAD_ARGUMENTS }
CREATE TYPE ARRAY(T) AS Tuple(T); -- { serverError BAD_ARGUMENTS }
CREATE TYPE map(K, V) AS Tuple(K, V); -- { serverError BAD_ARGUMENTS }
CREATE TYPE mAp(K, V) AS Tuple(K, V); -- { serverError BAD_ARGUMENTS }
CREATE TYPE array AS String; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.user_defined_types WHERE lower(name) IN ('array', 'map');

DROP TABLE IF EXISTS t_udt_normalized_names;
CREATE TABLE t_udt_normalized_names (x array(UInt8), m map(String, UInt8)) ENGINE = Memory;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_udt_normalized_names' ORDER BY name;
DROP TABLE t_udt_normalized_names;
