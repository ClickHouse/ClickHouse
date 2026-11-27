-- Tags: no-fasttest
-- Tag no-fasttest: RapidJSON (allow_simdjson = 0) is not built in fasttest

-- An integer index outside the Int32 range must not wrap around.
SET allow_simdjson = 1;

SELECT JSONExtractInt('[10,20,30]', 1), JSONExtractInt('[10,20,30]', -1), JSONExtractInt('[10,20,30]', -3),
    JSONExtractInt('[10,20,30]', 4), JSONExtractInt('[10,20,30]', -4), JSONExtractInt('[10,20,30]', 0),
    JSONExtractInt('[10,20,30]', 4294967297), JSONExtractInt('[10,20,30]', 4294967295), JSONExtractInt('[10,20,30]', -4294967295),
    JSONExtractInt('[10,20,30]', 9223372036854775807), JSONExtractInt('[10,20,30]', -9223372036854775808),
    JSONExtractInt('[10,20,30]', 18446744073709551615);
SELECT JSONHas('[10,20,30]', 4294967297), JSONType('[10,"x",30]', 4294967298), JSONExtractString('["a","b"]', 4294967298),
    JSONExtractRaw('[10,20,30]', 4294967297), JSONExtractIntCaseInsensitive('[10,20,30]', 4294967297),
    JSONExtractInt('{"a":[10,20,30]}', 'a', 4294967298), JSONKey('{"a":1,"b":2}', 4294967298), JSONKey('{"a":1,"b":2}', -1),
    JSONLength('[[1,2],[3]]', 4294967297);
SELECT JSONExtractInt(j, 4294967297), JSONExtractInt(j, 18446744073709551615), JSONHas(j, 4294967297), JSONExtractInt(j, -1), JSONExtractInt(j, 2)
FROM (SELECT materialize('[10,20,30]') AS j);
SELECT i, JSONExtractInt('[10,20,30]', i), JSONHas('[10,20,30]', i)
FROM (SELECT arrayJoin([2, 4294967298, 18446744073709551615]::Array(UInt64)) AS i);
SELECT i, JSONExtractInt('[10,20,30]', i)
FROM (SELECT arrayJoin([-1, -3, -4, -4294967295, -9223372036854775808, 9223372036854775807]::Array(Int64)) AS i);

SET allow_simdjson = 0;

SELECT JSONExtractInt('[10,20,30]', 1), JSONExtractInt('[10,20,30]', -1), JSONExtractInt('[10,20,30]', -3),
    JSONExtractInt('[10,20,30]', 4), JSONExtractInt('[10,20,30]', -4), JSONExtractInt('[10,20,30]', 0),
    JSONExtractInt('[10,20,30]', 4294967297), JSONExtractInt('[10,20,30]', 4294967295), JSONExtractInt('[10,20,30]', -4294967295),
    JSONExtractInt('[10,20,30]', 9223372036854775807), JSONExtractInt('[10,20,30]', -9223372036854775808),
    JSONExtractInt('[10,20,30]', 18446744073709551615);
SELECT JSONHas('[10,20,30]', 4294967297), JSONType('[10,"x",30]', 4294967298), JSONExtractString('["a","b"]', 4294967298),
    JSONExtractRaw('[10,20,30]', 4294967297), JSONExtractIntCaseInsensitive('[10,20,30]', 4294967297),
    JSONExtractInt('{"a":[10,20,30]}', 'a', 4294967298), JSONKey('{"a":1,"b":2}', 4294967298), JSONKey('{"a":1,"b":2}', -1),
    JSONLength('[[1,2],[3]]', 4294967297);
SELECT JSONExtractInt(j, 4294967297), JSONExtractInt(j, 18446744073709551615), JSONHas(j, 4294967297), JSONExtractInt(j, -1), JSONExtractInt(j, 2)
FROM (SELECT materialize('[10,20,30]') AS j);
SELECT i, JSONExtractInt('[10,20,30]', i), JSONHas('[10,20,30]', i)
FROM (SELECT arrayJoin([2, 4294967298, 18446744073709551615]::Array(UInt64)) AS i);
SELECT i, JSONExtractInt('[10,20,30]', i)
FROM (SELECT arrayJoin([-1, -3, -4, -4294967295, -9223372036854775808, 9223372036854775807]::Array(Int64)) AS i);
