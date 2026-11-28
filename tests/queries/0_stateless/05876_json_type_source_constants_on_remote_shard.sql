-- A folded JSON constant with the source is sent to a shard as its source text, so the shard reads the same
-- source and builds the same object as the initiator.

SELECT 'source';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE tupleElement(materialize('{ "a" : 1 }'::JSON(with_source=1)), '__source') = '{ "a" : 1 }'
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

SELECT 'source inside an array';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE materialize(['{ "a" : 1 }'::JSON(with_source=1)])[1].__source = '{ "a" : 1 }'
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

SELECT 'source of a nested value';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE tupleElement(materialize('{"a":{"z":1,"b":2}}'::JSON(a JSON(with_source=1))).a, '__source') = '{"z":1,"b":2}'
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

SELECT 'typed date and time values';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE materialize('{"d":"2020-01-01 00:00:00.123","t":"2020-01-01 00:00:00"}'::JSON(d DateTime64(3, 'UTC'), t DateTime('UTC'))).d = toDateTime64('2020-01-01 00:00:00.123', 3, 'UTC')
    AND materialize('{"d":"2020-01-01 00:00:00.123","t":"2020-01-01 00:00:00"}'::JSON(d DateTime64(3, 'UTC'), t DateTime('UTC'))).t = toDateTime('2020-01-01 00:00:00', 'UTC')
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

SELECT 'typed date and time values right after the epoch';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE materialize('{"d":"1970-01-01 00:00:01.500","t":"1970-01-01 00:00:01"}'::JSON(d DateTime64(3, 'UTC'), t DateTime('UTC'))).d = toDateTime64('1970-01-01 00:00:01.500', 3, 'UTC')
    AND materialize('{"d":"1970-01-01 00:00:01.500","t":"1970-01-01 00:00:01"}'::JSON(d DateTime64(3, 'UTC'), t DateTime('UTC'))).t = toDateTime('1970-01-01 00:00:01', 'UTC')
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0, date_time_input_format = 'basic';

SELECT 'object';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE JSONAllPaths(materialize('{"a" : 1, "b" : 2}'::JSON(with_source=1, SKIP b))) = ['a']
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
