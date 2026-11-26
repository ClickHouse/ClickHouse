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

SELECT 'object';
SELECT count() FROM remote('127.0.0.1', system.one)
WHERE JSONAllPaths(materialize('{"a" : 1, "b" : 2}'::JSON(with_source=1, SKIP b))) = ['a']
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
