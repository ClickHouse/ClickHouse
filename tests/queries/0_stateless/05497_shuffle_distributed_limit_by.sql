-- `SHUFFLE` with `LIMIT BY` in a distributed query: the internal key column, by which the initiator merges
-- the shuffled streams of the shards, must be kept through the `LIMIT BY` expression on the shards.

SET allow_experimental_shuffle_query = 1;

SELECT count() FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(100)) SHUFFLE LIMIT 1 BY number);
SELECT count(), uniqExact(k) FROM (SELECT number % 10 AS k FROM remote('127.0.0.{1,2}', numbers(100)) SHUFFLE LIMIT 1 BY number % 10);
SELECT count(), uniqExact(number) FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(100)) SHUFFLE LIMIT 2 BY number LIMIT 200);
SELECT count() FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(100)) SHUFFLE LIMIT 1 OFFSET 1 BY number);
SELECT count() FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(100)) SHUFFLE LIMIT 1 BY number LIMIT 10);
