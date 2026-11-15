-- An exception from the conversion of two-level aggregated data to chunks (here, finalization of `mannWhitneyUTest`
-- with an empty second sample) was swallowed when the conversion ran synchronously (at most 100000 groups), and the
-- query returned an incomplete result instead of the exception.

DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS l;

CREATE TABLE l (id UInt32, val Float64) ENGINE = Memory;
INSERT INTO l SELECT number, number FROM numbers(2000);
CREATE TABLE d AS l ENGINE = Distributed(test_shard_localhost, currentDatabase(), l);
CREATE VIEW v AS SELECT id, val FROM d;

SET optimize_trivial_view_pushdown_to_distributed = 1, prefer_localhost_replica = 0, max_threads = 8,
    group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0;

SELECT id, mannWhitneyUTest(val, toUInt8(0)) FROM remote('127.0.0.{1,2}', currentDatabase(), v) GROUP BY id
SETTINGS distributed_aggregation_memory_efficient = 0; -- { serverError BAD_ARGUMENTS }
SELECT id, mannWhitneyUTest(val, toUInt8(0)) FROM remote('127.0.0.{1,2}', currentDatabase(), v) GROUP BY id
SETTINGS distributed_aggregation_memory_efficient = 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE v;
DROP TABLE d;
DROP TABLE l;
