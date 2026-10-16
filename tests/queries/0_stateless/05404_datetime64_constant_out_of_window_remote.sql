-- A DateTime64 constant has to reach another server as the same ticks, also past 9999-12-31 23:59:59,
-- where a conversion from a number or text into DateTime64 saturates.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_dt64_dist;
DROP TABLE IF EXISTS t_dt64_far;
DROP TABLE IF EXISTS t_dt64;

-- The stored values are within the range of the type, the constant is not.
CREATE TABLE t_dt64 (id UInt32, valid_to DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dt64 SELECT 1, toDateTime64('2024-05-01 00:00:00', 0, 'UTC') UNION ALL SELECT 2, toDateTime64('9999-12-31 23:59:59', 0, 'UTC');
CREATE TABLE t_dt64_dist AS t_dt64 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_dt64);

SELECT count() FROM t_dt64 WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND;
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64) WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND;
SELECT count() FROM t_dt64_dist WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND;
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64) WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND
    SETTINGS date_time_overflow_behavior = 'throw';
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64) WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND
    SETTINGS serialize_query_plan = 1;
SELECT count() FROM (SELECT valid_to FROM remote('127.0.0.2', currentDatabase(), t_dt64)) WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND
    SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1, serialize_query_plan = 0;
SELECT count() FROM t_dt64 WHERE valid_to < toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND
    SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;
SELECT id, toUnixTimestamp64Second(materialize(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND)) FROM t_dt64 ORDER BY id
    SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

-- Stored values past the range: every remote count must equal the local one (1).
CREATE TABLE t_dt64_far (d DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dt64_far SELECT toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + number + 1 FROM numbers(2);
SELECT count() FROM t_dt64_far WHERE d != toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1;
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64_far) WHERE d != toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1;
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64_far) WHERE d = toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1;
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64_far) WHERE d NOT IN (SELECT toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1);
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64_far)
    WHERE d IN (toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 1, toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + 5);
-- A chain of equalities is rewritten to `IN`.
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_dt64)
    WHERE valid_to = toDateTime64('2024-05-01 00:00:00', 0, 'UTC') OR valid_to = toDateTime64('9999-12-31 23:59:59', 0, 'UTC') OR valid_to = toDateTime64('2000-01-01 00:00:00', 0, 'UTC');

-- `materialize` makes the other server compute the ticks of the value it received.
SELECT toUnixTimestamp64Second(materialize(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND)),
    toUnixTimestamp64Milli(materialize(fromUnixTimestamp64Milli(253402300800000, 'Europe/Berlin'))),
    toUnixTimestamp64Second(materialize(fromUnixTimestamp64Second(-62167219201, 'UTC'))),
    toUnixTimestamp64Milli(materialize(toDateTime64('2024-05-01 00:00:00.123', 3, 'UTC')))
FROM remote('127.0.0.2', system.one);
SELECT toUnixTimestamp64Second(materialize([toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND])[1]),
    toUnixTimestamp64Second(tupleElement(materialize((toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND, 1)), 1)),
    toUnixTimestamp64Second(materialize(map('a', toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND))['a']),
    toUnixTimestamp64Second(assumeNotNull(materialize(CAST(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND, 'Nullable(DateTime64(0, \'UTC\'))'))))
FROM remote('127.0.0.2', system.one);
SELECT variantType(v), toUnixTimestamp64Second(variantElement(v, 'DateTime64(0, \'UTC\')')),
    dynamicType(d), toUnixTimestamp64Second(dynamicElement(d, 'DateTime64(0, \'UTC\')')),
    toUnixTimestamp64Second(dynamicElement(s, 'DateTime64(0, \'UTC\')'))
FROM
(
    SELECT materialize(CAST(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND, 'Variant(DateTime64(0, \'UTC\'), String)')) AS v,
        materialize(CAST(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND, 'Dynamic')) AS d,
        materialize(CAST(toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND, 'Dynamic(max_types = 0)')) AS s
    FROM remote('127.0.0.2', system.one)
);
SELECT toDateTime64('9999-12-31 23:59:59', 0, 'UTC') + INTERVAL 1 SECOND, count() FROM remote('127.0.0.2', currentDatabase(), t_dt64) GROUP BY 1;

DROP TABLE t_dt64_dist;
DROP TABLE t_dt64_far;
DROP TABLE t_dt64;
