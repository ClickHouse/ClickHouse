-- A DateTime64, Date32, Time or Time64 constant has to reach another server as the same value, also past the
-- range of its type (arithmetic gets there), where a conversion from a number or text into the type saturates.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_time;
DROP TABLE IF EXISTS t_d32;
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

-- Date32: the last and first day of the range, and one day past each.
CREATE TABLE t_d32 (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_d32 SELECT toDate32('9999-12-31') + number FROM numbers(2) UNION ALL SELECT toDate32('0000-01-01') - number FROM numbers(2);
SELECT arraySort(groupArray(toInt32(d))) FROM remote('127.0.0.2', currentDatabase(), t_d32)
    WHERE d < toDate32('9999-12-31') + 1 AND d > toDate32('0000-01-01') - 1;
SELECT arraySort(groupArray(toInt32(d))) FROM remote('127.0.0.2', currentDatabase(), t_d32)
    WHERE d IN (toDate32('9999-12-31') + 1, toDate32('0000-01-01') - 1);
SELECT arraySort(groupArray(toInt32(d))) FROM (SELECT d FROM remote('127.0.0.2', currentDatabase(), t_d32))
    WHERE d < toDate32('9999-12-31') + 1 AND d > toDate32('0000-01-01') - 1
    SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1, serialize_query_plan = 0;
SELECT toInt32(d), toInt32(materialize(toDate32('9999-12-31') + 1)) FROM t_d32 WHERE d IN (toDate32('9999-12-31') + 1, toDate32('0000-01-01') - 1) ORDER BY d
    SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

-- Time and Time64: the last representable value at each end of the range, and one second past each.
CREATE TABLE t_time (t Time, t64 Time64(3)) ENGINE = MergeTree ORDER BY t;
INSERT INTO t_time SELECT toTime('999:59:59') + number, toTime64('999:59:59.999', 3) + number FROM numbers(2)
    UNION ALL SELECT toTime('-999:59:59') - number, toTime64('-999:59:59.999', 3) - number FROM numbers(2);
SELECT arraySort(groupArray(toInt64(t))) FROM remote('127.0.0.2', currentDatabase(), t_time)
    WHERE t < toTime('999:59:59') + 1 AND t > toTime('-999:59:59') - 1;
SELECT arraySort(groupArray(toDecimal64(t64, 3))) FROM remote('127.0.0.2', currentDatabase(), t_time)
    WHERE t64 < toTime64('999:59:59.999', 3) + 1 AND t64 > toTime64('-999:59:59.999', 3) - 1;
SELECT arraySort(groupArray(toDecimal64(t64, 3))) FROM remote('127.0.0.2', currentDatabase(), t_time)
    WHERE t64 < toTime64('999:59:59.999', 3) + 1 AND t64 > toTime64('-999:59:59.999', 3) - 1
    SETTINGS date_time_overflow_behavior = 'throw';

-- The other server computes the value it received, nested in every container.
SELECT toInt32(materialize(toDate32('9999-12-31') + 1)), toInt32(materialize([toDate32('9999-12-31') + 1])[1]),
    toInt32(tupleElement(materialize((toDate32('0000-01-01') - 1, 1)), 1)), toInt32(materialize(map('a', toDate32('9999-12-31') + 1))['a']),
    toInt32(mapKeys(materialize(map(toDate32('9999-12-31') + 1, 'a')))[1]),
    toInt32(assumeNotNull(materialize(CAST(toDate32('9999-12-31') + 1, 'Nullable(Date32)')))),
    toInt32(variantElement(materialize(CAST(toDate32('9999-12-31') + 1, 'Variant(Date32, String)')), 'Date32')),
    toInt32(dynamicElement(materialize(CAST(toDate32('9999-12-31') + 1, 'Dynamic')), 'Date32')),
    toInt32(dynamicElement(materialize(CAST(toDate32('9999-12-31') + 1, 'Dynamic(max_types = 0)')), 'Date32'))
FROM remote('127.0.0.2', system.one);
SELECT toInt32(materialize(CAST(toDate32('9999-12-31') + 1, 'LowCardinality(Date32)'))),
    toInt32(assumeNotNull(materialize(CAST(toDate32('9999-12-31') + 1, 'LowCardinality(Nullable(Date32))'))))
FROM remote('127.0.0.2', system.one) SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT arrayMap(x -> toInt32(x), materialize([toDate32('9999-12-31'), toDate32('9999-12-31') + 1]))
FROM remote('127.0.0.2', system.one) SETTINGS use_variant_as_common_type = 0;
SELECT toInt64(materialize(toTime('999:59:59') + 1)), toDecimal64(materialize(toTime64('999:59:59.999', 3) + 1), 3),
    toDecimal64(materialize([toTime64('-999:59:59.999', 3) - 1])[1], 3)
FROM remote('127.0.0.2', system.one);

DROP TABLE t_time;
DROP TABLE t_d32;
DROP TABLE t_dt64_dist;
DROP TABLE t_dt64_far;
DROP TABLE t_dt64;
