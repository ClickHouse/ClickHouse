-- A `Decimal` value can have more digits than the precision of its type, e.g. `toDecimal64(1, 18)` or a sum.
-- A query sent to another server must carry such a constant with the same value.

SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_05410;
DROP TABLE IF EXISTS t_05410_dist;

CREATE TABLE t_05410 (d32 Decimal(9, 0), d64 Decimal(18, 0), d128 Decimal(38, 0), d256 Decimal(76, 0)) ENGINE = MergeTree ORDER BY d64;
INSERT INTO t_05410 VALUES (1, 1, 170141183, 5789604), (2, 2, 2, 2), (-1, -1, -1, -1);
CREATE TABLE t_05410_dist AS t_05410 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05410);

SELECT toTypeName(toDecimal64(1, 18)), toTypeName(toDecimal32(0.6, 9) + toDecimal32(0.6, 9)), toDecimal32(0.6, 9) + toDecimal32(0.6, 9);

SELECT '-- local';
SELECT count() FROM t_05410 WHERE d64 = toDecimal64(1, 18);
SELECT countIf(d32 = toDecimal32(1, 9)), countIf(d128 = toDecimal128(170141183, 30)), countIf(d256 = toDecimal256(5789604, 70)),
    countIf(d64 > toDecimal64(-1, 18)), countIf(d32 > toDecimal32(0.6, 9) + toDecimal32(0.6, 9)), countIf(d64 < toDecimal64(0.5, 18))
FROM t_05410;

SELECT '-- remote';
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_05410) WHERE d64 = toDecimal64(1, 18);
SELECT countIf(d32 = toDecimal32(1, 9)), countIf(d128 = toDecimal128(170141183, 30)), countIf(d256 = toDecimal256(5789604, 70)),
    countIf(d64 > toDecimal64(-1, 18)), countIf(d32 > toDecimal32(0.6, 9) + toDecimal32(0.6, 9)), countIf(d64 < toDecimal64(0.5, 18))
FROM remote('127.0.0.2', currentDatabase(), t_05410);
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_05410) WHERE d64 IN (toDecimal64(1, 18), toDecimal64(2, 18));
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_05410) WHERE d64 IN (SELECT toDecimal64(1, 18));

SELECT '-- Distributed';
SELECT count() FROM t_05410_dist WHERE d64 = toDecimal64(1, 18);
SELECT countIf(d32 = toDecimal32(1, 9)), countIf(d128 = toDecimal128(170141183, 30)), countIf(d256 = toDecimal256(5789604, 70)),
    countIf(d64 > toDecimal64(-1, 18)), countIf(d32 > toDecimal32(0.6, 9) + toDecimal32(0.6, 9)), countIf(d64 < toDecimal64(0.5, 18))
FROM t_05410_dist;

SELECT '-- filter pushed down into a subquery';
SELECT count() FROM (SELECT d64 FROM remote('127.0.0.2', currentDatabase(), t_05410)) WHERE d64 = toDecimal64(1, 18)
SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1, serialize_query_plan = 0;

SELECT '-- serialized query plan';
SELECT count() FROM t_05410_dist WHERE d64 = toDecimal64(1, 18) SETTINGS serialize_query_plan = 1;

SELECT '-- parallel replicas';
SELECT count() FROM t_05410 WHERE d64 = toDecimal64(1, 18)
SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

SELECT '-- values computed by the other server';
SELECT toDecimal64(1, 18), toDecimal32(0.6, 9) + toDecimal32(0.6, 9), toDecimal64(0.5, 18) FROM remote('127.0.0.2', system.one);
SELECT toString(materialize(toDecimal32(1, 9))), toString(materialize(toDecimal64(-1, 18))), toString(materialize(toDecimal128(170141183, 30)))
FROM remote('127.0.0.2', system.one);
SELECT toString(materialize(toDecimal256(5789604, 70))) FROM remote('127.0.0.2', system.one);
SELECT materialize([toDecimal64(1, 18), toDecimal64(0.5, 18)]), materialize((toDecimal64(1, 18), 1)), materialize(map('a', toDecimal64(1, 18))),
    materialize(toNullable(toDecimal64(1, 18)))
FROM remote('127.0.0.2', system.one);
SELECT toString(materialize(CAST(toDecimal64(1, 18), 'Variant(Decimal(18, 18), String)'))), toString(materialize(toDecimal64(1, 18)::Dynamic))
FROM remote('127.0.0.2', system.one);

SELECT '-- partial aggregation results from two shards';
SELECT toDecimal64(1, 18) AS c, count() FROM remote('127.0.0.{1,2}', system.one) GROUP BY c;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_05410) WHERE d64 = toDecimal64(1, 18) GROUP BY d64;

SELECT '-- the bounds of the precision and of the native integer';
SELECT toString(materialize(reinterpret(toInt64(999999999999999999), 'Decimal(18, 18)'))), toString(materialize(reinterpret(toInt64(1000000000000000000), 'Decimal(18, 18)'))),
    toString(materialize(reinterpret(toInt32(-2147483648), 'Decimal(9, 9)')))
FROM remote('127.0.0.2', system.one);
SELECT toString(materialize(reinterpret(toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'), 'Decimal(76, 70)')))
FROM remote('127.0.0.2', system.one);

DROP TABLE t_05410_dist;
DROP TABLE t_05410;
