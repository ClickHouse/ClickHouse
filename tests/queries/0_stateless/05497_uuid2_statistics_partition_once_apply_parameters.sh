#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL user-defined functions are server-global rather than per-database, so their names embed the test database.
f_parameter="${CLICKHOUSE_DATABASE}_05497_parameter"
f_scope="${CLICKHOUSE_DATABASE}_05497_scope"

$CLICKHOUSE_CLIENT "
SELECT '-- Cardinality statistics of UUID2 columns';
SET allow_statistics = 1;
DROP TABLE IF EXISTS t_stats;
CREATE TABLE t_stats (u UUID2 STATISTICS(uniq), v Nullable(UUID2) STATISTICS(uniq_v2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_stats SELECT generateUUIDv4(), if(number % 2, NULL, generateUUIDv4()) FROM numbers(10);
ALTER TABLE t_stats MATERIALIZE STATISTICS u, v SETTINGS mutations_sync = 2;
SELECT count() FROM t_stats WHERE u != toUUID2('00000000-0000-0000-0000-000000000000');
DROP TABLE t_stats;
"

echo "-- The partition value and its active Variant alternative come from one evaluation"
$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (u UUID1) ENGINE = MergeTree PARTITION BY u ORDER BY tuple();
"
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT "
    SET allow_suspicious_variant_types = 1;
    TRUNCATE TABLE t_partition;
    INSERT INTO t_partition VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0001-0000-000000000000');
    ALTER TABLE t_partition DROP PARTITION tuple(CAST(if(randConstant() % 2,
        CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2)),
        CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2)))
        AS Variant(UUID1, UUID2)));
    SELECT u FROM t_partition;
    "
done | sort | uniq -c | sed 's/^ *//'
$CLICKHOUSE_CLIENT "DROP TABLE t_partition"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $f_parameter;
DROP FUNCTION IF EXISTS $f_scope;
CREATE FUNCTION $f_parameter AS () -> 0.5;
CREATE FUNCTION $f_scope AS x -> defaultValueOfTypeName('UUID');

SELECT '-- A SQL UDF in the parameters of an aggregate function in APPLY keeps the name of the column';
SELECT \`quantileExact($f_parameter())(a)\` FROM (SELECT * APPLY(quantileExact($f_parameter())) FROM values('a UInt8', 1, 2));
DROP TABLE IF EXISTS t_apply;
CREATE TABLE t_apply ENGINE = Memory AS
SELECT \`quantileExact($f_parameter())(a)\` FROM (SELECT * APPLY(quantileExact($f_parameter())) FROM values('a UInt8', 1, 2));
SELECT * FROM t_apply;
DROP TABLE t_apply;

SELECT '-- A lambda of the query shadows a function in the body of a SQL UDF in APPLY';
SET uuid_type_version = 2;
WITH (x -> x) AS defaultValueOfTypeName
SELECT \`$f_scope(a)\` FROM (SELECT * APPLY $f_scope FROM values('a UInt8', 1));
CREATE TABLE t_apply ENGINE = Memory AS
WITH (x -> x) AS defaultValueOfTypeName
SELECT \`$f_scope(a)\` FROM (SELECT * APPLY $f_scope FROM values('a UInt8', 1));
SELECT * FROM t_apply;
DROP TABLE t_apply;

DROP FUNCTION $f_parameter;
DROP FUNCTION $f_scope;
"
