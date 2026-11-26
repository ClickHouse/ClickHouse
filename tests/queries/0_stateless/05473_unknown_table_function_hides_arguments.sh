#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table function the server does not know is rejected after the statement is logged, so its arguments are hidden.
while read -r query; do
    $CLICKHOUSE_FORMAT --oneline --query "$query"
done <<'EOF'
SELECT * FROM s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')
INSERT INTO TABLE FUNCTION s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue', 'CSV', 'x UInt64') SELECT 1
CREATE TABLE t AS s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')
SELECT * FROM unknown_view(param = 'TopSecretValue')
SELECT * FROM s3Clustr('TopSecretValue' = 1)
SELECT * FROM viewIfPermitted(SELECT 1 ELSE s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue'))
SELECT * FROM numbers(1)
SELECT s3Clustr('visible')
CREATE TABLE t AS s3Clustr('TopSecretValue')(1)
SELECT * FROM viewIfPermitted(SELECT 1 ELSE s3Clustr('TopSecretValue')(1))
CREATE TABLE t AS s3('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')(1)
CREATE TABLE t ENGINE = S3('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')(1)
CREATE DATABASE d ENGINE = Remote('127.0.0.1:1', 'db', 'user', 'TopSecretValue')(1)
BACKUP TABLE t TO S3('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')(1)
SELECT quantile(0.5)(number) FROM numbers(1)
EOF

query_id="${CLICKHOUSE_DATABASE}_unknown_table_function"
$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$query_id" --query "SELECT * FROM s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')" 2>&1 | grep -o -m1 UNKNOWN_FUNCTION
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
$CLICKHOUSE_CLIENT --query "SELECT query FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "SELECT countIf(message LIKE '%TopSecretValue%'), countIf(logger_name = 'executeQuery' AND message LIKE '%[HIDDEN]%') > 0 FROM system.text_log WHERE query_id = '$query_id' AND event_date >= yesterday() AND event_time >= now() - 600 SETTINGS enable_parallel_replicas = 0"

query_id="${CLICKHOUSE_DATABASE}_unknown_table_function_parameters"
$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$query_id" --query "CREATE TABLE t AS s3Clustr('TopSecretValue')(1)" 2>&1 | grep -o -m1 UNKNOWN_FUNCTION
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
$CLICKHOUSE_CLIENT --query "SELECT query FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "SELECT countIf(message LIKE '%TopSecretValue%'), countIf(logger_name = 'executeQuery' AND message LIKE '%[HIDDEN]%') > 0 FROM system.text_log WHERE query_id = '$query_id' AND event_date >= yesterday() AND event_time >= now() - 600 SETTINGS enable_parallel_replicas = 0"

# A call in the table function argument of a table function: an unknown name is hidden, a function stays visible.
while read -r query; do
    $CLICKHOUSE_FORMAT --oneline --query "$query"
done <<'EOF'
SELECT * FROM remote('127.0.0.1', s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue'))
SELECT * FROM remoteSecure('127.0.0.1', s3Clustr('TopSecretValue'), 'user', 'TopSecretValue')
SELECT * FROM remote(named_collection, database = s3Clustr('TopSecretValue'))
SELECT * FROM cluster(test_shard_localhost, s3Clustr('TopSecretValue'))
SELECT * FROM clusterAllReplicas(test_shard_localhost, s3Clustr('TopSecretValue'))
SELECT * FROM loop(s3Clustr('TopSecretValue'))
SELECT * FROM remote('127.0.0.1', concat('visible', ''), 'visible')
SELECT * FROM loop(concat('visible', ''), 'visible')
SELECT * FROM remote('127.0.0.1', numbers(1))
EOF

query_id="${CLICKHOUSE_DATABASE}_unknown_nested_table_function"
$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$query_id" --query "SELECT * FROM remote('127.0.0.1', s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue'))" 2>&1 | grep -o -m1 UNKNOWN_FUNCTION
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
$CLICKHOUSE_CLIENT --query "SELECT query, exception_code, position(exception, 'TopSecretValue') = 0 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "SELECT countIf(message LIKE '%TopSecretValue%'), countIf(logger_name = 'executeQuery' AND message LIKE '%[HIDDEN]%') > 0 FROM system.text_log WHERE query_id = '$query_id' AND event_date >= yesterday() AND event_time >= now() - 600 SETTINGS enable_parallel_replicas = 0"

# Parsed outside of a SELECT, such a call is reported as an unknown table function; a function still names the database.
nc="${CLICKHOUSE_DATABASE}_remote"
udf="${CLICKHOUSE_DATABASE}_database"
$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION $nc AS addresses_expr = '127.0.0.1', table = 'one'"
$CLICKHOUSE_CLIENT --query "CREATE FUNCTION $udf AS () -> 'system'"
query_id="${CLICKHOUSE_DATABASE}_unknown_nested_table_function_parsed"
while read -r query; do
    $CLICKHOUSE_CLIENT --log_queries=1 --query_id "$query_id" --query "$query" 2>&1 | grep -o -m1 -e 'Unknown table function s3Clustr' -e TopSecretValue
done <<EOF
DESCRIBE TABLE remote('127.0.0.1', s3Clustr('TopSecretValue'))
DESCRIBE TABLE cluster(test_shard_localhost, s3Clustr('TopSecretValue'))
CREATE TABLE t AS remoteSecure('127.0.0.1', s3Clustr('TopSecretValue'))
INSERT INTO FUNCTION clusterAllReplicas(test_shard_localhost, s3Clustr('TopSecretValue')) SELECT 1
CREATE TABLE t (x UInt8) ENGINE = Remote('127.0.0.1', s3Clustr('TopSecretValue'))
DESCRIBE TABLE remote($nc, database = s3Clustr('TopSecretValue'))
DESCRIBE TABLE remote($nc, db = s3Clustr('TopSecretValue'))
EOF
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
$CLICKHOUSE_CLIENT --query "SELECT countIf(type != 'QueryStart'), countIf(position(query, 'TopSecretValue') > 0 OR position(exception, 'TopSecretValue') > 0) FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "SELECT countIf(message LIKE '%TopSecretValue%') FROM system.text_log WHERE query_id = '$query_id' AND event_date >= yesterday() AND event_time >= now() - 600 SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "DESCRIBE TABLE remote('127.0.0.1', concat('sys', 'tem'), 'one')" | cut -f1,2
$CLICKHOUSE_CLIENT --query "DESCRIBE TABLE remote('127.0.0.1', $udf(), 'one')" | cut -f1,2
$CLICKHOUSE_CLIENT --query "DESCRIBE TABLE remote($nc, database = $udf())" | cut -f1,2
$CLICKHOUSE_CLIENT --query "DROP FUNCTION $udf"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION $nc"

# The named collection rejects the override after evaluating it, which must not record the arguments in system.errors.
locked="${CLICKHOUSE_DATABASE}_remote_locked"
$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION $locked AS addresses_expr = '127.0.0.1', database = 'system' NOT OVERRIDABLE, table = 'one'"
for key in database db; do
    query_id="${CLICKHOUSE_DATABASE}_unknown_nested_table_function_locked_$key"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "DESCRIBE TABLE remote($locked, $key = s3Clustr('TopSecretValue'))" 2>&1 | grep -o -m1 -e 'Unknown table function s3Clustr' -e TopSecretValue
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.errors WHERE query_id = '$query_id' AND position(last_error_message, 'TopSecretValue') > 0"
done
# A computed key is resolved as the named collection resolves it; a sharding key outside that form is not evaluated.
n=0
for key in "concat('data', 'base')" "lower('DB')"; do
    n=$((n + 1))
    query_id="${CLICKHOUSE_DATABASE}_unknown_nested_table_function_locked_computed_$n"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "DESCRIBE TABLE remote($locked, $key = s3Clustr('TopSecretValue'))" 2>&1 | grep -o -m1 -e 'Unknown table function s3Clustr' -e TopSecretValue
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM system.errors WHERE query_id = '$query_id' AND position(last_error_message, 'TopSecretValue') > 0"
done
$CLICKHOUSE_CLIENT --query "SELECT count() FROM remote('127.0.0.{1,2}', system.one, rand() % 2 = 0)"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION $locked"
