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
EOF

query_id="${CLICKHOUSE_DATABASE}_unknown_table_function"
$CLICKHOUSE_CLIENT --log_queries=1 --query_id "$query_id" --query "SELECT * FROM s3Clustr('http://127.0.0.1:1/x/', 'minio', 'TopSecretValue')" 2>&1 | grep -o -m1 UNKNOWN_FUNCTION
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
$CLICKHOUSE_CLIENT --query "SELECT query FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "SELECT countIf(message LIKE '%TopSecretValue%'), countIf(logger_name = 'executeQuery' AND message LIKE '%[HIDDEN]%') > 0 FROM system.text_log WHERE query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
