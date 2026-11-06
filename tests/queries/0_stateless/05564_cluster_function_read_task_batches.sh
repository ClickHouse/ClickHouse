#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${S3_DIR}/file_{_partition_id}.tsv', 'TSV', 'a UInt64')
    PARTITION BY a % 30
    SELECT number FROM numbers(300)"

for tasks_per_request in 1 3 1000; do
    query_id="${CLICKHOUSE_DATABASE}_${tasks_per_request}_${RANDOM}${RANDOM}"
    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" --query "
        SELECT ${tasks_per_request}, count(), sum(a)
        FROM s3Cluster('test_cluster_two_shards_localhost', '${S3_DIR}/file_*.tsv', 'TSV', 'a UInt64')
        SETTINGS cluster_function_tasks_per_request = ${tasks_per_request}"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            count(),
            if(${tasks_per_request} = 1, toString(sum(ProfileEvents['ReadTaskRequestsSent'])), toString(sum(ProfileEvents['ReadTaskRequestsSent']) < 30))
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = '${query_id}'"
done
