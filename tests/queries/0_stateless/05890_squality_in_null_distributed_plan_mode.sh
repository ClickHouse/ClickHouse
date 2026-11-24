#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SELECT number, ifNull(toString(p), 'NULL')
    FROM
    (
        SELECT number, number IN (1, NULL) AS p FROM numbers(3)
        SETTINGS in_null_as_unknown = 1
    )
    ORDER BY number
    SETTINGS in_null_as_unknown = 0, transform_null_in = 0,
        make_distributed_plan = 1, distributed_plan_execute_locally = 1,
        distributed_plan_fallback_to_local_execution = 0,
        enable_cascades_optimizer = 0, enable_parallel_replicas = 0
"

${CLICKHOUSE_CLIENT} --query "
    SELECT number, ifNull(toString(p), 'NULL')
    FROM
    (
        SELECT number,
            CAST(if(number = 0, NULL, toUInt64(1)) AS Variant(UInt64, String)) IN tuple() AS p
        FROM numbers(2)
        SETTINGS in_null_as_unknown = 0
    )
    ORDER BY number
    SETTINGS in_null_as_unknown = 1, transform_null_in = 0,
        make_distributed_plan = 1, distributed_plan_execute_locally = 1,
        distributed_plan_fallback_to_local_execution = 0,
        enable_cascades_optimizer = 0, enable_parallel_replicas = 0
"

