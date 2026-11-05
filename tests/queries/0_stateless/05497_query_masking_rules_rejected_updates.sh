#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

rule="${CLICKHOUSE_DATABASE}_rejected_update"
secret="qmr_rejected_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "drop query masking rule if exists \`${rule}\`"
}

trap cleanup EXIT
cleanup
$CLICKHOUSE_CLIENT -q "create query masking rule \`${rule}\` regexp '${secret}' replace '[qmr_preserved]'"

for operation in 'alter' 'create or replace'; do
    if [[ "$operation" == 'alter' ]]; then
        statement="alter query masking rule"
    else
        statement="create query masking rule or replace"
    fi

    $CLICKHOUSE_CLIENT -m -q "${statement} \`${rule}\` regexp '('; -- { serverError CANNOT_COMPILE_REGEXP }"

    query_id="${rule}_${operation// /_}"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --log_queries=1 --log_queries_probability=1 \
        -q "select '${secret}' format Null"

    if error=$($CLICKHOUSE_CLIENT -q "select throwIf(1, '${secret}')" 2>&1); then
        echo 'Expected an exception'
        exit 1
    fi
    printf '%s\n' "$error" | grep -v '^(query: ' | grep -oF -e '[qmr_preserved]' -e "$secret" | LC_ALL=C sort -u

    $CLICKHOUSE_CLIENT -m -q "
        system flush logs query_log;
        select count(), countIf(position(query, '[qmr_preserved]') > 0), countIf(position(query, '${secret}') > 0)
        from system.query_log
        where current_database = currentDatabase() and query_id = '${query_id}' and type = 'QueryFinish';
    "
done

