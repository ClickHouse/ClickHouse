#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Azure Blob Storage support.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dynamic disk with a `from_env` value and explicit credentials keeps its identity whatever the
# `s3_allow_server_credentials_in_user_queries` of the session that reuses or re-attaches it.

KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
# `readonly`, `container_already_exists` and `skip_access_check` keep the disk from contacting the endpoint.
ARGS="type = azure_blob_storage, storage_account_url = 'https://localhost:11111/devstoreaccount1',
    container_name = 'cont${CLICKHOUSE_DATABASE//_/}', account_name = 'devstoreaccount1', account_key = '${KEY}',
    readonly = 1, container_already_exists = 1, skip_access_check = 1, common_key_prefix_for_azure = 'from_env PATH'"
NAMED="disk(name = 'disk_${CLICKHOUSE_DATABASE}', ${ARGS})"
UNNAMED="disk(${ARGS})"

OPT_IN="--s3_allow_server_credentials_in_user_queries=1"

run()
{
    local out
    if out=$(${CLICKHOUSE_CLIENT} --dynamic_disk_allow_from_env=1 "$@" 2>&1); then echo "OK"; else echo "$out" | grep -oE '\([A-Z_]+\)' | head -1; fi
}

run -q "CREATE TABLE t_named (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${NAMED}" > /dev/null
echo "same named disk, session with the opt-in: $(run ${OPT_IN} -q "CREATE TABLE t_named_2 (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${NAMED}")"

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_named"
echo "re-attach, session without the opt-in: $(run -q "ATTACH TABLE t_named")"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_named"
echo "re-attach, session with the opt-in: $(run ${OPT_IN} -q "ATTACH TABLE t_named")"

run -q "CREATE TABLE t_unnamed (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = ${UNNAMED}" > /dev/null
before=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths FROM system.tables WHERE database = currentDatabase() AND name = 't_unnamed'")
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_unnamed"
run ${OPT_IN} -q "ATTACH TABLE t_unnamed" > /dev/null
after=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths FROM system.tables WHERE database = currentDatabase() AND name = 't_unnamed'")
echo "unnamed disk kept after re-attach with the opt-in: $([ "$before" = "$after" ] && echo 1 || echo 0)"

# A detached table is not dropped with the database.
run -q "ATTACH TABLE IF NOT EXISTS t_named" > /dev/null
run -q "ATTACH TABLE IF NOT EXISTS t_unnamed" > /dev/null
