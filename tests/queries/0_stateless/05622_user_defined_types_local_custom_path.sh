#!/usr/bin/env bash

# `clickhouse-local` loads the user-defined types from the configured storage, also when `user_defined_path`
# points outside the default `<path>/user_defined` directory, so a type created by one invocation is known to
# the next one, including to the persisted expressions of its tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/data" "${WORK_DIR}/types"

cat > "${WORK_DIR}/config.xml" <<XML
<clickhouse>
    <user_defined_path>${WORK_DIR}/types/</user_defined_path>
</clickhouse>
XML

function local_query()
{
    $CLICKHOUSE_LOCAL --path "${WORK_DIR}/data" --config-file "${WORK_DIR}/config.xml" --query "$1"
}

local_query "CREATE TYPE ExternalId AS UInt64; CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY CAST(x, 'ExternalId'); INSERT INTO t VALUES (1), (2);"
ls "${WORK_DIR}/types"
test -e "${WORK_DIR}/data/user_defined" && echo "the default directory exists"

local_query "SELECT toTypeName(CAST(1, 'ExternalId')); SELECT * FROM t ORDER BY x;"

rm -rf "${WORK_DIR}"
