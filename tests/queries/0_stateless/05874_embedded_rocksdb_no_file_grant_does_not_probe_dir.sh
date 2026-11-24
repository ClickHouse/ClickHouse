#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb, no-replicated-database
# no-fasttest: EmbeddedRocksDB requires libraries
# no-replicated-database: on a replicated database the DDL runs with no user, so the in-storage
# `FILE` check is not applied (https://github.com/ClickHouse/ClickHouse/issues/111561).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `read_only` `EmbeddedRocksDB` table over an explicit `rocksdb_dir` needs `READ ON FILE` when the
# directory exists and `READ, WRITE ON FILE` when it would have to be created. A user without any
# `FILE` grant must get the same denial in both cases: otherwise the message tells whether a
# directory under `user_files` exists.

NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"
USER="u_${NAME}"
EXISTING_DIR="${NAME}_existing"
MISSING_DIR="${NAME}_missing"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON EmbeddedRocksDB TO ${USER}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE existing (k String) ENGINE = EmbeddedRocksDB(0, '${EXISTING_DIR}', 0) PRIMARY KEY k"

for dir in "${EXISTING_DIR}" "${MISSING_DIR}"
do
    ${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE probe (k String) ENGINE = EmbeddedRocksDB(0, '${dir}', 1) PRIMARY KEY k" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z, ]+ ON FILE" | head -n1
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE existing SYNC SETTINGS ignore_drop_queries_probability = 0"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
[ -n "${CLICKHOUSE_USER_FILES}" ] && rm -rf "${CLICKHOUSE_USER_FILES:?}/${EXISTING_DIR}"
exit 0
