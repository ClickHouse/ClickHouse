#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

PG_USER="postgresql_user_05843_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${PG_USER}"
    ${CLICKHOUSE_CLIENT} --query 'DROP TABLE IF EXISTS `function`, `FuNcTiOn`, `copy.target` SYNC'
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"
${CLICKHOUSE_CLIENT} --query '
CREATE TABLE `function` (id UInt32) ENGINE = Memory;
CREATE TABLE `copy.target` (id UInt32) ENGINE = Memory;
'

PSQL=(psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" --dbname "${CLICKHOUSE_DATABASE}" --username "${PG_USER}" --no-psqlrc --no-align --tuples-only --quiet --set ON_ERROR_STOP=1)

# Both quoted and bare targets must remain identifiers in the generated `INSERT`.
printf '1\n' | "${PSQL[@]}" -c 'COPY function FROM STDIN'
printf '2\n' | "${PSQL[@]}" -c 'COPY "function" (id) FROM STDIN'
${CLICKHOUSE_CLIENT} --query 'SELECT id FROM `function` ORDER BY id; DROP TABLE `function` SYNC'

# Use the case variant sequentially so the test also works on case-insensitive filesystems.
${CLICKHOUSE_CLIENT} --query 'CREATE TABLE `FuNcTiOn` (id UInt32) ENGINE = Memory'
printf '3\n' | "${PSQL[@]}" -c 'COPY "FuNcTiOn" FROM STDIN'

# Quote each component separately, preserving a literal dot inside the table name.
printf '4\n' | "${PSQL[@]}" -c "COPY \"${CLICKHOUSE_DATABASE}\".\"copy.target\" FROM STDIN"

"${PSQL[@]}" -c 'COPY "FuNcTiOn" TO STDOUT'
"${PSQL[@]}" -c "COPY \"${CLICKHOUSE_DATABASE}\".\"copy.target\" TO STDOUT"

# Quoting the catalog prefix must preserve the handler's emulated-catalog lookup.
catalog_copy=$("${PSQL[@]}" -c 'COPY pg_catalog.pg_type (oid) TO STDOUT' | LC_ALL=C sort)
catalog_select=$("${PSQL[@]}" -c 'SELECT oid FROM pg_catalog.pg_type' | LC_ALL=C sort)
test -n "$catalog_copy"
test "$catalog_copy" = "$catalog_select"
echo 'catalog lookup preserved'
