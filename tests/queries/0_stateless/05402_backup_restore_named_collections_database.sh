#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: `BACKUP TABLE system.named_collections` captures every SQL-created collection on the
#              server and RESTORE re-creates them, which would interfere with concurrent tests.
# no-replicated-database: named collections are server-global, not database-scoped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}_remote"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -m --query "
DROP DATABASE IF EXISTS ${db};
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS
    host = 'localhost', port = ${CLICKHOUSE_PORT_TCP}, database = '${CLICKHOUSE_DATABASE}';
CREATE DATABASE ${db} ENGINE = Remote(${collection});
"

echo "backup a database whose engine references a named collection, together with the collections"
${CLICKHOUSE_CLIENT} --query "
    BACKUP DATABASE ${db}, TABLE system.named_collections TO ${backup_name}
" | grep -c "BACKUP_CREATED"

echo "restore after dropping both: the collection must be restored before the database is created"
${CLICKHOUSE_CLIENT} -m --query "
DROP DATABASE ${db};
DROP NAMED COLLECTION ${collection};
"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name}" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = '${db}'"

${CLICKHOUSE_CLIENT} -m --query "
DROP DATABASE ${db};
DROP NAMED COLLECTION ${collection};
"
