#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the failpoints are process-global and would fail the `CREATE DATABASE` or `ATTACH DATABASE`
# of any concurrently running test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CREATE DATABASE` that fails after the database and its metadata were published, and whose rollback
# then fails to remove the metadata, leaves the database attached and its metadata on disk. The dependency
# on the named collection must survive, or the collection could be dropped and the next server start could
# not load the database.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_s3"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT attach_database_fail_after_load" 2>/dev/null ||:
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_database_fail_rollback_metadata_removal" 2>/dev/null ||:
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:1/'"

echo "--- CREATE DATABASE fails after publication and its rollback fails to remove the metadata ---"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT attach_database_fail_after_load"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT create_database_fail_rollback_metadata_removal"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = S3(${NC})" 2>&1 | grep -o -F "FAULT_INJECTED" | head -n 1
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT attach_database_fail_after_load"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_database_fail_rollback_metadata_removal"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${DB}'"

echo "--- the surviving database still holds the collection ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- the collection is released once the database is dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
DROP DATABASE ${DB};
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
