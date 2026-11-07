#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `drop_database_fail_before_metadata_removal` failpoint is process-global and would
# fail the `DROP DATABASE` of any concurrently running test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `DROP DATABASE` that fails to remove the `CREATE DATABASE` metadata after the engine's drop went
# through leaves the database out of the catalog, but the metadata is still replayed by a later
# `ATTACH DATABASE` or server start, so it has to keep the named collection it references protected.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_s3"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT drop_database_fail_before_metadata_removal" 2>/dev/null ||:
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:1/';
CREATE DATABASE ${DB} ENGINE = S3(${NC});
"

echo "--- the metadata removal of DROP DATABASE fails ---"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT drop_database_fail_before_metadata_removal"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}" 2>&1 | grep -o -F "FAULT_INJECTED" | head -n 1
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT drop_database_fail_before_metadata_removal"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${DB}'"

echo "--- the surviving metadata still holds the collection ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- the database is attached back from the metadata and the collection is released once it is dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${DB};
SELECT count() FROM system.databases WHERE name = '${DB}';
DROP DATABASE ${DB};
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
