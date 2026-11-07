#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `database_remove_detached_permanently_flag_fail` failpoint is process-global and would
# make the first `CREATE TABLE` of any concurrently running test fail.
# no-replicated-database: the test creates a database with the `Ordinary` engine.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `RENAME TABLE` from an `Atomic` into an `Ordinary` database creates the table in the destination,
# which can publish the table and its metadata and throw afterwards. The table then lives under its
# destination identity (the name, without a UUID), while its named collection dependency kept the
# source identity, which no live table resolves to. `DROP NAMED COLLECTION` used to prune that entry
# and drop the collection while the destination metadata still referenced it.

NC="nc_${CLICKHOUSE_DATABASE}"
ORD="${CLICKHOUSE_DATABASE}_ordinary"
ATOM="${CLICKHOUSE_DATABASE}_atomic"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_remove_detached_permanently_flag_fail" 2>/dev/null ||:
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --send_logs_level=error -m -q "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${ORD} ENGINE = Ordinary;
CREATE DATABASE ${ATOM} ENGINE = Atomic;
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${ATOM}.t (x UInt32) ENGINE = URL(${NC});
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_remove_detached_permanently_flag_fail"
${CLICKHOUSE_CLIENT} -q "RENAME TABLE ${ATOM}.t TO ${ORD}.t" 2>&1 | grep -o -m1 "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_remove_detached_permanently_flag_fail"

# The destination published the table despite the exception.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${ORD}' AND name = 't'"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"

# The source metadata was left behind as well, so the collection stays protected until it is gone.
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${ORD}.t"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
DROP DATABASE ${ATOM};
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP DATABASE ${ORD};
"
