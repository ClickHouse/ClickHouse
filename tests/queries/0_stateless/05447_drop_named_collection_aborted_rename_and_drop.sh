#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the failpoints are process-global and would make the `DROP TABLE`, `RENAME TABLE` or
# `RENAME DATABASE` of a concurrently running test fail.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local, and the test creates a database with the `Ordinary` engine.
# no-ordinary-database: the failpoint of `DROP` is in `DatabaseAtomic`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# DDL queries that fail halfway must keep the named collections protected while a definition that
# references them survives: otherwise `DROP NAMED COLLECTION` succeeds, and the next load of that
# definition fails with `NAMED_COLLECTION_DOESNT_EXIST`.

NC="nc_${CLICKHOUSE_DATABASE}"
ORD="${CLICKHOUSE_DATABASE}_ordinary"
ATOM="${CLICKHOUSE_DATABASE}_atomic"
PG="${CLICKHOUSE_DATABASE}_pg"

function cleanup()
{
    for failpoint in database_atomic_drop_table_fail_before_replace database_rename_table_fail_before_create_in_destination \
        database_rename_table_fail_before_removing_source_metadata database_atomic_rename_database_fail_after_publish
    do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${failpoint}" 2>/dev/null ||:
    done
}
trap cleanup EXIT

function drop_collection()
{
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"
}

function fail_with()
{
    local failpoint=$1
    local query=$2
    ${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${failpoint}"
    ${CLICKHOUSE_CLIENT} -q "${query}" 2>&1 | grep -o -m1 "FAULT_INJECTED"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${failpoint}"
}

${CLICKHOUSE_CLIENT} --send_logs_level=error -m -q "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${ORD} ENGINE = Ordinary;
CREATE DATABASE ${ATOM} ENGINE = Atomic;
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
"

echo "--- a failed DROP of a never loaded dictionary ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${ATOM}.d (id UInt64, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME ${NC}))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 1);
"
fail_with database_atomic_drop_table_fail_before_replace "DROP DICTIONARY ${ATOM}.d"
drop_collection
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY ${ATOM}.d"

echo "--- a RENAME that fails before the destination publishes the table ---"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${ORD}.t (x UInt32) ENGINE = URL(${NC})"
fail_with database_rename_table_fail_before_create_in_destination "RENAME TABLE ${ORD}.t TO ${ATOM}.t"
drop_collection
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${ORD}.t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${ORD}.t"

echo "--- a RENAME that fails to remove the source metadata after the destination published the table ---"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${ORD}.t2 (x UInt32) ENGINE = URL(${NC})"
fail_with database_rename_table_fail_before_removing_source_metadata "RENAME TABLE ${ORD}.t2 TO ${ATOM}.t2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${ATOM}.t2"
drop_collection
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${ORD}.t2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${ORD}.t2"

echo "--- the same for a never loaded dictionary ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${ORD}.d2 (id UInt64, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME ${NC}))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 1);
"
fail_with database_rename_table_fail_before_removing_source_metadata "RENAME DICTIONARY ${ORD}.d2 TO ${ATOM}.d2"
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY ${ATOM}.d2"
drop_collection
${CLICKHOUSE_CLIENT} -q "ATTACH DICTIONARY ${ORD}.d2"
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY ${ORD}.d2"

echo "--- a RENAME DATABASE that fails after the database is published under the new name ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC}_pg AS host = 'localhost', port = 1, database = 'db', user = 'u', password = 'p';
SET allow_experimental_database_materialized_postgresql = 1;
CREATE DATABASE ${PG} ENGINE = MaterializedPostgreSQL(${NC}_pg);
"
fail_with database_atomic_rename_database_fail_after_publish "RENAME DATABASE ${PG} TO ${PG}_renamed"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}_pg" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"
# The engine complains that it cannot drop its replication slot on a server it cannot reach.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${PG}_renamed" 2>/dev/null

echo "--- the collections are released once the definitions are gone ---"
${CLICKHOUSE_CLIENT} -m -q "
DROP NAMED COLLECTION ${NC};
DROP NAMED COLLECTION ${NC}_pg;
SELECT count() FROM system.named_collections WHERE name IN ('${NC}', '${NC}_pg');
DROP DATABASE ${ORD};
DROP DATABASE ${ATOM};
"
