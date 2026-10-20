#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `create_database_pause_after_engine` failpoint is process-global and would pause the
# first `CREATE DATABASE` of any concurrently running test.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The database engine registers its named collection dependency while it is constructed, before the
# database is published. A concurrent `DROP NAMED COLLECTION` must not take the entry of such an
# in-flight `CREATE DATABASE` for a leftover of a failed create: the create would then succeed with
# the collection gone, and the database could not be attached at the next server start. The drop
# synchronizes on the `DDLGuard` of the database name, which the creating query holds for the whole window.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_s3"

drop_log="${CLICKHOUSE_TMP}/drop_${CLICKHOUSE_DATABASE}.log"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_database_pause_after_engine" 2>/dev/null ||:
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:1/'"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT create_database_pause_after_engine"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = S3(${NC})" &
create_pid=$!

# The create has registered its dependency and paused before publishing the database.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_database_pause_after_engine PAUSE"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" > "$drop_log" 2>&1 &
drop_pid=$!

# Release the create once the drop either waits on the guard (visible in the process list) or has
# already terminated (only possible if it wrongly pruned the dependency and dropped the collection).
while kill -0 "$drop_pid" 2>/dev/null \
    && [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query LIKE 'DROP NAMED COLLECTION ${NC}%'") -eq 0 ]]
do
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_database_pause_after_engine"

wait "$create_pid"
wait "$drop_pid" ||:
if grep -q "NAMED_COLLECTION_IS_USED" "$drop_log"
then
    echo "NAMED_COLLECTION_IS_USED"
else
    cat "$drop_log"
fi

${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
SELECT count() FROM system.databases WHERE name = '${DB}';
DROP DATABASE ${DB};
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
