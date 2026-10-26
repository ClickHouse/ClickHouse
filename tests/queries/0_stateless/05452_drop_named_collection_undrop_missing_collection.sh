#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the named collection is created on one replica only, so the other replicas cannot create the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `UNDROP TABLE` moves the metadata of the dropped table back into the database before attaching it, and
# the metadata stays there if the attach fails. When the attach fails because the named collection does not
# exist, nothing was registered for the table, but the collection must still be protected once it is
# created again: the restored metadata references it, and the next `ATTACH` (or server start) needs it.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_1"

# A table without data on disk is removed from the queue of dropped tables at once and cannot be undropped,
# unless the catalog holds the stand-in of a `lazy_load_tables` database for it: it appears when the database
# is loaded, hence the re-attach.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:11111/', format = 'CSV';
CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${DB}.t (a UInt8) ENGINE = URL(${NC});
DETACH DATABASE ${DB};
ATTACH DATABASE ${DB};
DROP TABLE ${DB}.t SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0;
DROP NAMED COLLECTION ${NC};
"

${CLICKHOUSE_CLIENT} -q "UNDROP TABLE ${DB}.t" 2>&1 | grep -o -m1 "NAMED_COLLECTION_DOESNT_EXIST"

${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:11111/', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE ${DB}.t;
SELECT name FROM system.tables WHERE database = '${DB}' AND name = 't';
DROP DATABASE ${DB} SYNC;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
