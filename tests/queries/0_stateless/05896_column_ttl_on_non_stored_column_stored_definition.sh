#!/usr/bin/env bash
# A table that an older server stored with a TTL on an ALIAS or EPHEMERAL column still loads and can be altered: only an
# ALTER or a full-definition ATTACH that writes such a TTL is refused (05895_column_ttl_on_non_stored_column covers CREATE).
#
# Such a definition can no longer be written through SQL, so the stored metadata of a table created without the TTLs is
# edited into the form the older server wrote.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr, e UInt64 EPHEMERAL 0, v UInt64 DEFAULT e)
ENGINE = MergeTree ORDER BY id;
INSERT INTO db.t (id, dt, arr, e) VALUES (1, '2100-01-01', [1, 2, 3], 5);
CREATE TABLE db.r (id UInt64, al UInt64 ALIAS id, dt Date) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 YEAR TO db.r AS SELECT 1 AS id, toDate('2100-01-01') AS dt;
SYSTEM WAIT VIEW db.mv;
CREATE TABLE db.a (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db.e (id UInt64, dt Date, e UInt64 EPHEMERAL 0, v UInt64 DEFAULT e) ENGINE = MergeTree ORDER BY id;
"

metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t.sql')
sed -i 's/ ALIAS arr,$/ ALIAS arr TTL dt + toIntervalMonth(1),/; s/ EPHEMERAL 0,$/ EPHEMERAL 0 TTL dt + toIntervalMonth(1),/' "${metadata_file}"
# Without this the load below would run against an unmodified definition, i.e. assert nothing.
grep -c -F -e 'ALIAS arr TTL dt + toIntervalMonth(1),' -e 'EPHEMERAL 0 TTL dt + toIntervalMonth(1),' "${metadata_file}"
r_metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='r.sql')
sed -i 's/ ALIAS id,$/ ALIAS id TTL dt + toIntervalMonth(1),/' "${r_metadata_file}"
grep -c -F 'ALIAS id TTL dt + toIntervalMonth(1),' "${r_metadata_file}"
a_metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='a.sql')
sed -i 's/ ALIAS arr$/ ALIAS arr TTL dt + toIntervalMonth(1)/' "${a_metadata_file}"
grep -c -F 'ALIAS arr TTL dt + toIntervalMonth(1)' "${a_metadata_file}"
e_metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='e.sql')
sed -i 's/ EPHEMERAL 0,$/ EPHEMERAL 0 TTL dt + toIntervalMonth(1),/' "${e_metadata_file}"
grep -c -F 'EPHEMERAL 0 TTL dt + toIntervalMonth(1),' "${e_metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT id, arr, al, v FROM db.t;
-- The refresh re-creates the target from its stored definition.
SYSTEM REFRESH VIEW db.mv;
SYSTEM WAIT VIEW db.mv;
SELECT id, al FROM db.r;
SELECT countSubstrings(create_table_query, 'ALIAS id TTL dt + toIntervalMonth(1)') FROM system.tables WHERE database = 'db' AND name = 'r';
-- The view refreshes at every start of clickhouse-local, so no later run may exit while a refresh is in progress.
DROP TABLE db.mv;
-- Inserted after the TTLs are in the definition, so the parts carry TTL infos for the merge to apply.
INSERT INTO db.a VALUES (1, '2000-01-01', [1, 2, 3]), (2, '2100-01-01', [4]);
OPTIMIZE TABLE db.a FINAL;
SELECT id, arr, al FROM db.a ORDER BY id;
INSERT INTO db.e (id, dt, e) VALUES (1, '2000-01-01', 5);
OPTIMIZE TABLE db.e FINAL;
SELECT id, v FROM db.e;
SELECT countSubstrings(create_table_query, ' TTL dt + toIntervalMonth(1)') FROM system.tables WHERE database = 'db' AND name = 't';
DETACH TABLE db.t;
ATTACH TABLE db.t;
SELECT id, arr, al, v FROM db.t;
-- A subcolumn of the ALIAS column read through a structure inferred from the table, which drops the column TTLs.
CREATE TABLE db.m ENGINE = Merge('db', '^t$');
SELECT al, al.size0 FROM db.m;
DROP TABLE db.m;
CREATE TABLE db.b ENGINE = Buffer(db, t, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT al, al.size0 FROM db.b;
DROP TABLE db.b;
-- An ALTER that leaves such a TTL as it was is not refused, nor one that only renames the column or a column the TTL uses.
ALTER TABLE db.t ADD COLUMN x UInt64;
ALTER TABLE db.t COMMENT COLUMN al 'c';
ALTER TABLE db.t RENAME COLUMN dt TO dt2;
ALTER TABLE db.t RENAME COLUMN al TO al2;
ALTER TABLE db.t MODIFY COLUMN al2 Array(UInt64) ALIAS arr;
SELECT name, default_kind, comment FROM system.columns WHERE database = 'db' AND table = 't' ORDER BY position;
SELECT countSubstrings(create_table_query, ' TTL dt2 + toIntervalMonth(1)') FROM system.tables WHERE database = 'db' AND name = 't';
"

# An ALTER that changes such a TTL, or writes a new one, is refused.
for query in \
    "ALTER TABLE db.t MODIFY COLUMN al2 TTL dt2 + INTERVAL 2 MONTH" \
    "ALTER TABLE db.t DROP COLUMN al2, ADD COLUMN al2 Array(UInt64) ALIAS arr TTL dt2 + INTERVAL 1 MONTH"
do
    $CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "${query}" 2>&1 | grep -c "Cannot set TTL for ALIAS column \`al2\`: it is not stored"
done

# A CREATE AS that copies such a TTL is refused.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "CREATE TABLE db.copy AS db.t" 2>&1 | grep -c -E "Cannot set TTL for (ALIAS|EPHEMERAL) column \`(al2|e)\`: it is not stored"
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "EXISTS TABLE db.copy"

# The same definition is refused when it comes with the ATTACH query rather than from the stored metadata.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ATTACH TABLE db.t3 UUID '05896000-0000-0000-0000-000000000003' (id UInt64, dt Date, al UInt64 ALIAS id TTL dt + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY id;
" 2>&1 | grep -c "Cannot set TTL for ALIAS column \`al\`: it is not stored"

# REMOVE TTL is the way out.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
ALTER TABLE db.t MODIFY COLUMN al2 REMOVE TTL;
ALTER TABLE db.t MODIFY COLUMN e REMOVE TTL;
SELECT countSubstrings(create_table_query, 'TTL') FROM system.tables WHERE database = 'db' AND name = 't';
SELECT id, arr, al2, v FROM db.t;
"

rm -rf "${WORKING_DIR}"
