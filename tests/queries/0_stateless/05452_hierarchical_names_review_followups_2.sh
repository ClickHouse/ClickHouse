#!/usr/bin/env bash
# Tags: no-replicated-database, zookeeper
# Tag no-replicated-database: `ON CLUSTER` is not allowed for a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): an exact qualifier is preferred over one crossing the boundary
# between the database and the table; the source of `CLONE AS` is resolved before the table is created; the names in
# `MODIFY QUERY` are bound before the query is replayed `ON CLUSTER` or by a `Replicated` database; parameterized views;
# the internal database of the temporary tables; the old analyzer keeps the split of a bound `joinGet` / `dictGet` name;
# the stored form of a bound name is understood by the dependency checks and by the renaming of a restore; the name
# expressions that depend on the context of the call are not frozen in a view.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E '\(([A-Z_]+)\)' | head -1
}

run "CREATE DATABASE \`${db}.b\`"

echo '--- an exact qualifier is preferred over a quoted one crossing the boundary between the database and the table'
run "CREATE TABLE ${db}.\`b.c\` (id UInt8) ENGINE = Memory"
run "CREATE TABLE \`${db}.b\`.c (id UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\`b.c\` VALUES (1)"
run "INSERT INTO \`${db}.b\`.c VALUES (2)"
run "SELECT \`${db}.b\`.c.id FROM ${db}.\`b.c\` CROSS JOIN \`${db}.b\`.c"
run "SELECT ${db}.\`b.c\`.id FROM ${db}.\`b.c\` CROSS JOIN \`${db}.b\`.c"
run "SELECT \`${db}.b\`.c.id FROM ${db}.\`b.c\` CROSS JOIN \`${db}.b\`.c SETTINGS single_join_prefer_left_table = 0"
run "SELECT \`${db}.b\`.c.id FROM \`${db}.b\`.c CROSS JOIN ${db}.\`b.c\`"

echo '--- the source of CLONE AS is the table the name denotes before the new table is created'
run "CREATE TABLE ${db}.\`b.t\` (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "INSERT INTO ${db}.\`b.t\` VALUES (42)"
run "CREATE TABLE \`${db}.b\`.t CLONE AS ${db}.b.t"
run "SELECT * FROM \`${db}.b\`.t"
run "SELECT * FROM ${db}.\`b.t\`"

echo '--- MODIFY QUERY ON CLUSTER binds the names of its query on the initiator'
run "CREATE TABLE ${db}.\`ns.t\` (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "CREATE TABLE ${db}.dst (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "CREATE TABLE ${db}.other (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "CREATE MATERIALIZED VIEW ${db}.mv TO ${db}.dst AS SELECT * FROM ${db}.other"
run "ALTER TABLE ${db}.mv ON CLUSTER test_shard_localhost MODIFY QUERY SELECT * FROM ${db}.ns.t FORMAT Null"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = '${db}' AND name = 'mv'"
run "INSERT INTO ${db}.\`ns.t\` VALUES (7)"
run "SELECT * FROM ${db}.dst"

echo '--- and so does MODIFY QUERY in a Replicated database'
run "CREATE DATABASE \`${db}_repl\` ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db}_repl', 's1', 'r1')"
run "SET distributed_ddl_output_mode = 'none'; CREATE TABLE \`${db}_repl\`.\`ns.t\` (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "SET distributed_ddl_output_mode = 'none'; CREATE TABLE \`${db}_repl\`.dst (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "SET distributed_ddl_output_mode = 'none'; CREATE TABLE \`${db}_repl\`.other (x UInt8) ENGINE = MergeTree ORDER BY ()"
run "SET distributed_ddl_output_mode = 'none'; CREATE MATERIALIZED VIEW \`${db}_repl\`.mv TO \`${db}_repl\`.dst AS SELECT * FROM \`${db}_repl\`.other"
run "ALTER TABLE \`${db}_repl\`.mv MODIFY QUERY SELECT * FROM ${db}_repl.ns.t FORMAT Null"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = '${db}_repl' AND name = 'mv'"
run "INSERT INTO \`${db}_repl\`.\`ns.t\` VALUES (8)"
run "SELECT * FROM \`${db}_repl\`.dst"
run "DROP DATABASE \`${db}_repl\` SYNC"

echo '--- parameterized views have hierarchical names too'
run "CREATE VIEW ${db}.\`ns.v\` AS SELECT {x:UInt8} AS x"
run "SELECT * FROM ${db}.ns.v(x = 1)"
run "SELECT * FROM ${db}.ns.v(x = 2) SETTINGS enable_analyzer = 0"
run "USE ${db}.ns; SELECT * FROM v(x = 3)"
run "USE ${db}.ns; SELECT * FROM v(x = 4) SETTINGS enable_analyzer = 0"
run "DESCRIBE ${db}.ns.v(x = 5)"

echo '--- the temporary tables of another session are not reachable by an explicit name'
session="${CLICKHOUSE_TEST_UNIQUE_NAME}_session"
url="${CLICKHOUSE_URL}&session_id=${session}"
echo "CREATE TEMPORARY TABLE tmp_secret (secret UInt8)" | ${CLICKHOUSE_CURL} -sSg "$url" -d @-
uuid=$(echo "SELECT uuid FROM system.tables WHERE name = 'tmp_secret' AND is_temporary" | ${CLICKHOUSE_CURL} -sSg "$url" -d @-)
run "SELECT hasColumnInTable('_temporary_and_external_tables', '_tmp_${uuid}', 'secret')" | error_code
run "CREATE TABLE ${db}.tmp_copy AS _temporary_and_external_tables.\`_tmp_${uuid}\`" | error_code
echo "DROP TEMPORARY TABLE tmp_secret" | ${CLICKHOUSE_CURL} -sSg "$url" -d @-

echo '--- the old analyzer keeps the split of a bound name of joinGet and dictGet'
run "CREATE TABLE ${db}.\`ns.j\` (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.\`ns.j\` VALUES (1, 'original')"
run "CREATE DATABASE \`${db}.ns\`"
run "CREATE TABLE \`${db}.ns\`.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO \`${db}.ns\`.j VALUES (1, 'other')"
run "CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k"
run "INSERT INTO ${db}.src VALUES (1, 'original')"
run "CREATE TABLE \`${db}.ns\`.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k"
run "INSERT INTO \`${db}.ns\`.src VALUES (1, 'other')"
run "CREATE DICTIONARY ${db}.\`ns.d\` (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}')) LAYOUT(FLAT()) LIFETIME(0)"
run "CREATE DICTIONARY \`${db}.ns\`.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}.ns')) LAYOUT(FLAT()) LIFETIME(0)"
run "SELECT joinGet(${db}.\`ns.j\`, 'v', toUInt64(1)), dictGet(${db}.\`ns.d\`, 'v', toUInt64(1)) SETTINGS enable_analyzer = 0"
run "SELECT joinGet(\`${db}.ns\`.j, 'v', toUInt64(1)), dictGet(\`${db}.ns\`.d, 'v', toUInt64(1)) SETTINGS enable_analyzer = 0"
run "SELECT joinGet(${db}.\`ns.j\`, 'v', toUInt64(1)), dictGet(${db}.\`ns.d\`, 'v', toUInt64(1)) SETTINGS enable_analyzer = 1"
run "DROP DICTIONARY \`${db}.ns\`.d"
run "DROP TABLE \`${db}.ns\`.src"
run "DROP TABLE \`${db}.ns\`.j"
run "DROP DATABASE \`${db}.ns\`"

echo '--- a stored bound name is a dependency of the view'
run "USE ${db}; CREATE VIEW ${db}.v_dep AS SELECT joinGet('ns.j', 'v', toUInt64(1)) AS a, dictGet('ns.d', 'v', toUInt64(1)) AS b"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = '${db}' AND name = 'v_dep'"
run "DROP TABLE ${db}.\`ns.j\` SETTINGS check_referential_table_dependencies = 1" | error_code
run "DROP DICTIONARY ${db}.\`ns.d\` SETTINGS check_referential_table_dependencies = 1" | error_code

echo '--- and it is renamed by a restore into another database'
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
run "BACKUP DATABASE ${db} TO ${backup} FORMAT Null"
run "RESTORE DATABASE ${db} AS \`${db}_copy\` FROM ${backup} FORMAT Null"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = '${db}_copy' AND name = 'v_dep'"
run "TRUNCATE TABLE \`${db}_copy\`.\`ns.j\`"
run "INSERT INTO \`${db}_copy\`.\`ns.j\` VALUES (1, 'restored')"
run "SELECT a FROM \`${db}_copy\`.v_dep"
run "DROP DATABASE \`${db}_copy\` SYNC"
run "DROP TABLE ${db}.v_dep"

echo '--- a name expression that depends on the context of the call is not frozen in a view'
run "CREATE TABLE ${db}.j_one (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "CREATE TABLE ${db}.j_two (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.j_one VALUES (1, 'one')"
run "INSERT INTO ${db}.j_two VALUES (1, 'two')"
run "USE ${db}; SET query_cache_tag = 'one'; CREATE VIEW ${db}.v_setting AS SELECT joinGet(concat('j_', getSetting('query_cache_tag')), 'v', toUInt64(1)) AS a"
run "SELECT * FROM ${db}.v_setting SETTINGS query_cache_tag = 'one'"
run "SELECT * FROM ${db}.v_setting SETTINGS query_cache_tag = 'two'"

run "DROP DATABASE \`${db}.b\` SYNC"
