#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires the mysql client.
# Tag no-replicated-database: Replicated database does not support `ON CLUSTER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): a name relative to the current database does not take precedence
# over the alias of another source; a compound right argument of `IN` that may be a subcolumn stays an expression on
# every pass of a mutation; a source database does not make an unrelated qualified name an expression; a column default
# of `ALTER ... ON CLUSTER` is bound before it is shipped; an unqualified name of an access statement gets the current
# database once; a compound first argument of `joinGet` that may be an expression is not bound to a table; the MySQL
# `COM_INIT_DB` command checks the access for the database a hierarchical name denotes.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | sed "s/${db}/db/g"
}

function error_code()
{
    grep -o -m1 -E 'Code: [0-9]+|[A-Z_]{5,}' | head -1
}

echo '--- a name relative to the current database does not take precedence over an alias'
run "CREATE TABLE ${db}.\`ns.t\` (id UInt8) ENGINE = Memory"
run "CREATE TABLE ${db}.other (id UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\`ns.t\` VALUES (1)"
run "INSERT INTO ${db}.other VALUES (2)"
for compatibility in 0 1; do
    for prefer_left in 0 1; do
        run "USE ${db}.ns; SELECT t.id FROM ${db}.ns.t CROSS JOIN ${db}.other AS t
            SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = ${compatibility}, single_join_prefer_left_table = ${prefer_left}"
        run "USE ${db}.ns; SELECT t.id FROM ${db}.ns.t INNER JOIN ${db}.other AS t ON 1
            SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = ${compatibility}, single_join_prefer_left_table = ${prefer_left}"
    done
done

echo '--- a compound right argument of IN that may be a subcolumn stays an expression in a mutation'
run "CREATE TABLE ${db}.\`a.b\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`a.b\` VALUES (2)"
run "CREATE TABLE ${db}.t_mut (a Tuple(b Array(UInt8)), hit UInt8) ENGINE = MergeTree ORDER BY tuple()"
run "INSERT INTO ${db}.t_mut SELECT tuple([toUInt8(1)]), 7"
run "USE ${db}; ALTER TABLE t_mut UPDATE hit = 1 IN a.b WHERE 1 SETTINGS mutations_sync = 2, alter_update_mode = 'heavy'"
run "SELECT hit FROM ${db}.t_mut"
run "CREATE TABLE ${db}.t_upd (a Tuple(b Array(UInt8)), hit UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"
run "INSERT INTO ${db}.t_upd SELECT tuple([toUInt8(1)]), 7"
run "USE ${db}; UPDATE t_upd SET hit = 1 IN a.b WHERE 1"
run "SELECT hit FROM ${db}.t_upd"

echo '--- a source database does not make an unrelated qualified name an expression'
run "CREATE TABLE ${db}.src (x UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.src VALUES (1), (2)"
run "CREATE TABLE ${db}.\`ns.keys\` (x UInt8) ENGINE = Set"
run "INSERT INTO ${db}.\`ns.keys\` VALUES (1)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.copy ON CLUSTER test_shard_localhost ENGINE = Memory AS
    SELECT x FROM ${db}.src WHERE x IN ${db}.ns.keys FORMAT Null" 2>&1 | error_code
run "SELECT x FROM ${db}.copy"

echo '--- a column default of ALTER ON CLUSTER is bound before it is shipped'
run "CREATE TABLE ${db}.\`ns.j\` (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.\`ns.j\` VALUES (1, 42)"
run "CREATE TABLE ${db}.dst (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.dst VALUES (1)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${db}.dst ON CLUSTER test_shard_localhost
    ADD COLUMN v UInt64 DEFAULT joinGet(${db}.ns.j, 'v', toUInt64(1)) FORMAT Null" 2>&1 | error_code
run "SELECT x, v FROM ${db}.dst"

echo '--- an unqualified name of an access statement gets the current database once'
user="u_05498_${db}"
run "CREATE DATABASE \`${db}_x.y\`"
run "CREATE TABLE \`${db}_x.y\`.\`${db}_x.y.t\` (v UInt8) ENGINE = Memory"
run "DROP USER IF EXISTS ${user}; CREATE USER ${user}"
run "USE \`${db}_x.y\`; GRANT SELECT ON t TO ${user}"
run "SHOW GRANTS FOR ${user}"
run "USE \`${db}_x.y\`; CREATE ROW POLICY p_05498 ON t USING 1 TO ${user}"
run "SELECT database, table FROM system.row_policies WHERE short_name = 'p_05498' AND database = '${db}_x.y'"
run "DROP ROW POLICY p_05498 ON \`${db}_x.y\`.t"
run "DROP DATABASE \`${db}_x.y\`"

echo '--- a compound first argument of joinGet that may be an expression is not bound to a table'
run "CREATE TABLE ${db}.j_one (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.j_one VALUES (1, 'one')"
run "CREATE TABLE ${db}.\`p.q\` (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.\`p.q\` VALUES (1, 'other')"
run "USE ${db}; WITH CAST(tuple('j_one'), 'Tuple(q String)') AS p SELECT joinGet(p.q, 'v', toUInt64(1)) AS value"
run "USE ${db}; CREATE VIEW v_join AS WITH CAST(tuple('j_one'), 'Tuple(q String)') AS p SELECT joinGet(p.q, 'v', toUInt64(1)) AS value"
run "SELECT value FROM ${db}.v_join"
echo '--- the same name without such an alias is the table'
run "USE ${db}; CREATE VIEW v_join_table AS SELECT joinGet(p.q, 'v', toUInt64(1)) AS value"
run "SELECT value FROM ${db}.v_join_table"

echo '--- COM_INIT_DB checks the access for the database a hierarchical name denotes'
run "GRANT SHOW DATABASES ON ${db}.* TO ${user}"
${MYSQL_CLIENT_BINARY} --protocol tcp --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_MYSQL}" --user "${user}" --no-auto-rehash \
    --batch --skip-column-names --execute "use \`${db}.ns\`; SELECT currentDatabase();" 2>&1 | grep -v "^WARNING" | sed "s/${db}/db/g"
run "REVOKE SHOW DATABASES ON ${db}.* FROM ${user}"
run "GRANT SHOW DATABASES ON \`${db}.ns\`.* TO ${user}"
${MYSQL_CLIENT_BINARY} --protocol tcp --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_MYSQL}" --user "${user}" --no-auto-rehash \
    --batch --skip-column-names --execute "use \`${db}.ns\`; SELECT currentDatabase();" 2>&1 | grep -o -m1 'ACCESS_DENIED'
run "DROP USER ${user}"
