#!/usr/bin/env bash

# ALTER ADD/MODIFY COLUMN with an expression needs SELECT on the columns the expression reads,
# ALTER RENAME COLUMN needs SELECT on the renamed column, and an Alias table refuses both forms.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=${CLICKHOUSE_DATABASE}
U_PARTIAL="u_partial_${DB}"
U_COLS="u_cols_${DB}"
U_SUB="u_sub_${DB}"
U_FULL="u_full_${DB}"
U_DOT="u_dot_${DB}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${U_PARTIAL}, ${U_COLS}, ${U_SUB}, ${U_FULL}, ${U_DOT};

    CREATE TABLE t
    (
        id UInt64, restricted String, other String, pub String ALIAS 'public',
        tup Tuple(a String, b String), arr Array(Tuple(restricted String)),
        \`n.secret\` Array(String), \`n.x\` Array(UInt8),
        pub_tup Tuple(s String) ALIAS tuple(restricted),
        eph String EPHEMERAL concat('E-', restricted),
        \`q.k\` String, k String
    ) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t (id, restricted, other, tup, arr, \`n.secret\`, \`n.x\`, \`q.k\`, k) VALUES
        (1, 'S1', 'o1', ('a1', 'b1'), [tuple('e1')], ['N1'], [1], 'q1', 'K1'),
        (2, 'S2', 'o2', ('a2', 'b2'), [tuple('e2')], ['N2'], [2], 'q2', 'K2');

    SET allow_experimental_alias_table_engine = 1;
    CREATE TABLE t_alias ENGINE = Alias('t');

    CREATE TABLE dict_src (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dict_src VALUES (1, 'd1'), (2, 'd2');
    CREATE DICTIONARY d (id UInt64, v String) PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dict_src' DB '${DB}')) LIFETIME(0) LAYOUT(FLAT());

    CREATE USER ${U_PARTIAL};
    GRANT SELECT, ALTER ADD COLUMN, ALTER MODIFY COLUMN, ALTER RENAME COLUMN ON ${DB}.t TO ${U_PARTIAL};
    REVOKE SELECT(restricted), SELECT(\`n.secret\`), SELECT(pub_tup) ON ${DB}.t FROM ${U_PARTIAL};
    CREATE USER ${U_COLS};
    GRANT SELECT(id, pub, arr), ALTER ADD COLUMN ON ${DB}.t TO ${U_COLS};
    GRANT dictGet ON ${DB}.d TO ${U_COLS};
    CREATE USER ${U_SUB};
    GRANT SELECT(id, \`tup.a\`), ALTER ADD COLUMN ON ${DB}.t TO ${U_SUB};
    CREATE USER ${U_FULL};
    GRANT SELECT, ALTER ADD COLUMN ON ${DB}.t TO ${U_FULL};
    CREATE USER ${U_DOT};
    GRANT SELECT(id, \`q.k\`), ALTER ADD COLUMN ON ${DB}.t TO ${U_DOT};
"

# check <user|default> <label> <query> [<column>]
check()
{
    local user=$1 label=$2 query=$3 column=${4:-}
    local out rc=0
    echo "-- ${label}"
    if [ "${user}" = default ]; then
        out=$(${CLICKHOUSE_CLIENT} --query "${query}" 2>&1) || rc=$?
    else
        out=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "${query}" 2>&1) || rc=$?
    fi
    if [ "${rc}" -eq 0 ] && [ -z "${out}" ]; then
        echo "OK"
    elif [ -n "${column}" ] && [[ "${out}" == *ACCESS_DENIED* ]] \
        && { [[ "${out}" == *"SELECT(${column})"* ]] || [[ "${out}" == *"SELECT(\`${column}\`)"* ]]; }; then
        echo "ACCESS_DENIED SELECT(${column})"
    elif [[ "${out}" == *NOT_IMPLEMENTED* ]]; then
        echo "NOT_IMPLEMENTED"
    else
        echo "UNEXPECTED: ${out%%$'\n'*}"
    fi
}

check "${U_PARTIAL}" "alias" "ALTER TABLE t ADD COLUMN a_alias String ALIAS concat('x', restricted)" restricted
check "${U_PARTIAL}" "default" "ALTER TABLE t ADD COLUMN a_default String DEFAULT restricted" restricted
check "${U_PARTIAL}" "materialized" "ALTER TABLE t ADD COLUMN a_materialized String MATERIALIZED restricted" restricted
check "${U_PARTIAL}" "ephemeral" "ALTER TABLE t ADD COLUMN a_ephemeral String EPHEMERAL restricted" restricted
check "${U_PARTIAL}" "inside a lambda" "ALTER TABLE t ADD COLUMN a_lambda Array(String) ALIAS arrayMap(x -> concat(x, restricted), ['a'])" restricted
check "${U_PARTIAL}" "qualified" "ALTER TABLE t ADD COLUMN a_qualified String ALIAS dummy.restricted" restricted
check "${U_PARTIAL}" "shadowed by an expression alias" "ALTER TABLE t ADD COLUMN a_shadow String ALIAS concat((toString(id) AS restricted), restricted)" restricted
check "${U_PARTIAL}" "modify alias" "ALTER TABLE t MODIFY COLUMN pub String ALIAS restricted" restricted
check "${U_PARTIAL}" "modify default" "ALTER TABLE t MODIFY COLUMN other String DEFAULT upper(restricted)" restricted
check "${U_PARTIAL}" "multiple commands" "ALTER TABLE t ADD COLUMN m_ok UInt8 DEFAULT 1, ADD COLUMN m_bad String ALIAS restricted" restricted
check "${U_PARTIAL}" "nested prefix" "ALTER TABLE t ADD COLUMN a_nested String ALIAS toString(n)" n.secret
check "${U_PARTIAL}" "subcolumn of an alias column" "ALTER TABLE t ADD COLUMN a_alias_sub String ALIAS pub_tup.s" pub_tup
check "${U_PARTIAL}" "ephemeral column" "ALTER TABLE t ADD COLUMN a_eph String DEFAULT eph" restricted
check default "alias table, expression" "ALTER TABLE t_alias ADD COLUMN via_alias String DEFAULT upper(other)"
check default "alias table, rename" "ALTER TABLE t_alias RENAME COLUMN other TO other2"
check default "alias table, modify" "ALTER TABLE t_alias MODIFY COLUMN other String DEFAULT upper(restricted)"
check default "alias table, constant" "ALTER TABLE t_alias ADD COLUMN via_alias_const String DEFAULT 'k'"
check default "alias table, lambda only" "ALTER TABLE t_alias ADD COLUMN via_alias_lambda Array(UInt8) DEFAULT arrayMap(x -> x + 1, [1])"
check "${U_PARTIAL}" "rename" "ALTER TABLE t RENAME COLUMN restricted TO s2" restricted
check "${U_PARTIAL}" "granted column" "ALTER TABLE t ADD COLUMN ok_default String DEFAULT toString(id)"
check "${U_PARTIAL}" "modify, granted column" "ALTER TABLE t MODIFY COLUMN other String DEFAULT concat('o', toString(id))"
check "${U_PARTIAL}" "rename, granted column" "ALTER TABLE t RENAME COLUMN ok_default TO ok_default2"
check "${U_COLS}" "lambda parameter" "ALTER TABLE t ADD COLUMN c_lambda Array(UInt64) ALIAS arrayMap(x -> x + id, [1, 2])"
check "${U_COLS}" "field of a lambda parameter" "ALTER TABLE t ADD COLUMN c_field Array(String) ALIAS arrayMap(x -> x.restricted, arr)"
check "${U_COLS}" "granted alias column" "ALTER TABLE t ADD COLUMN c_pub String ALIAS concat(pub, '!')"
check "${U_COLS}" "dictionary identifier" "ALTER TABLE t ADD COLUMN c_dict String ALIAS dictGet(d, 'v', id)"
check "${U_COLS}" "not granted column" "ALTER TABLE t ADD COLUMN c_restricted String ALIAS restricted" restricted
check "${U_COLS}" "not granted virtual column" "ALTER TABLE t ADD COLUMN c_virtual String ALIAS _table" _table
check "${U_PARTIAL}" "granted virtual column" "ALTER TABLE t ADD COLUMN p_virtual String ALIAS _table"
check "${U_SUB}" "granted subcolumn" "ALTER TABLE t ADD COLUMN s_a String ALIAS tup.a"
check "${U_SUB}" "not granted subcolumn" "ALTER TABLE t ADD COLUMN s_b String ALIAS tup.b" tup.b
check "${U_DOT}" "quoted dotted column" "ALTER TABLE t ADD COLUMN d_quoted String ALIAS \`q.k\`"
check "${U_FULL}" "full access" "ALTER TABLE t ADD COLUMN f String ALIAS concat('F-', restricted)"

echo "-- full access, read"
${CLICKHOUSE_CLIENT} --user "${U_FULL}" --query "SELECT f FROM t ORDER BY id"
echo "-- added columns"
${CLICKHOUSE_CLIENT} --query "SELECT ok_default2, c_lambda, c_field, s_a, d_quoted FROM t ORDER BY id"
echo "-- schema"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${U_PARTIAL}, ${U_COLS}, ${U_SUB}, ${U_FULL}, ${U_DOT};
    DROP TABLE t_alias;
    DROP TABLE t;
    DROP DICTIONARY d;
    DROP TABLE dict_src;
"
