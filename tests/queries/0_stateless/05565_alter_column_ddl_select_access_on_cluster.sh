#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database

# ALTER ... ON CLUSTER ADD COLUMN with an expression needs SELECT on the columns the expression reads,
# and SELECT on the whole table when an identifier names no column of the initiator's table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=${CLICKHOUSE_DATABASE}
U_OC="u_oc_${DB}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${U_OC};
    CREATE TABLE t (id UInt64, restricted String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t VALUES (1, 'S1'), (2, 'S2');
    CREATE USER ${U_OC};
    GRANT SELECT, ALTER ADD COLUMN ON ${DB}.t TO ${U_OC};
    REVOKE SELECT(restricted) ON ${DB}.t FROM ${U_OC};
    GRANT CLUSTER ON *.* TO ${U_OC};
    GRANT SELECT(id), ALTER ADD COLUMN ON ${DB}.t_absent TO ${U_OC};
"

# check <label> <query> [<table>]
check()
{
    local label=$1 query=$2 table=${3:-t}
    local out rc=0
    echo "-- ${label}"
    out=$(${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "${U_OC}" --query "${query}" 2>&1) || rc=$?
    out=${out//${DB}./db.}
    if [ "${rc}" -eq 0 ] && [ -z "${out}" ]; then
        echo "OK"
    elif [[ "${out}" == *ACCESS_DENIED* ]] && [[ "${out}" == *"necessary to have the grant "*" ON db.${table}"* ]]; then
        local grant=${out#*necessary to have the grant }
        echo "ACCESS_DENIED ${grant%%" ON db.${table}"*} ON db.${table}"
    else
        echo "${out}" | grep -o -E '\([A-Z_]+\)' | head -n 1
    fi
}

check "on cluster, not granted column" "ALTER TABLE t ON CLUSTER test_shard_localhost ADD COLUMN oc_restricted String ALIAS restricted"
check "on cluster, identifier without a column" "ALTER TABLE t ON CLUSTER test_shard_localhost ADD COLUMN oc_unbound String ALIAS concat('x', absent)"
check "on cluster, granted column" "ALTER TABLE t ON CLUSTER test_shard_localhost ADD COLUMN oc_ok String DEFAULT toString(id)"
check "on cluster, table only on the hosts" "ALTER TABLE t_absent ON CLUSTER test_shard_localhost ADD COLUMN oc_absent String ALIAS toString(id)" t_absent
check "local, identifier without a column" "ALTER TABLE t ADD COLUMN l_unbound String ALIAS concat('x', absent)"

echo "-- schema"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${U_OC};
    DROP TABLE t;
"
