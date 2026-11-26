#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
u_cols="u_cols_${db}"
u_full="u_full_${db}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $u_cols, $u_full"
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE $db.t (sort_key UInt64, id UInt64, secret String) ENGINE = MergeTree ORDER BY sort_key;
CREATE TABLE $db.t_alias ENGINE = Alias('t');
CREATE USER $u_cols, $u_full IDENTIFIED WITH no_password;
GRANT CLUSTER ON *.* TO $u_cols, $u_full;
GRANT SELECT(sort_key, id), OPTIMIZE ON $db.t TO $u_cols;
GRANT SELECT, OPTIMIZE ON $db.t TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.t_alias TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.t_absent TO $u_full;
"

# Refills the table with 6 rows, 4 of them distinct by (sort_key, secret).
reset()
{
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE $db.t"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.t VALUES (1, 1, 'A'), (1, 2, 'A'), (1, 3, 'B'), (2, 4, 'A'), (2, 5, 'C'), (2, 6, 'C')"
}

count() { echo "rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.t")"; }

# Prints OK when the query succeeds silently, otherwise the error code and whether the error text names `$4`.
check()
{
    local label=$1 user=$2 query=$3 name=${4:-secret} out line
    out=$(${CLICKHOUSE_CLIENT} --user "$user" --distributed_ddl_output_mode none --query "$query" 2>&1)
    line=$(grep -m1 -E '^Code: [0-9]+\. DB::Exception' <<< "$out")
    if [ -z "$line" ]; then
        echo "$label: ${out:-OK}" | sed "s/${db}\./db./g"
    elif grep -q -w "$name" <<< "$line"; then
        echo "$label: $(grep -o -E '\([A-Z_]+\)' <<< "$line" | tail -1) names $name"
    else
        echo "$label: $(grep -o -E '\([A-Z_]+\)' <<< "$line" | tail -1) hides $name"
    fi
}

echo '-- the hosts do not run the query as the user, so DEDUPLICATE BY requires SELECT on the whole table'
reset
check 'column not granted   ' "$u_cols" "OPTIMIZE TABLE $db.t ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, secret"
count
check 'columns granted      ' "$u_cols" "OPTIMIZE TABLE $db.t ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, id"
check 'DEDUPLICATE, no list ' "$u_cols" "OPTIMIZE TABLE $db.t ON CLUSTER test_shard_localhost FINAL DEDUPLICATE"

echo '-- an Alias or a table missing on the initiator cannot be checked there'
reset
check 'Alias                ' "$u_full" "OPTIMIZE TABLE $db.t_alias ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, id"
check 'Alias, no list       ' "$u_full" "OPTIMIZE TABLE $db.t_alias ON CLUSTER test_shard_localhost FINAL DEDUPLICATE"
check 'missing table        ' "$u_full" "OPTIMIZE TABLE $db.t_absent ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key" initiator

echo '-- SELECT on the table'
reset
check 'table granted        ' "$u_full" "OPTIMIZE TABLE $db.t ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, secret"
count

${CLICKHOUSE_CLIENT} --query "DROP USER $u_cols, $u_full"
