#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
u_cols="u_cols_${db}"
u_full="u_full_${db}"
u_def="u_def_${db}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $u_cols, $u_full, $u_def"
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE $db.t (sort_key UInt64, id UInt64, secret String) ENGINE = MergeTree ORDER BY sort_key;
CREATE TABLE $db.t_alias ENGINE = Alias('t');
CREATE MATERIALIZED VIEW $db.mv ENGINE = MergeTree ORDER BY sort_key AS SELECT * FROM $db.t;
CREATE MATERIALIZED VIEW $db.rmv REFRESH EVERY 1 YEAR ENGINE = MergeTree ORDER BY sort_key EMPTY AS SELECT * FROM $db.t;
CREATE USER $u_cols, $u_full, $u_def IDENTIFIED WITH no_password;
GRANT CLUSTER ON *.* TO $u_cols, $u_full;
GRANT SELECT(sort_key, id), OPTIMIZE ON $db.t TO $u_cols;
GRANT SELECT, OPTIMIZE ON $db.t TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.t_alias TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.t_absent TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.mv TO $u_cols, $u_full;
GRANT SELECT, OPTIMIZE ON $db.rmv TO $u_cols;
CREATE TABLE $db.t2 (sort_key UInt64, id UInt64, secret String) ENGINE = MergeTree ORDER BY sort_key;
GRANT SELECT ON $db.t2 TO $u_def;
GRANT INSERT ON $db.* TO $u_def;
CREATE MATERIALIZED VIEW $db.mv_def ENGINE = MergeTree ORDER BY sort_key DEFINER = $u_def SQL SECURITY DEFINER AS SELECT * FROM $db.t2;
GRANT SELECT, OPTIMIZE ON $db.mv_def TO $u_cols;
"
${CLICKHOUSE_CLIENT} --async_insert 0 --query "INSERT INTO $db.t2 VALUES (1, 1, 'A'), (1, 2, 'A'), (1, 3, 'B'), (2, 4, 'A'), (2, 5, 'C'), (2, 6, 'C')"

# Refills the table with 6 rows, 4 of them distinct by (sort_key, secret).
reset()
{
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE $db.t"
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE $db.mv"
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

echo '-- a materialized view also needs SELECT on its whole source table'
reset
check 'view, source columns ' "$u_cols" "OPTIMIZE TABLE $db.mv ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, id"
check 'view, source granted ' "$u_full" "OPTIMIZE TABLE $db.mv ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, secret"
echo "mv rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.mv")"

echo '-- a view with SQL security is checked on the hosts, as its read is'
reset
${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW $db.rmv"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT VIEW $db.rmv"
check 'refreshable view     ' "$u_cols" "OPTIMIZE TABLE $db.rmv ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, secret"
echo "rmv rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.rmv")"
check 'definer has source   ' "$u_cols" "OPTIMIZE TABLE $db.mv_def ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, secret"
echo "mv_def rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.mv_def")"

echo '-- SELECT on the table'
reset
check 'table granted        ' "$u_full" "OPTIMIZE TABLE $db.t ON CLUSTER test_shard_localhost FINAL DEDUPLICATE BY sort_key, secret"
count

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.mv_def"
${CLICKHOUSE_CLIENT} --query "DROP USER $u_cols, $u_full, $u_def"
