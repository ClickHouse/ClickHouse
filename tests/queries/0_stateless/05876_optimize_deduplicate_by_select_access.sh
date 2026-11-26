#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
u_cols="u_cols_${db}"
u_opt="u_opt_${db}"
u_full="u_full_${db}"
u_ghost="u_ghost_${db}"
u_id="u_id_${db}"
u_alias="u_alias_${db}"
u_mv="u_mv_${db}"
u_def="u_def_${db}"
users="$u_cols, $u_opt, $u_full, $u_ghost, $u_id, $u_alias, $u_mv, $u_def"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $users"
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE $db.t (sort_key UInt64, id UInt64, secret String) ENGINE = MergeTree ORDER BY sort_key;
CREATE TABLE $db.t_alias ENGINE = Alias('t');
CREATE MATERIALIZED VIEW $db.mv ENGINE = MergeTree ORDER BY sort_key AS SELECT * FROM $db.t;
CREATE USER $users IDENTIFIED WITH no_password;
GRANT SELECT(sort_key, id), OPTIMIZE ON $db.t TO $u_cols;
GRANT OPTIMIZE ON $db.t TO $u_opt;
GRANT SELECT, OPTIMIZE ON $db.t TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.t_alias TO $u_full;
GRANT SELECT(sort_key, id, nope), OPTIMIZE ON $db.t TO $u_ghost;
GRANT SELECT(id), OPTIMIZE ON $db.t TO $u_id;
GRANT SELECT, OPTIMIZE ON $db.t_alias TO $u_alias;
GRANT SELECT(sort_key, id), OPTIMIZE ON $db.t TO $u_alias;
GRANT SELECT, OPTIMIZE ON $db.mv TO $u_mv;
GRANT SELECT(sort_key, id) ON $db.t TO $u_mv;
"
# The view's insert trigger runs as its definer, so t2 is filled before the definer loses `secret`.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE $db.t2 (sort_key UInt64, id UInt64, secret String) ENGINE = MergeTree ORDER BY sort_key;
GRANT SELECT ON $db.t2 TO $u_def;
GRANT INSERT ON $db.* TO $u_def;
CREATE MATERIALIZED VIEW $db.mv_def ENGINE = MergeTree ORDER BY sort_key DEFINER = $u_def SQL SECURITY DEFINER AS SELECT * FROM $db.t2;
"
${CLICKHOUSE_CLIENT} --async_insert 0 --query "INSERT INTO $db.t2 VALUES (1, 1, 'A'), (1, 2, 'A'), (1, 3, 'B'), (2, 4, 'A'), (2, 5, 'C'), (2, 6, 'C')"
${CLICKHOUSE_CLIENT} -m --query "
REVOKE SELECT(secret) ON $db.t2 FROM $u_def;
GRANT SELECT ON $db.t2 TO $u_full;
GRANT SELECT, OPTIMIZE ON $db.mv_def TO $u_full;
"

# Refills the table with 6 rows, 4 of them distinct by (sort_key, secret), and lists its active parts.
reset()
{
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE $db.t"
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE $db.mv"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.t VALUES (1, 1, 'A'), (1, 2, 'A'), (1, 3, 'B'), (2, 4, 'A'), (2, 5, 'C'), (2, 6, 'C')"
    parts=$(${CLICKHOUSE_CLIENT} --query "SELECT arrayStringConcat(groupArray(concat(char(39), name, char(39))), ', ') FROM system.parts WHERE database = '$db' AND table = 't' AND active FORMAT TSVRaw")
}

count() { echo "rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.t")"; }

# Prints OK when the query succeeds silently, otherwise the error code and whether the error text names `$4`.
check()
{
    local label=$1 user=$2 query=$3 name=${4:-secret} out line
    out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "$query" 2>&1)
    line=$(grep -m1 -E '^Code: [0-9]+\. DB::Exception' <<< "$out")
    if [ -z "$line" ]; then
        echo "$label: ${out:-OK}" | sed "s/${db}\./db./g"
    elif grep -q -w "$name" <<< "$line"; then
        echo "$label: $(grep -o -E '\([A-Z_]+\)' <<< "$line" | tail -1) names $name"
    else
        echo "$label: $(grep -o -E '\([A-Z_]+\)' <<< "$line" | tail -1) hides $name"
    fi
}

echo '-- DEDUPLICATE BY requires SELECT on the columns it compares'
reset
check 'listed, granted      ' "$u_cols" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY sort_key, id"
check 'listed, not granted  ' "$u_cols" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY sort_key, secret"
check 'no SELECT at all     ' "$u_opt" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY sort_key, secret"
check 'SELECT on the table  ' "$u_full" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY sort_key, secret"
check 'asterisk             ' "$u_cols" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY *"
check 'asterisk except      ' "$u_cols" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY * EXCEPT secret"
check 'COLUMNS matcher      ' "$u_cols" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE BY sort_key, COLUMNS('^se')"
check 'DEDUPLICATE, no list ' "$u_cols" "OPTIMIZE TABLE $db.t DRY RUN PARTS $parts DEDUPLICATE"
check 'FINAL, not granted   ' "$u_cols" "OPTIMIZE TABLE $db.t FINAL DEDUPLICATE BY sort_key, secret"
count

echo '-- through an Alias, SELECT is also required on the target'
reset
check 'target not granted   ' "$u_alias" "OPTIMIZE TABLE $db.t_alias FINAL DEDUPLICATE BY sort_key, secret"
count
check 'target granted       ' "$u_alias" "OPTIMIZE TABLE $db.t_alias FINAL DEDUPLICATE BY sort_key, id"
count

echo '-- through a materialized view, SELECT is also required on its source table'
reset
check 'read, source denied  ' "$u_mv" "SELECT secret FROM $db.mv FORMAT Null"
check 'source not granted   ' "$u_mv" "OPTIMIZE TABLE $db.mv FINAL DEDUPLICATE BY sort_key, secret"
echo "mv rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.mv")"
check 'source granted       ' "$u_mv" "OPTIMIZE TABLE $db.mv FINAL DEDUPLICATE BY sort_key, id"

echo '-- a materialized view with SQL SECURITY DEFINER checks its source table as the definer, as its read does'
check 'read, definer lacks  ' "$u_full" "SELECT secret FROM $db.mv_def FORMAT Null"
check 'definer lacks column ' "$u_full" "OPTIMIZE TABLE $db.mv_def FINAL DEDUPLICATE BY sort_key, secret"
echo "mv_def rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.mv_def")"
check 'definer has columns  ' "$u_full" "OPTIMIZE TABLE $db.mv_def FINAL DEDUPLICATE BY sort_key, id"

echo '-- the errors name the columns only for a user who may DESCRIBE the table'
check 'unknown, default     ' default "OPTIMIZE TABLE $db.t DEDUPLICATE BY sort_key, nope"
check 'unknown, no SELECT   ' "$u_opt" "OPTIMIZE TABLE $db.t DEDUPLICATE BY sort_key, nope"
check 'DESCRIBE, ghost grant' "$u_ghost" "DESCRIBE TABLE $db.t"
check 'unknown, ghost grant ' "$u_ghost" "OPTIMIZE TABLE $db.t DEDUPLICATE BY sort_key, nope"
check 'DESCRIBE Alias       ' "$u_alias" "DESCRIBE TABLE $db.t_alias"
check 'unknown, Alias hidden' "$u_alias" "OPTIMIZE TABLE $db.t_alias DEDUPLICATE BY sort_key, nope"
check 'unknown, Alias shown ' "$u_full" "OPTIMIZE TABLE $db.t_alias DEDUPLICATE BY sort_key, nope"
check 'empty list           ' "$u_cols" "OPTIMIZE TABLE $db.t DEDUPLICATE BY COLUMNS('^nope')"
check 'empty, no SELECT     ' "$u_opt" "OPTIMIZE TABLE $db.t DEDUPLICATE BY COLUMNS('^nope')"
check 'key missing, default ' default "OPTIMIZE TABLE $db.t DEDUPLICATE BY id" sort_key
check 'key missing, SELECT  ' "$u_id" "OPTIMIZE TABLE $db.t DEDUPLICATE BY id" sort_key

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.mv_def"
${CLICKHOUSE_CLIENT} --query "DROP USER $users"
