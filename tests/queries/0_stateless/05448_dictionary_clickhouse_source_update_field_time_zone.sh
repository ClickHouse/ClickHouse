#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lower bound of an incremental update (previous update time - update_lag) must be the same instant
# whatever the time zones of the server and of the source column. update_lag puts it at B: in the second
# occurrence of the hour repeated in Europe/Amsterdam at the end of DST (2026-10-25 01:30:00 UTC), and at
# an 8-digit timestamp (1970-08-20), which the DateTime parser reads as YYYYMMDD.
# The String, Dynamic and SimpleAggregateFunction(anyLast, Dynamic) update_fields hold UTC text and are compared as text,
# so their rows are two days from B.
# Every case has its own source table and dictionaries, all created up front, so that one round of periodic
# updates covers all of them.
cases=("1792891800 t" "20000101 t" "1792891800 s" "1792891800 dyn" "1792891800 sdyn")
NOW=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp(now())")

dicts=()
setup=""
inserts=""
selects=""
cleanup=""
for i in "${!cases[@]}"; do
    read -r B field <<< "${cases[$i]}"
    setup+="
        CREATE TABLE src_$i (key UInt64, value String, t DateTime('Europe/Amsterdam'), s String, dyn Dynamic, sdyn SimpleAggregateFunction(anyLast, Dynamic)) ENGINE = Memory;
        INSERT INTO src_$i VALUES (1, 'initial', now(), toString(now()), toString(now()), toString(now()));"
    # One INSERT per table, so that one update sees all three rows. Key 3 is above any bound.
    inserts+="
        INSERT INTO src_$i SELECT key, value, t, toString(toDateTime(s, 'UTC')), toString(toDateTime(s, 'UTC')), toString(toDateTime(s, 'UTC'))
        FROM values('key UInt64, value String, t UInt32, s UInt32',
            (2, 'below the bound', $((B - 1200)), $((B - 172800))),
            (3, 'far future', 4102444800, 4102444800),
            (4, 'above the bound', $((B + 1200)), $((B + 172800))));"
    sources=("TABLE 'src_$i'" "QUERY \$doc\$SELECT key, value FROM $CLICKHOUSE_DATABASE.src_$i WHERE {condition}\$doc\$")
    for j in 0 1; do
        dicts+=("d_${i}_$j")
        setup+="
            CREATE DICTIONARY d_${i}_$j (key UInt64, value String)
            PRIMARY KEY key
            SOURCE(CLICKHOUSE(${sources[$j]} UPDATE_FIELD '$field' UPDATE_LAG $((NOW - B))))
            LAYOUT(HASHED())
            LIFETIME(1);"
        selects+="SELECT key, value FROM d_${i}_$j ORDER BY key;"
        cleanup+="DROP DICTIONARY d_${i}_$j;"
    done
    cleanup+="DROP TABLE src_$i;"
done

# Succeeds when every dictionary has the key. The first call does the initial loads.
function all_have_key()
{
    local query="SELECT 0"
    for d in "${dicts[@]}"; do
        query+=" + dictHas('$d', toUInt64($1))"
    done
    [[ $($CLICKHOUSE_CLIENT -q "$query") == "${#dicts[@]}" ]]
}

$CLICKHOUSE_CLIENT -q "$setup"

while clickhouse_test_time_left; do
    all_have_key 1 && break
    sleep 0.3
done

$CLICKHOUSE_CLIENT -q "$inserts"

while clickhouse_test_time_left; do
    all_have_key 3 && break
    sleep 0.3
done

$CLICKHOUSE_CLIENT -q "$selects"
$CLICKHOUSE_CLIENT -q "$cleanup"
