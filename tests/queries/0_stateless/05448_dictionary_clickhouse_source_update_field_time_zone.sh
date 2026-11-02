#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lower bound of an incremental update (previous update time - update_lag) must be the same instant
# whatever the time zones of the server and of the source column. update_lag puts it at B: in the second
# occurrence of the hour repeated in Europe/Amsterdam at the end of DST (2026-10-25 01:30:00 UTC), and at
# an 8-digit timestamp (1970-08-20), which the DateTime parser reads as YYYYMMDD.
# The String and Dynamic update_fields hold UTC text and are compared as text, so their rows are two days from B.
$CLICKHOUSE_CLIENT -q "CREATE TABLE src (key UInt64, value String, t DateTime('Europe/Amsterdam'), s String, dyn Dynamic) ENGINE = Memory"

for case in "1792891800 t" "20000101 t" "1792891800 s" "1792891800 dyn"; do
    read -r B field <<< "$case"
    LAG=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp(now()) - $B")

    for source in "TABLE 'src'" "QUERY \$doc\$SELECT key, value FROM $CLICKHOUSE_DATABASE.src WHERE {condition}\$doc\$"; do
        $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE src"
        $CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (1, 'initial', now(), toString(now()), toString(now()))"
        $CLICKHOUSE_CLIENT -q "
            CREATE DICTIONARY d (key UInt64, value String)
            PRIMARY KEY key
            SOURCE(CLICKHOUSE($source UPDATE_FIELD '$field' UPDATE_LAG $LAG))
            LAYOUT(HASHED())
            LIFETIME(1)"

        while clickhouse_test_time_left; do
            [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM d WHERE key = 1") == 1 ]] && break
            sleep 0.3
        done

        # One INSERT, so that one update sees all three rows. Key 3 is above any bound.
        $CLICKHOUSE_CLIENT -q "
            INSERT INTO src SELECT key, value, t, toString(toDateTime(s, 'UTC')), toString(toDateTime(s, 'UTC'))
            FROM values('key UInt64, value String, t UInt32, s UInt32',
                (2, 'below the bound', $((B - 1200)), $((B - 172800))),
                (3, 'far future', 4102444800, 4102444800),
                (4, 'above the bound', $((B + 1200)), $((B + 172800))))"

        while clickhouse_test_time_left; do
            [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM d WHERE key = 3") == 1 ]] && break
            sleep 0.3
        done

        $CLICKHOUSE_CLIENT -q "SELECT key, value FROM d ORDER BY key"
        $CLICKHOUSE_CLIENT -q "DROP DICTIONARY d"
    done
done

$CLICKHOUSE_CLIENT -q "DROP TABLE src"
