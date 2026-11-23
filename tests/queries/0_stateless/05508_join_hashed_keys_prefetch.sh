#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Joins on keys that use the `hashed` and `two_level_hashed` maps, on a table with 150000 rows, so that they prefetch. A
# prefetching join must neither build nor prefetch the key of a row that it skips.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE big ENGINE = MergeTree ORDER BY id AS
    SELECT number + 1 AS id, toString(number % 70000) AS s, number % 3 AS u, if(number % 5 = 0, NULL, s) AS ns, (ns, number) AS tn,
        if(number % 9 = 0, NULL, number * 2) AS nts
    FROM numbers(150000);
"

function settings_of()
{
    case $1 in
        hash) echo "join_algorithm = 'hash'" ;;
        parallel1) echo "join_algorithm = 'parallel_hash', max_threads = 1, max_bytes_ratio_before_external_join = 0" ;;
        # The table is below `parallel_hash_join_threshold`, so `hash_join_layout` asks for the parallel layout.
        parallel8) echo "join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 8, max_bytes_ratio_before_external_join = 0" ;;
    esac
}

# A `Nullable` ASOF column: the build reads only the rows without a NULL in it.
for kind in "ASOF" "ASOF LEFT"
do
    for algorithm in hash parallel1 parallel8
    do
        echo "SELECT '$kind big $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b $kind JOIN big ON b.s = big.s AND b.u = big.u AND b.id * 2 >= big.nts SETTINGS $(settings_of "$algorithm");"
    done
done | $CLICKHOUSE_CLIENT

for algorithm in hash parallel1 parallel8
do
    echo "SELECT 'big s_u64 $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b JOIN big ON b.s = big.s AND b.u = big.u SETTINGS $(settings_of "$algorithm");"
    echo "SELECT 'big tn $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b LEFT JOIN big ON b.tn = big.tn SETTINGS $(settings_of "$algorithm");"
    echo "SELECT 'big or $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b JOIN big ON b.s = big.s AND b.u = big.u OR b.tn = big.tn SETTINGS $(settings_of "$algorithm");"
    # No row matches the first map, so an ANY join starts to prefetch the second one as well.
    echo "SELECT 'big or any $algorithm', count(), sum(cityHash64(b.id, big.id != 0)) FROM big AS b LEFT ANY JOIN big ON b.s = big.s AND b.u = big.u + 3 OR b.tn = big.tn SETTINGS $(settings_of "$algorithm");"
    # As above, but the second map has `NULL` keys.
    echo "SELECT 'big or ns any $algorithm', count(), sum(cityHash64(b.id, big.id != 0)) FROM big AS b LEFT ANY JOIN big ON b.s = big.s AND b.u = big.u + 3 OR b.ns = big.ns AND b.u = big.u SETTINGS $(settings_of "$algorithm");"
    echo "SELECT 'big ns mixed $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b LEFT JOIN big ON b.ns = big.ns AND b.u = big.u AND (b.id + big.id) % 3 != 0 SETTINGS $(settings_of "$algorithm");"
done | $CLICKHOUSE_CLIENT

# The joins on `big` prefetch, so they must neither build nor prefetch the key of a row that they skip. `ns` is `NULL`
# in one row in 5. Each condition skips either 19 rows in 20, or runs of 2000 rows plus one row in 10 between the runs.
for kind in "INNER ALL" "LEFT ALL" "RIGHT ALL" "FULL ALL"
do
    for algorithm in hash parallel8
    do
        settings=$(settings_of "$algorithm")
        echo "SELECT 'big ns $kind $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b $kind JOIN big ON b.ns = big.ns AND b.u = big.u SETTINGS $settings;"
        echo "SELECT 'big left condition $kind $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b $kind JOIN big ON b.s = big.s AND b.u = big.u AND b.id % 20 = 0 SETTINGS $settings;"
        echo "SELECT 'big left runs $kind $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b $kind JOIN big ON b.s = big.s AND b.u = big.u AND intDiv(b.id, 1000) % 3 = 0 AND b.id % 10 != 0 SETTINGS $settings;"
        echo "SELECT 'big right condition $kind $algorithm', count(), sum(cityHash64(b.id, big.id)) FROM big AS b $kind JOIN big ON b.s = big.s AND b.u = big.u AND big.id % 20 = 0 SETTINGS $settings;"
    done
done | $CLICKHOUSE_CLIENT

