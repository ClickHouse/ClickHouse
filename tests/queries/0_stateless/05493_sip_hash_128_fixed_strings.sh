#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `sipHash128` hashes the strings of a `FixedString(n)` column in groups of 8 and hashes the rest one by one.
# `sipHash128Keyed` with zero keys gives the same hashes, one string at a time. Blocks of 7, 9 and 1003 rows have 0, 1
# and 125 groups, with 7, 1 and 3 rows left over. Each line counts the rows whose hashes differ, for `FixedString`,
# `Nullable`, `LowCardinality`, `Array`, `Tuple` and `Map`.
for block in 7 9 1003; do
    for n in $(seq 1 17) 24 31 32 33 63 64 65 100 255 256 1000; do
        cat <<EOF
WITH (0::UInt64, 0::UInt64) AS key
SELECT $block, $n,
    countIf(sipHash128(f) != sipHash128Keyed(key, f)),
    countIf(NOT isNotDistinctFrom(sipHash128(nf), sipHash128Keyed(key, nf))),
    countIf(sipHash128(lc) != sipHash128Keyed(key, lc)),
    countIf(sipHash128(a) != sipHash128Keyed(key, a)),
    countIf(sipHash128(t) != sipHash128Keyed(key, t)),
    countIf(sipHash128(m) != sipHash128Keyed(key, m))
FROM
(
    SELECT
        unhex(substring(arrayStringConcat(arrayMap(i -> hex(cityHash64(number, i)), range($((n / 8 + 1))))), 1, 2 * $n)) AS bytes,
        toFixedString(multiIf(number % 11 = 0, '', number % 11 = 1, repeat('\xFF', $n), bytes), $n) AS f,
        if(number % 7 = 0, NULL, f) AS nf,
        toLowCardinality(f) AS lc,
        [f, reverse(f)] AS a,
        (f, reverse(f)) AS t,
        map(f, reverse(f)) AS m
    FROM numbers(1003)
)
SETTINGS max_block_size = $block;
EOF
    done
done | $CLICKHOUSE_CLIENT
