#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Runs every `hash` and `grace_hash` join with `enable_packed_string_keys_in_hash_join` off and on, and checks that the
# results are identical. With the setting on, the `hash` and `grace_hash` joins on a single `String` key must use the
# packed keys, and the other joins must not. 05457 does the same for the `parallel_hash` joins.

# A key of up to 15 bytes is stored inline and a longer one as a hash, so the keys are around that limit: lengths 0, 1,
# 2, 8, 14, 15, 16 and 17, keys that share their first 15 bytes, '\0' at byte 14, 'a' and 'a\0', and 0xFF bytes. A left
# table has the right keys, the right keys with the last byte changed or with '\0' appended, the first 15 bytes of the
# longer right keys, and missing keys. Each key is in `b_s` twice, and `b_s` also has a few keys of 100 KB.
#
# The right tables are inserted in parts, so that a join reads several right blocks. The hash table of `b_big` is larger
# than the L2 cache, so the joins on it use software prefetch. `b_big` also has enough keys for the spilling join to
# switch to `grace_hash` in the middle of the build.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE keys (j UInt64, k String) ENGINE = Memory;
    INSERT INTO keys
    SELECT j, multiIf(
            f = 0, multiIf(v = 0, '', v <= 26, char(64 + v), concat(char(65 + v % 26), toString(v))),
            f = 1, if(v < 260, char(65 + v % 26, 48 + intDiv(v, 26)), concat(char(65 + v % 26), toString(v), 'x')),
            f = 2, substring(h, 1, 8),
            f = 3, substring(h, 1, 14),
            f = 4, substring(h, 1, 15),
            f = 5, substring(h, 1, 16),
            f = 6, substring(h, 1, 17),
            f = 7, substring(shared, 1, 15),
            f = 8, concat(substring(shared, 1, 15), toString(v)),
            f = 9, concat(substring(h, 1, 14), '\0'),
            f = 10, if(v < 5, ['a', 'a\0', 'a\0\0', '\0', '\0\0'][v + 1], concat(['a', 'a\0', '\0'][v % 3 + 1], toString(v))),
            if(v < 3, repeat('\xFF', 14 + v), concat(repeat('\xFF', 15), toString(v))))
    FROM
    (
        SELECT number AS j, j % 12 AS f, intDiv(j, 12) AS v, hex(sipHash128(toString(j))) AS h,
            hex(sipHash128(toString(intDiv(v, 2) + 1000000))) AS shared
        FROM numbers(60000)
    );

    CREATE TABLE b_s (k String, ts UInt64, v UInt64) ENGINE = Memory;
    CREATE TABLE b_big (k String, ts UInt64, v UInt64) ENGINE = Memory;
"

for part in $(seq 0 11)
do
    echo "INSERT INTO b_s SELECT k, i * 10, i FROM keys ARRAY JOIN [j, j + 3000] AS i WHERE j < 3000 AND intDiv(j, 250) = $part;"
    echo "INSERT INTO b_big SELECT k, j * 10, j FROM keys WHERE intDiv(j, 5000) = $part;"
done | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT -q "
    INSERT INTO b_s SELECT repeat(hex(sipHash128(toString(number % 3))), 3200), 60000 + number * 10, 6000 + number FROM numbers(6);

    CREATE TABLE p_s (k String, ts UInt64, w UInt64) ENGINE = Memory;
    CREATE TABLE p_big (k String, ts UInt64, w UInt64) ENGINE = Memory;
    INSERT INTO p_s
    SELECT multiIf(
            c = 0, k,
            c = 1, if(k = '', 'b', concat(substring(k, 1, length(k) - 1), if(right(k, 1) = 'z', 'y', 'z'))),
            c = 2, concat(k, '\0'),
            length(k) > 15, substring(k, 1, 15),
            concat(k, toString(n + 100000))),
        j * 20 + n % 7 * 3, n
    FROM (SELECT number AS n, cityHash64(number) % 3000 AS j, cityHash64(number, 1) % 4 AS c FROM numbers(9000)) AS q
    JOIN keys USING (j)
    SETTINGS join_algorithm = 'hash';
    INSERT INTO p_s
    SELECT if(number % 2 = 0, key, concat(substring(key, 1, length(key) - 1), 'x')), 60000 + number * 7, 9000 + number
    FROM (SELECT number, repeat(hex(sipHash128(toString(number % 3))), 3200) AS key FROM numbers(12));
    INSERT INTO p_big
    SELECT multiIf(
            c = 0, k,
            c = 1, if(k = '', 'b', concat(substring(k, 1, length(k) - 1), if(right(k, 1) = 'z', 'y', 'z'))),
            c = 2, concat(k, '\0'),
            length(k) > 15, substring(k, 1, 15),
            concat(k, toString(n + 100000))),
        j * 20 + n % 7 * 3, n
    FROM (SELECT number AS n, cityHash64(number) % 60000 AS j, cityHash64(number, 1) % 4 AS c FROM numbers(60000)) AS q
    JOIN keys USING (j)
    SETTINGS join_algorithm = 'hash';

    CREATE TABLE b_n (k Nullable(String), ts UInt64, v UInt64) ENGINE = Memory;
    INSERT INTO b_n SELECT if(v % 11 = 0, NULL, k), ts, v FROM b_s;
    CREATE TABLE p_n (k Nullable(String), ts UInt64, w UInt64) ENGINE = Memory;
    INSERT INTO p_n SELECT if(w % 13 = 0, NULL, k), ts, w FROM p_s;
    CREATE TABLE b_lc (k LowCardinality(String), ts UInt64, v UInt64) ENGINE = Memory;
    INSERT INTO b_lc SELECT * FROM b_s;
    CREATE TABLE p_lc (k LowCardinality(String), ts UInt64, w UInt64) ENGINE = Memory;
    INSERT INTO p_lc SELECT * FROM p_s;
    CREATE TABLE b_lcn (k LowCardinality(Nullable(String)), ts UInt64, v UInt64) ENGINE = Memory;
    INSERT INTO b_lcn SELECT * FROM b_n;
    CREATE TABLE p_lcn (k LowCardinality(Nullable(String)), ts UInt64, w UInt64) ENGINE = Memory;
    INSERT INTO p_lcn SELECT * FROM p_n;
    CREATE TABLE p_c (k String, ts UInt64, w UInt64) ENGINE = Memory;
    INSERT INTO p_c SELECT * FROM p_s;
    INSERT INTO p_c VALUES ('a constant key of 31 bytes ....', 100000, 100000);

    CREATE TABLE j_any (k String, v UInt64) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO j_any SELECT k, v FROM b_s SETTINGS enable_packed_string_keys_in_hash_join = 1, log_comment = '05460:on:j:engine:insert_any';
    CREATE TABLE j_all (k String, v UInt64) ENGINE = Join(ALL, INNER, k);
    INSERT INTO j_all SELECT k, v FROM b_s SETTINGS enable_packed_string_keys_in_hash_join = 1, log_comment = '05460:on:j:engine:insert_all';
"

# Prints a query of the test. Its result is one line: the config, the number of rows and a checksum of the rows.
function query()
{
    local mode=$1 config=$2 select=$3 settings=$4
    echo "SELECT '$config', count(), sum(cityHash64(*)) FROM ($select) SETTINGS $settings, max_block_size = 997, max_joined_block_size_rows = 701, log_comment = '05460:$mode:$config';"
}

function settings_of()
{
    case $1 in
        hash) echo "join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0" ;;
        # With the packed keys, a spilling join that keeps every right row starts below the threshold and crosses it
        # after a few right blocks. The other settings keep the size of the build as it is measured here.
        hspill) echo "join_algorithm = 'hash', max_bytes_before_external_join = 8000000, max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 2, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0, collect_hash_table_stats_during_joins = 0, enable_hash_join_row_store = 0, query_plan_join_swap_table = 'false'" ;;
        grace) echo "join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4" ;;
    esac
}

function queries()
{
    local mode=$1
    for t in s n lc lcn c big
    do
        local from="FROM p_$t AS p" right="b_$t" algorithms="hash grace"
        if [ "$t" = c ]
        then
            right="(SELECT 'a constant key of 31 bytes ....' AS k, number * 10 AS ts, number AS v FROM numbers(1500))"
        fi
        if [ "$t" = big ]
        then
            algorithms="hash hspill"
        fi
        for algorithm in $algorithms
        do
            local settings c="$t:$algorithm"
            settings=$(settings_of "$algorithm")

            # A key has several right rows, so ANY joins select only what does not depend on which of them is taken.
            for kind in "INNER ALL" "LEFT ALL" "RIGHT ALL" "FULL ALL"
            do
                query "$mode" "$c:${kind// /_}" "SELECT cityHash64(p.k), p.w, cityHash64(b.k), b.v $from $kind JOIN $right AS b ON p.k = b.k" "$settings"
            done
            if [ "$t" != c ]
            then
                query "$mode" "$c:INNER_ANY" "SELECT cityHash64(p.k), cityHash64(b.k) $from INNER ANY JOIN $right AS b ON p.k = b.k" "$settings"
            fi
            query "$mode" "$c:LEFT_ANY" "SELECT cityHash64(p.k), p.w, cityHash64(b.k) $from LEFT ANY JOIN $right AS b ON p.k = b.k" "$settings"
            query "$mode" "$c:RIGHT_ANY" "SELECT cityHash64(b.k), b.v $from RIGHT ANY JOIN $right AS b ON p.k = b.k" "$settings"
            query "$mode" "$c:LEFT_ANTI" "SELECT cityHash64(p.k), p.w $from LEFT ANTI JOIN $right AS b ON p.k = b.k" "$settings"
            query "$mode" "$c:RIGHT_SEMI" "SELECT cityHash64(b.k), b.v $from RIGHT SEMI JOIN $right AS b ON p.k = b.k" "$settings"
            if [ "$t" != c ]
            then
                query "$mode" "$c:LEFT_SEMI" "SELECT cityHash64(p.k), p.w $from LEFT SEMI JOIN $right AS b ON p.k = b.k" "$settings"
                query "$mode" "$c:RIGHT_ANTI" "SELECT cityHash64(b.k), b.v $from RIGHT ANTI JOIN $right AS b ON p.k = b.k" "$settings"
            fi

            if [ "$algorithm" = hash ]
            then
                query "$mode" "$c:LEFT_ASOF" "SELECT cityHash64(p.k), p.w, b.v $from LEFT ASOF JOIN $right AS b ON p.k = b.k AND p.ts >= b.ts" "$settings"
                if [ "$t" != c ]
                then
                    query "$mode" "$c:INNER_ASOF" "SELECT cityHash64(p.k), p.w, b.v $from INNER ASOF JOIN $right AS b ON p.k = b.k AND p.ts >= b.ts" "$settings"
                fi
                for kind in "INNER ALL" "LEFT ALL" "RIGHT ALL" "FULL ALL"
                do
                    query "$mode" "$c:filter_${kind// /_}" "SELECT cityHash64(p.k), p.w, cityHash64(b.k), b.v $from $kind JOIN $right AS b ON p.k = b.k AND cityHash64(p.w, b.v) % 3 != 0" "$settings"
                done
                if [ "$t" != c ]
                then
                    query "$mode" "$c:filter_LEFT_SEMI" "SELECT cityHash64(p.k), p.w $from LEFT SEMI JOIN $right AS b ON p.k = b.k AND cityHash64(p.w, b.v) % 3 != 0" "$settings"
                fi
                query "$mode" "$c:filter_LEFT_ANTI" "SELECT cityHash64(p.k), p.w $from LEFT ANTI JOIN $right AS b ON p.k = b.k AND cityHash64(p.w, b.v) % 3 != 0" "$settings"
                # With `join_use_nulls` a `LowCardinality` right key becomes `LowCardinality(Nullable(String))`, which
                # these joins do not store as packed keys.
                if [ "$t" != lc ]
                then
                    for kind in "LEFT ALL" "RIGHT ALL" "FULL ALL"
                    do
                        query "$mode" "$c:use_nulls_${kind// /_}" "SELECT cityHash64(p.k), p.w, cityHash64(b.k), b.v $from $kind JOIN $right AS b ON p.k = b.k" "$settings, join_use_nulls = 1"
                    done
                fi
            fi

            # Only `hash` runs joins with `OR`. The map of the second condition holds the first 15 bytes of each right
            # key, all inline. The map of the first condition holds the longer right keys as hashes. Whichever right row
            # a LEFT ANY join takes, its first 15 bytes are those of the left key. The first map of `b_big` is larger
            # than the L2 cache, so there the key getter of the first condition builds its keys ahead of the probe, for
            # the prefetch. The key getter of the second condition builds each key where it is read.
            if [ "$algorithm" = hash ] && { [ "$t" = s ] || [ "$t" = n ] || [ "$t" = lc ] || [ "$t" = big ]; }
            then
                local on_or="ON p.k = b.k OR p.k = substring(b.k, 1, 15)" settings_or="$settings, query_plan_join_swap_table = 'false'"
                local or_kinds=("INNER ALL" "LEFT ALL" "RIGHT ALL" "FULL ALL" "LEFT ANY" "LEFT SEMI" "LEFT ANTI")
                if [ "$t" = big ]
                then
                    or_kinds=("INNER ALL" "RIGHT ALL" "LEFT ANY" "LEFT SEMI")
                fi
                for kind in "${or_kinds[@]}"
                do
                    local columns="cityHash64(p.k), p.w, cityHash64(b.k), b.v"
                    case $kind in
                        "LEFT ANY") columns="cityHash64(p.k), p.w, cityHash64(substring(b.k, 1, 15))" ;;
                        "LEFT SEMI" | "LEFT ANTI") columns="cityHash64(p.k), p.w" ;;
                    esac
                    query "$mode" "$c:or_${kind// /_}" "SELECT $columns $from $kind JOIN $right AS b $on_or" "$settings_or"
                done
            fi

            if [ "$t" = s ] || [ "$t" = n ] || [ "$t" = lc ]
            then
                for kind in "INNER ALL" "LEFT ALL" "FULL ALL"
                do
                    query "$mode" "$c:using_${kind// /_}" "SELECT cityHash64(k), p.w, b.v $from $kind JOIN $right AS b USING (k)" "$settings"
                done
            fi
        done
    done

    # Joins whose keys are not a single `String`: the maps `hashed` and `keys256`.
    local settings c="neg:hash" all="SELECT cityHash64(p.k), p.w, cityHash64(b.k), b.v FROM p_s AS p INNER ALL JOIN b_s AS b"
    settings=$(settings_of hash)
    query "$mode" "$c:or_w" "$all ON p.k = b.k OR p.w = b.v" "$settings"
    query "$mode" "$c:and_w" "$all ON p.k = b.k AND p.w = b.v" "$settings"
    query "$mode" "$c:fixed_string" "$all ON toFixedString(substring(p.k, 1, 17), 17) = toFixedString(substring(b.k, 1, 17), 17)" "$settings"

    # Tables with the `Join` engine keep exact keys, so that they can be read back.
    query "$mode" "j:engine:select" "SELECT cityHash64(k), v FROM j_all" "join_algorithm = 'hash'"
    query "$mode" "j:engine:join" "SELECT cityHash64(p.k), p.w, j.v FROM p_s AS p ANY LEFT JOIN j_any AS j USING (k)" "join_algorithm = 'hash'"
    query "$mode" "j:engine:join_get" "SELECT joinGet('j_any', 'v', k) FROM p_s" "join_algorithm = 'hash'"
}

OUT_OFF="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_off.tsv"
OUT_ON="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_on.tsv"
# The right tables have fewer rows than the default `min_rows_for_packed_string_keys_in_hash_join`.
queries off | $CLICKHOUSE_CLIENT --enable_packed_string_keys_in_hash_join=0 > "$OUT_OFF"
queries on | $CLICKHOUSE_CLIENT --enable_packed_string_keys_in_hash_join=1 --min_rows_for_packed_string_keys_in_hash_join=0 > "$OUT_ON"

if cmp -s "$OUT_OFF" "$OUT_ON"
then
    echo "results are identical"
else
    echo "results differ:"
    diff "$OUT_OFF" "$OUT_ON" | head -n 20
fi

# The number of rows of each query, so that an empty result does not pass unnoticed.
awk '{ print $1, $2 }' "$OUT_ON"
rm -f "$OUT_OFF" "$OUT_ON"

# With the setting on, every `hash` and `grace_hash` join on a single `String` key must use the packed keys, and no
# other join may: not the joins on `LowCardinality(Nullable(String))` keys or on the keys of `neg`, and not the tables
# with the `Join` engine. A join with one condition, not ASOF, on a non-nullable `LowCardinality(String)` key may use
# the hash table of its dictionary instead, depending on the plan. Its `expected` is NULL, so either way passes.
#
# The spilling joins that keep every right row must switch to `grace_hash` after they have inserted rows with packed
# keys. The others keep a smaller hash table, which may stay below the threshold. Fewer than all the rows of `b_big` go
# in before the switch, and `grace_hash` inserts all of them again with packed keys. So a join that switched inserts
# more packed keys than `b_big` has rows. A join with `OR` inserts a key for each of its two conditions.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;

    CREATE TABLE used ENGINE = Memory AS
    SELECT splitByChar(':', log_comment)[2] AS mode, splitByChar(':', log_comment)[3] AS t,
        splitByChar(':', log_comment)[4] AS algorithm, splitByChar(':', log_comment)[5] AS kind,
        substring(log_comment, length('05460:') + length(mode) + 2) AS config,
        ProfileEvents['JoinPackedStringKeysInline'] AS inline_keys, ProfileEvents['JoinPackedStringKeysHashed'] AS hashed_keys,
        ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0 AS spilled,
        multiIf(
            mode = 'off' OR t IN ('lcn', 'neg', 'j'), 0,
            t = 'lc' AND NOT endsWith(kind, 'ASOF') AND NOT startsWith(kind, 'or_'), NULL,
            1) AS expected
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND startsWith(log_comment, '05460:');

    SELECT 'queries', mode, count(), countIf(expected = 1) FROM used GROUP BY mode ORDER BY mode;
    SELECT 'used where not expected', config, mode FROM used WHERE inline_keys + hashed_keys > 0 AND expected = 0 ORDER BY ALL;
    SELECT 'not used where expected', config FROM used WHERE inline_keys + hashed_keys = 0 AND expected = 1 ORDER BY ALL;
    SELECT 'no inline keys', config FROM used WHERE inline_keys = 0 AND expected = 1 AND NOT startsWith(config, 'c:') ORDER BY ALL;
    SELECT 'no hashed keys', config FROM used WHERE hashed_keys = 0 AND expected = 1 ORDER BY ALL;
    SELECT 'did not spill', config, mode FROM used
    WHERE algorithm = 'hspill' AND NOT spilled AND (config LIKE '%_ALL' OR config LIKE '%:RIGHT_%')
    ORDER BY ALL;
    SELECT 'grace after the switch kept exact keys', config FROM used
    WHERE expected = 1 AND algorithm = 'hspill' AND spilled AND inline_keys + hashed_keys <= (SELECT count() FROM b_big)
    ORDER BY ALL;
    SELECT 'OR keys', config, inline_keys + hashed_keys FROM used
    WHERE mode = 'on' AND (startsWith(config, 's:hash:or_') AND inline_keys + hashed_keys != 2 * (SELECT count() FROM b_s)
        OR startsWith(config, 'big:hash:or_') AND inline_keys + hashed_keys != 2 * (SELECT count() FROM b_big))
    ORDER BY ALL;
"
