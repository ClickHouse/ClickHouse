#!/usr/bin/env bash
# Tags: long, no-msan
# no-msan: its ~190 joins take minutes under MSan and seconds in release; ASan, UBSan and TSan still run it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Joins on keys that use the `hashed` and `two_level_hashed` maps, where a key is the `sipHash128` of the key columns of
# a row, built for many rows at a time. The keys must be equal to those that `hash128` builds row by row.
#
# Every key column is a function of `g`. A row of `r` or `l` has a unique `id`. `r` has the keys of g = 1 to 2400, those
# up to 800 twice. `l` has those of g = 301 to 3300, those up to 600 twice. A `String` key has 0 to 102 bytes, some with
# '\0' bytes, and one row in 500 has 100 KB.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE keys
    (
        g UInt64,
        u1 UInt64, u2 UInt64, u3 UInt64, u4 UInt64, u5 UInt64,
        uuid UUID, w UInt256, d Decimal256(10), f Float64,
        s String, s2 String,
        fs1 FixedString(1), fs2 FixedString(2), fs3 FixedString(3), fs4 FixedString(4), fs5 FixedString(5),
        fs6 FixedString(6), fs7 FixedString(7), fs8 FixedString(8), fs9 FixedString(9), fs33 FixedString(33),
        fs40 FixedString(40),
        ns Nullable(String), nu Nullable(UInt64), lcs LowCardinality(String),
        arr Array(UInt32), m Map(String, UInt64), t1 Tuple(UInt64), tup Tuple(UInt64, String),
        tn Tuple(Nullable(String), UInt64), tlc Tuple(LowCardinality(String))
    ) ENGINE = Memory;

    INSERT INTO keys
    SELECT g, g, g * 7, g % 13, intHash64(g), g + 1000000,
        reinterpretAsUUID(sipHash128(toString(g))), toUInt256(g) * 1000000007, toDecimal256(g, 10) / 7,
        multiIf(g % 5 = 0, -0., g % 5 = 1, 0., g % 5 = 2, nan, g / 3),
        s, s2,
        toFixedString(substring(h, 1, 1), 1), toFixedString(substring(h, 1, 2), 2), toFixedString(substring(h, 1, 3), 3),
        toFixedString(substring(h, 1, 4), 4), toFixedString(substring(h, 1, 5), 5), toFixedString(substring(h, 1, 6), 6),
        toFixedString(substring(h, 1, 7), 7), toFixedString(substring(h, 1, 8), 8), toFixedString(substring(h, 1, 9), 9),
        toFixedString(substring(h, 1, 33), 33), toFixedString(substring(h, 1, 40), 40),
        if(g % 6 = 0, NULL, s), if(g % 4 = 0, NULL, g % 1000), s2,
        range(toUInt32(g % 5)), map(toString(g % 3), g % 7), tuple(g % 1000), (g, s),
        (if(g % 6 = 0, NULL, s2), g % 3), tuple(s2)
    FROM
    (
        SELECT number + 1 AS g, repeat(hex(sipHash128(toString(g))), 4) AS h,
            [0, 1, 7, 8, 9, 15, 16, 17, 100][g % 9 + 1] AS len,
            multiIf(
                g % 500 = 3, repeat(substring(h, 1, 32), 3200),
                g % 4 = 0, concat('\0', substring(h, 1, len)),
                g % 4 = 1, concat(substring(h, 1, len), '\0', 'x'),
                substring(h, 1, len)) AS s,
            substring(hex(sipHash128(toString(g * 3))), 1, g % 23) AS s2
        FROM numbers(3300)
    );

    CREATE TABLE r ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128 AS
    SELECT id, g, k.*, if(id % 7 = 0, NULL, id * 10) AS nts
    FROM (SELECT number + 1 AS id, (id - 1) % 2400 + 1 AS g FROM numbers(3200)) AS ids JOIN keys AS k USING (g)
    SETTINGS join_algorithm = 'hash';

    CREATE TABLE l ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128 AS
    SELECT id, g, k.*, id * 10 + 3 AS ts
    FROM (SELECT number + 1 AS id, (id - 1) % 3000 + 301 AS g FROM numbers(3300)) AS ids JOIN keys AS k USING (g)
    SETTINGS join_algorithm = 'hash';
"

function settings_of()
{
    case $1 in
        hash) echo "join_algorithm = 'hash'" ;;
        parallel1) echo "join_algorithm = 'parallel_hash', max_threads = 1, max_bytes_ratio_before_external_join = 0" ;;
        # The tables are below `parallel_hash_join_threshold`, so `hash_join_layout` asks for the parallel layout.
        parallel8) echo "join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 8, max_bytes_ratio_before_external_join = 0" ;;
        grace) echo "join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4" ;;
        spill) echo "join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4, max_bytes_before_external_join = 2000000, max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 2" ;;
        auto) echo "join_algorithm = 'auto'" ;;
    esac
}

# The rows of a join reduced to one line. Which right row an ANY join takes, and which left row a RIGHT ANY join takes,
# can depend on the order of the blocks, so only the rows of the other side and whether they matched are summed.
function checksum_of()
{
    case $1 in
        "INNER ALL"|"LEFT ALL"|"RIGHT ALL"|"FULL ALL") echo "l.id, r.id" ;;
        "LEFT ANY"|"LEFT SEMI"|"LEFT ANTI") echo "l.id, r.id != 0" ;;
        "RIGHT ANY"|"RIGHT SEMI"|"RIGHT ANTI") echo "r.id, l.id != 0" ;;
        *) echo "l.id != 0, r.id != 0" ;;
    esac
}

# Prints a query: LABEL KIND ON SETTINGS.
function query()
{
    local label=$1 kind=$2 on=$3 settings=$4
    echo "SELECT '$label', count(), sum(cityHash64($(checksum_of "$kind"))) FROM l $kind JOIN r ON $on SETTINGS $settings, max_block_size = 997;"
}

declare -A keys=(
    [u64x5]="l.u1 = r.u1 AND l.u2 = r.u2 AND l.u3 = r.u3 AND l.u4 = r.u4 AND l.u5 = r.u5"
    [uuid_w]="l.uuid = r.uuid AND l.w = r.w"
    [w_u64]="l.w = r.w AND l.u1 = r.u1"
    [d_u64]="l.d = r.d AND l.u3 = r.u3"
    [s_u64]="l.s = r.s AND l.u1 = r.u1"
    [s_s]="l.s = r.s AND l.s2 = r.s2"
    [f_s]="l.f = r.f AND l.s2 = r.s2"
    [t1]="l.t1 = r.t1"
    [tup]="l.tup = r.tup"
    [ns_u64]="l.ns = r.ns AND l.u1 = r.u1"
    [lcs_u64]="l.lcs = r.lcs AND l.u3 = r.u3"
    [arr_u64]="l.arr = r.arr AND l.u3 = r.u3"
    [map]="l.m = r.m"
    [tn]="l.tn = r.tn"
    [tlc]="l.tlc = r.tlc"
    [nullsafe_s]="l.ns <=> r.ns"
    [nullsafe_s_u64]="l.ns <=> r.ns AND l.u3 = r.u3"
    [nullsafe_u64_s]="l.nu <=> r.nu AND l.s2 = r.s2"
)

for key in $(printf '%s\n' "${!keys[@]}" | sort)
do
    for algorithm in hash parallel8
    do
        query "$key $algorithm" "INNER ALL" "${keys[$key]}" "$(settings_of "$algorithm")"
    done
done | $CLICKHOUSE_CLIENT

# The other algorithms take the keys from the same getter, so a key of each layout is enough.
for key in u64x5 s_u64 tn nullsafe_s
do
    for algorithm in parallel1 grace spill auto
    do
        query "$key $algorithm" "INNER ALL" "${keys[$key]}" "$(settings_of "$algorithm")"
    done
done | $CLICKHOUSE_CLIENT

# `FixedString` keys of each width up to 9, and wider ones, with a `UInt256`, so that a key does not fit in 32 bytes.
for n in 1 2 3 4 5 6 7 8 9 33 40
do
    for algorithm in hash parallel8
    do
        # `parallel8` differs from `hash` only in the scatter, so a few widths are enough for it.
        [ $algorithm = parallel8 ] && [[ $n != 1 && $n != 8 && $n != 33 ]] && continue
        query "fs$n $algorithm" "INNER ALL" "l.fs$n = r.fs$n AND l.w = r.w" "$(settings_of "$algorithm")"
    done
done | $CLICKHOUSE_CLIENT

for key in u64x5 tn
do
    for kind in "INNER ALL" "LEFT ALL" "RIGHT ALL" "FULL ALL" "INNER ANY" "LEFT ANY" "RIGHT ANY" "LEFT SEMI" "RIGHT SEMI" "LEFT ANTI" "RIGHT ANTI"
    do
        for algorithm in hash parallel8
        do
            query "$key $kind $algorithm" "$kind" "${keys[$key]}" "$(settings_of "$algorithm")"
        done
    done
done | $CLICKHOUSE_CLIENT

# A join skips the rows with a `NULL` key or a false `ON` condition, so the keys of a chunk with many of them are built
# from the other rows.
for kind in "INNER ALL" "LEFT ALL"
do
    for algorithm in hash parallel8
    do
        settings=$(settings_of "$algorithm")
        query "nu_u64x4 $kind $algorithm" "$kind" "l.nu = r.nu AND l.u1 = r.u1 AND l.u2 = r.u2 AND l.u3 = r.u3 AND l.u4 = r.u4" "$settings"
        query "t1 with condition $kind $algorithm" "$kind" "l.t1 = r.t1 AND l.g % 4 != 0" "$settings"
        query "u64x5 with condition $kind $algorithm" "$kind" "${keys[u64x5]} AND l.g % 3 = 0" "$settings"
        query "s_u64 with condition $kind $algorithm" "$kind" "${keys[s_u64]} AND l.g % 5 != 1" "$settings"
    done
done | $CLICKHOUSE_CLIENT

# The maps of an `OR` join have one map type, but each has its own key columns. With a condition in `ON`, a row is
# looked up in every map.
for kind in "INNER ALL" "LEFT ALL" "RIGHT ALL" "FULL ALL" "LEFT SEMI" "LEFT ANTI" "RIGHT ANTI"
do
    for algorithm in hash parallel8
    do
        # As above, two kinds are enough for `parallel8`.
        [ $algorithm = parallel8 ] && [[ $kind != "INNER ALL" && $kind != "FULL ALL" ]] && continue
        settings=$(settings_of "$algorithm")
        query "or u64 | s_u64 $kind $algorithm" "$kind" "l.u1 = r.u1 OR l.s = r.s AND l.u3 = r.u3" "$settings"
        query "or u64x2 | s $kind $algorithm" "$kind" "(l.u1 = r.u1 AND l.u2 = r.u2) OR l.s = r.s" "$settings"
        query "or nullsafe_s | w_u64 $kind $algorithm" "$kind" "l.ns <=> r.ns OR l.w = r.w AND l.u1 = r.u1" "$settings"
    done
done | $CLICKHOUSE_CLIENT

for kind in "INNER ALL" "LEFT ALL" "LEFT SEMI" "LEFT ANTI"
do
    query "or condition $kind" "$kind" "(l.s = r.s AND l.u1 = r.u1 AND l.id < r.id) OR (l.u2 = r.u2 AND l.tup = r.tup AND l.g > r.g - 5) OR (l.tn = r.tn AND l.u3 = r.u3 AND l.id % 3 = r.id % 3)" "$(settings_of hash)"
done | $CLICKHOUSE_CLIENT

# A `Nullable` ASOF column: the build reads only the rows without a NULL in it.
for kind in "ASOF" "ASOF LEFT"
do
    for algorithm in hash parallel8
    do
        echo "SELECT '$kind $algorithm', count(), sum(cityHash64(l.id, r.id)) FROM l $kind JOIN r ON l.s = r.s AND l.u3 = r.u3 AND l.ts >= r.nts SETTINGS $(settings_of "$algorithm");"
    done
done | $CLICKHOUSE_CLIENT

# Block sizes around the chunks of 256 rows, and a probe that stops in the middle of a chunk.
for max_block_size in 1 7 8 9 255 256 257 65505
do
    for algorithm in hash parallel8
    do
        # As above, two block sizes are enough for `parallel8`.
        [ $algorithm = parallel8 ] && [[ $max_block_size != 7 && $max_block_size != 256 ]] && continue
        for key in s_u64 tn
        do
            echo "SELECT '$key $algorithm max_block_size $max_block_size', count(), sum(cityHash64(l.id, r.id)) FROM l JOIN r ON ${keys[$key]} SETTINGS $(settings_of "$algorithm"), max_block_size = $max_block_size, max_joined_block_size_rows = 100;"
        done
    done
done | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT -q "
    SELECT 'constant key', count(), sum(cityHash64(l.id, r.id)) FROM (SELECT *, toUInt64(5) AS c FROM l) AS l JOIN r ON l.c = r.u3 AND l.s = r.s;
    SELECT 'empty tuple', count() FROM (SELECT tuple() AS e, number AS id FROM numbers(3)) AS a JOIN (SELECT tuple() AS e, number AS id FROM numbers(4)) AS b ON a.e = b.e SETTINGS enable_join_runtime_filters = 0;
    SELECT 'empty tuple and UInt64', count() FROM (SELECT tuple() AS e, number AS id FROM numbers(3)) AS a JOIN (SELECT tuple() AS e, number AS id FROM numbers(4)) AS b ON a.e = b.e AND a.id = b.id SETTINGS enable_join_runtime_filters = 0;
    SELECT 'correlated', count(), sum(cityHash64(id)) FROM l WHERE EXISTS (SELECT 1 FROM r WHERE r.ns = l.ns AND r.u3 = l.u3);

    CREATE TABLE j_s_u64 (s String, u1 UInt64, id UInt64) ENGINE = Join(ANY, LEFT, s, u1);
    INSERT INTO j_s_u64 SELECT s, u1, id FROM r WHERE id <= 2400;
    CREATE TABLE j_tn (tn Tuple(Nullable(String), UInt64), id UInt64) ENGINE = Join(ANY, LEFT, tn);
    INSERT INTO j_tn SELECT tn, min(id) FROM r GROUP BY tn;
"

for step in before after
do
    [ $step = after ] && $CLICKHOUSE_CLIENT -q "DETACH TABLE j_s_u64; ATTACH TABLE j_s_u64; DETACH TABLE j_tn; ATTACH TABLE j_tn;"
    $CLICKHOUSE_CLIENT -q "
        -- With parallel replicas, joinGet can look the table up in the default database, not in the test database.
        SELECT 'joinGet $step', count(), sum(cityHash64(id, joinGet('j_s_u64', 'id', s, u1))) FROM l SETTINGS enable_parallel_replicas = 0;
        SELECT 'joinGetOrNull $step', countIf(isNull(joinGetOrNull('j_s_u64', 'id', s, u1))), sum(cityHash64(id, joinGetOrNull('j_s_u64', 'id', s, u1))) FROM l SETTINGS enable_parallel_replicas = 0;
        SELECT 'joinGet tn $step', count(), sum(cityHash64(id, joinGet('j_tn', 'id', tn))) FROM l SETTINGS enable_parallel_replicas = 0;
        -- Parallel replicas read the Join table to send it to the replicas, and a table with hashed keys cannot be read.
        SELECT 'join with Join $step', count(), sum(cityHash64(l.id, j.id)) FROM l ANY LEFT JOIN j_s_u64 AS j USING (s, u1) SETTINGS enable_parallel_replicas = 0;
    "
done
