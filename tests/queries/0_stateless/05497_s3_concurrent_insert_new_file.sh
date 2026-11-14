#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

filename="test_05497_${CLICKHOUSE_DATABASE}_${RANDOM}"
out="${CLICKHOUSE_TMP}/05497_${CLICKHOUSE_DATABASE}"
writers=8

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05497"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05497 (a UInt64) ENGINE = S3(s3_conn, filename='$filename', format=Parquet)"

# The next file name is derived from the path list, so the base object must exist for a new one
# to be derived at all.
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 -q "INSERT INTO t_05497 SELECT 0"

# Readers overlap the writer burst so that list reads run concurrently with list writes.
# They assert nothing: a path becomes visible before its object is uploaded, so any row count
# here would be legitimately racy. Their only job is to touch the list concurrently.
# A fixed iteration count, not a loop until the writers finish: a reader that outlives its
# database would keep querying a dropped one.
reader_pids=()
for _ in 1 2; do
    (
        for _ in $(seq 1 10); do
            $CLICKHOUSE_CLIENT --s3_ignore_file_doesnt_exist 1 \
                -q "SELECT count() FROM t_05497" > /dev/null 2>&1 || true
            $CLICKHOUSE_CLIENT --s3_ignore_file_doesnt_exist 1 \
                -q "SELECT count() FROM t_05497 WHERE _file LIKE '%1'" > /dev/null 2>&1 || true
        done
    ) &
    reader_pids+=("$!")
done

# Writers that derive the same name race for it, and the loser has to fail instead of overwriting
# the winner. Each writer records whether its INSERT was acknowledged.
writer_pids=()
for i in $(seq 1 $((writers - 1))); do
    (
        if $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 -q "INSERT INTO t_05497 SELECT $i" 2> "$out.$i.err"; then
            echo "$i" > "$out.$i.ok"
        fi
    ) &
    writer_pids+=("$!")
done

# One writer goes through the asynchronous insert queue: together with a direct INSERT, that is the
# thread pairing of the data race on the path list. VALUES, not SELECT: only an inlined payload is
# queued rather than run synchronously.
(
    if $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 --async_insert 1 --wait_for_async_insert 1 \
        -q "INSERT INTO t_05497 VALUES ($writers)" 2> "$out.$writers.err"; then
        echo "$writers" > "$out.$writers.ok"
    fi
) &
writer_pids+=("$!")

wait "${writer_pids[@]}"
wait "${reader_pids[@]}" 2>/dev/null || true

# A rejected writer must have lost the race for its name; any other error fails the test.
for i in $(seq 1 $writers); do
    if [ ! -f "$out.$i.ok" ] && ! grep -q 'pre-conditions you specified did not hold' "$out.$i.err"; then
        echo "writer $i failed unexpectedly:"
        cat "$out.$i.err"
    fi
done

acknowledged=$(cat "$out".*.ok 2>/dev/null | sort -n | tr '\n' ',')
rm -f "$out".*.ok "$out".*.err
[ -n "$acknowledged" ] || echo "no writer was acknowledged"

# Every acknowledged INSERT is stored in an object of its own and is read back by the table.
$CLICKHOUSE_CLIENT -q "
    SELECT arraySort(groupArray(a)) = arrayConcat([0], [${acknowledged%,}]::Array(UInt64)), countDistinct(_file) = count()
    FROM s3(s3_conn, filename='$filename*', format=Parquet, structure='a UInt64')"
$CLICKHOUSE_CLIENT -q "
    SELECT arraySort(groupArray(a)) = arrayConcat([0], [${acknowledged%,}]::Array(UInt64)) FROM t_05497"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05497"

# Two `s3()` queries do not share a path list, so both derive the name that follows the base object.
# The slow writer derives it first and uploads last, after the fast one has created it: exactly one of
# them may be acknowledged.
conflict="test_05497_conflict_${CLICKHOUSE_DATABASE}_${RANDOM}"
slow_query_id="05497_slow_${CLICKHOUSE_DATABASE}_${RANDOM}"
release="$out.release"
$CLICKHOUSE_CLIENT --s3_truncate_on_insert 1 \
    -q "INSERT INTO FUNCTION s3(s3_conn, filename='$conflict', format=Parquet, structure='a UInt64') SELECT 0"
# The name is derived when the sink is built, before the server asks for data, and the object is uploaded when the
# data ends: the slow writer keeps its input open until the fast one has finished, and sends its row on its own.
(
    if { echo 1; for _ in $(seq 1 3000); do [ -f "$release" ] && break; sleep 0.1; done; } \
        | $CLICKHOUSE_CLIENT --async_insert 0 --input_format_parallel_parsing 0 --max_insert_block_size 1 \
            --s3_create_new_file_on_insert 1 --query_id "$slow_query_id" \
            -q "INSERT INTO FUNCTION s3(s3_conn, filename='$conflict', format=Parquet, structure='a UInt64') FORMAT TSV" \
            2> "$out.slow.err"; then
        echo 1 > "$out.slow.ok"
    fi
) &
slow_pid=$!
# A row the server has received proves that the slow writer has derived its name.
deadline=$((SECONDS + 60))
until [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$slow_query_id' AND written_rows > 0")" = 1 ]; do
    [ "$SECONDS" -lt "$deadline" ] || { echo "the slow writer has not sent its row"; break; }
    sleep 0.1
done
if $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 \
    -q "INSERT INTO FUNCTION s3(s3_conn, filename='$conflict', format=Parquet, structure='a UInt64') SELECT 2" \
    2> "$out.fast.err"; then
    echo 2 > "$out.fast.ok"
fi
touch "$release"
wait "$slow_pid"
rm -f "$release"

for w in slow fast; do
    if [ ! -f "$out.$w.ok" ] && ! grep -q 'pre-conditions you specified did not hold' "$out.$w.err"; then
        echo "$w writer failed unexpectedly:"
        cat "$out.$w.err"
    fi
done
acknowledged=$(cat "$out".*.ok 2>/dev/null | sort -n | tr '\n' ',')
rm -f "$out".*.ok "$out".*.err
echo "acknowledged writers: $(echo -n "${acknowledged%,}" | tr ',' '\n' | grep -c .)"
$CLICKHOUSE_CLIENT -q "
    SELECT arraySort(groupUniqArray(a)) = arrayConcat([0], [${acknowledged%,}]::Array(UInt64))
    FROM s3(s3_conn, filename='$conflict*', format=Parquet, structure='a UInt64')"

# TRUNCATE deletes the objects but keeps their names in the path list: the inserts that follow must
# write to free names, not to a listed one that another insert has already created again.
truncated="test_05497_truncate_${CLICKHOUSE_DATABASE}_${RANDOM}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05497_truncate (a UInt64) ENGINE = S3(s3_conn, filename='$truncated', format=Parquet)"
for i in 1 2; do
    $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 -q "INSERT INTO t_05497_truncate SELECT $i"
done
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_05497_truncate"
for i in 3 4; do
    $CLICKHOUSE_CLIENT --s3_create_new_file_on_insert 1 -q "INSERT INTO t_05497_truncate SELECT $i"
done
$CLICKHOUSE_CLIENT -q "SELECT arraySort(groupArray(a)) FROM s3(s3_conn, filename='$truncated*', format=Parquet, structure='a UInt64')"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_05497_truncate"
