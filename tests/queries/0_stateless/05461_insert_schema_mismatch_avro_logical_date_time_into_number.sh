#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Avro` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Avro` schema inference reports an `int` annotated as `date` as `Date32` and a `long` annotated as
# `timestamp-millis` as `DateTime64(3)`, but `AvroDeserializer` reads the physical integer of both
# into a numeric column with `insertNumber`. So a parse error on a sibling column must not pick up a
# misleading structure-mismatch explanation because of such a field, and the same data with a valid
# sibling value is inserted successfully.

PHRASE="does not match the structure expected by the query"

DATA_BAD=$CLICKHOUSE_TMP/data_05461_bad.avro
DATA_GOOD=$CLICKHOUSE_TMP/data_05461_good.avro

$CLICKHOUSE_LOCAL -q "SELECT toDate32('1970-01-02') AS d, toDateTime64(5, 3, 'UTC') AS t, 'not-a-uuid' AS u FORMAT Avro" > "$DATA_BAD"
$CLICKHOUSE_LOCAL -q "SELECT toDate32('1970-01-02') AS d, toDateTime64(5, 3, 'UTC') AS t, '00000000-0000-0000-0000-000000000001' AS u FORMAT Avro" > "$DATA_GOOD"

for dest in "d UInt8, t Int64" "d Float64, t UInt64" "d Int32, t Decimal64(0)"; do
    echo "-- ($dest, u UUID): only u is invalid"
    $CLICKHOUSE_LOCAL -q "CREATE TABLE t ($dest, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Avro" < "$DATA_BAD" 2>&1 | {
        out=$(cat)
        if echo "$out" | grep -q "CANNOT_PARSE_UUID"; then echo "parse error as expected"; else echo "unexpected error"; fi
        if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
    }
    echo "-- ($dest, u UUID): a valid u is read"
    $CLICKHOUSE_LOCAL --input-format Avro --structure "$dest, u UUID" -q "SELECT * FROM table" < "$DATA_GOOD" 2>&1
done

rm -f "$DATA_BAD" "$DATA_GOOD"
