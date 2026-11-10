#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# The items that occupy no bytes (`null`, an empty record) of an array that the query reads are not materialized one by
# one: a row may have 16777216 of them for a few bytes of the file, and 8192 such rows (one batch of the reader) are
# 137438953472 items for 40960 bytes. `Array(Nothing)` and `Array(Tuple())` hold them in the offsets of the array alone,
# and a call of a reader for each of the items would take minutes. The values, the lengths and the limit of a row
# (16777216) stay as they are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PYEOF'
import json, sys

work = sys.argv[1]
SYNC = bytes(range(16))
CAP = 1 << 24


def vlong(n):
    u = ((n << 1) ^ (n >> 63)) & ((1 << 64) - 1)
    out = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        out.append(b | 0x80 if u else b)
        if not u:
            break
    return bytes(out)


def avstr(b):
    return vlong(len(b)) + b


def zero_width_array(count):
    """An array of `count` items that occupy no bytes: its only block, and the end of the array."""
    return (vlong(count) + vlong(0)) if count else vlong(0)


def ocf(name, item_type, rows):
    schema = json.dumps({
        "type": "record", "name": "r",
        "fields": [{"name": "a", "type": {"type": "array", "items": item_type}}],
    }).encode()
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(b"null") + vlong(0)
    payload = b"".join(rows)
    with open(f"{work}/{name}.avro", "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC + vlong(len(rows)) + vlong(len(payload)) + payload + SYNC)


empty_record = {"type": "record", "name": "e", "fields": []}
whole_budget = [zero_width_array(CAP) for _ in range(8192)]
ocf("nulls_whole_budget", "null", whole_budget)
ocf("empty_records_whole_budget", empty_record, whole_budget)
ocf("nulls_small", "null", [zero_width_array(n) for n in (0, 3, 1)])
ocf("empty_records_small", empty_record, [zero_width_array(n) for n in (0, 3, 1)])
ocf("nulls_over_budget", "null", [zero_width_array(CAP + 1)])
PYEOF

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "a $2" --query "$3" < "${WORK_DIR}/$1.avro"
}

echo "8192 rows of 16777216 items"
run nulls_whole_budget "Array(Nothing)" "SELECT count(), sum(length(a)) FROM table"
run empty_records_whole_budget "Array(Tuple())" "SELECT count(), sum(length(a)) FROM table"
run nulls_whole_budget "Array(Nothing)" "SELECT count(), sum(length(a)) FROM table SETTINGS max_block_size = 1"

echo "Values"
run nulls_small "Array(Nothing)" "SELECT length(a) FROM table"
run nulls_small "Array(Nullable(Nothing))" "SELECT a FROM table"
run empty_records_small "Array(Tuple())" "SELECT a FROM table"

echo "More than the budget of a row"
${CLICKHOUSE_LOCAL} --input-format Avro -S "a Array(Nothing)" --query "SELECT count() FROM table" < "${WORK_DIR}/nulls_over_budget.avro" 2>&1 \
    | grep -o -m1 "exceeds the limit of 16777216 per row"
