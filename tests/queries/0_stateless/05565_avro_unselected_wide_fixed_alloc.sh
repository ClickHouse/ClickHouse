#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# A `fixed` of a schema-declared size must cost memory only when a value of it is decoded. A ~170-byte file
# whose union has a 64 MiB `fixed` branch that no row selects used to make the Rust Avro decoder stage sixteen
# values of that width (1 GiB) up front and fail a 256 MiB memory limit without reading a single fixed byte.

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


def ocf(path, fixed_size, rows):
    """One block; each row is a record {x: ["null", fixed(fixed_size)]}. A row is None (null branch) or bytes."""
    schema = json.dumps({
        "type": "record", "name": "r",
        "fields": [{"name": "x", "type": ["null", {"type": "fixed", "name": "F", "size": fixed_size}]}],
    }).encode()
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(b"null") + vlong(0)
    sync = bytes(range(16))
    payload = b"".join(vlong(0) if row is None else vlong(1) + row for row in rows)
    open(path, "wb").write(b"Obj\x01" + meta + sync + vlong(len(rows)) + vlong(len(payload)) + payload + sync)


# The 64 MiB branch is never selected.
ocf(f"{work}/unselected.avro", 64 * 1024 * 1024, [None, None, None])
# Small fixed values still decode, between nulls.
ocf(f"{work}/small.avro", 4, [b"abcd", None, b"wxyz"])
# A wide fixed that is selected decodes in full, under the same limit.
ocf(f"{work}/selected_wide.avro", 1024 * 1024, [None, b"z" * (1024 * 1024), None])
# A 64 MiB fixed selected by the only row, with 4 bytes of it present: invalid data, not a memory problem.
ocf(f"{work}/truncated.avro", 64 * 1024 * 1024, [b"abcd"])
PYEOF

LIMIT="SETTINGS max_memory_usage = 268435456"

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "x Nullable(String)" --query "$1" < "${WORK_DIR}/$2"
}

echo "unselected"
run "SELECT count(), countIf(x IS NULL) FROM table $LIMIT" unselected.avro

echo "small"
run "SELECT x FROM table $LIMIT" small.avro

echo "selected_wide"
run "SELECT count(), countIf(x IS NULL), sum(length(x)) FROM table $LIMIT" selected_wide.avro

echo "truncated"
if out=$(run "SELECT count() FROM table $LIMIT" truncated.avro 2>&1); then
    echo "UNEXPECTED success: $out"
else
    # The value is cut short, which is a data error; a memory limit error would mean the 64 MiB was reserved up front.
    if [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]]; then
        echo "UNEXPECTED memory limit: $out"
    else
        echo "rejected"
    fi
fi
