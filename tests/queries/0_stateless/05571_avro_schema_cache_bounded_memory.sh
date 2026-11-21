#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# A short reference to a named type is printed as its full name in the Parsing Canonical Form of a schema, so a
# compact schema can have a canonical form millions of times larger than its text: a record in a namespace of 4000
# characters whose 10000 fields refer to one `fixed` type has 283 KB of text and 40 MB of canonical form. What the
# Avro reader keeps for a schema, in particular the identity of its cached program, must be as large as the text,
# not as the canonical form, and the cache of programs must have a bound in bytes.

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


def ocf(path, schema, rows):
    """One block; `rows` are the encoded rows."""
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(b"null") + vlong(0)
    sync = bytes(range(16))
    payload = b"".join(rows)
    open(path, "wb").write(b"Obj\x01" + meta + sync + vlong(len(rows)) + vlong(len(payload)) + payload + sync)


def shared_fixed(path, namespace_char, namespace_len, fields):
    """A record in a namespace of `namespace_len` characters; every field but the first refers to the `fixed(1)` type of the first."""
    fixed = {"type": "fixed", "name": "F", "size": 1}
    record = {
        "type": "record",
        "name": "R",
        "namespace": namespace_char * namespace_len,
        "fields": [{"name": "f0", "type": fixed}] + [{"name": f"f{i}", "type": "F"} for i in range(1, fields)],
    }
    ocf(path, json.dumps(record, separators=(",", ":")).encode(), [b"\x01" * fields])


# 283 KB of text, 40 MB of canonical form.
shared_fixed(f"{work}/big.avro", "n", 4000, 10000)
# Eight schemas that differ only in the namespace: 283 KB of text and 10 MB of canonical form each, 80 MB of it together.
for i in range(8):
    shared_fixed(f"{work}/many_{i}.avro", chr(ord("a") + i), 1000, 10000)

# The same canonical form, spelled differently: a cache keyed by it has to tell what the programs read.
plain = {"type": "record", "name": "T", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "string"}]}
spelled = {
    "name": "T",
    "doc": "the same record, with attributes that the canonical form drops",
    "type": "record",
    "fields": [
        {"type": "int", "doc": "x", "name": "x", "default": 0},
        {"type": {"type": "string"}, "name": "y", "aliases": ["z"]},
    ],
}
ocf(f"{work}/twin_a.avro", json.dumps(plain).encode(), [vlong(1) + avstr(b"a")])
ocf(f"{work}/twin_b.avro", json.dumps(spelled).encode(), [vlong(2) + avstr(b"b")])
PYEOF

# One read of the 40 MB canonical form schema, under a limit that the form does not fit.
echo "one read"
${CLICKHOUSE_LOCAL} --input-format Avro -S "f0 FixedString(1), f9999 FixedString(1)" \
    --query "SELECT hex(f0), hex(f9999), count() FROM table GROUP BY ALL SETTINGS max_memory_usage = 25165824" < "${WORK_DIR}/big.avro"

# Eight distinct schemas read by one query: the keys of their programs are kept, and 80 MB of canonical forms do not fit the limit.
echo "distinct schemas"
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(reinterpretAsUInt8(f0)) FROM file('${WORK_DIR}/many_*.avro', Avro, 'f0 FixedString(1)') SETTINGS max_memory_usage = 67108864, max_threads = 1"

echo "same canonical form"
${CLICKHOUSE_LOCAL} --query "SELECT x, y FROM file('${WORK_DIR}/twin_*.avro', Avro, 'x Int32, y String') ORDER BY x SETTINGS max_threads = 1"
