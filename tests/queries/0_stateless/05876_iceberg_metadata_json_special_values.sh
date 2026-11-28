#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# metadata.json with unusual JSON values: Int64 and UInt64 limits, doubles, null, escapes, surrogate pairs,
# raw UTF-8. Reading the table, `system.iceberg_history` and writing a new metadata version
# (which copies the parsed metadata and serializes it back) must keep every value.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}" 2>/dev/null
trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int64, \`ключ \"q\" \\\\ / é 😀\ttab\` String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a')"

# Publishes the next metadata version, serialized with non-ASCII characters escaped as \uXXXX ($1 = ascii)
# or as raw UTF-8, with extra values that the reader and the writer must keep as is.
publish()
{
    python3 - "${TABLE_PATH}metadata" "$1" <<'PY'
import json, os, re, sys

metadata_dir, ensure_ascii = sys.argv[1], sys.argv[2] == "ascii"
latest = max((f for f in os.listdir(metadata_dir) if re.fullmatch(r"v\d+\.metadata\.json", f)), key=lambda f: int(f[1:].split(".")[0]))
with open(os.path.join(metadata_dir, latest)) as fh:
    metadata = json.load(fh)

metadata.setdefault("properties", {})["ünïcødé ключ"] = "значение \U0001F600"
metadata["properties"]["escaped"] = "a\"b\\c/d\be\ff\ng\rh\ti\u0000j\u001f"
metadata["x-clickhouse-test"] = {
    "null": None,
    "max_int64": 9223372036854775807,
    "min_int64": -9223372036854775808,
    "max_uint64": 18446744073709551615,
    "zero": 0,
    "negative_zero": -0.0,
    "double": 1.5e300,
    "small_double": -2.5e-308,
    "fraction": 0.1,
    "whole_double": 3.0,
    "booleans": [True, False],
    "nested": [[[]], {}, [{"a": [None]}]],
}
metadata["last-updated-ms"] += 1000

version = int(latest[1:].split(".")[0]) + 1
tmp = os.path.join(metadata_dir, ".tmp_next")
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(metadata, fh, ensure_ascii=ensure_ascii)
os.rename(tmp, os.path.join(metadata_dir, f"v{version}.metadata.json"))
PY
}

# Prints the values of the latest metadata version that `publish` added.
show_latest()
{
    python3 - "${TABLE_PATH}metadata" <<'PY'
import json, os, re, sys

metadata_dir = sys.argv[1]
latest = max((f for f in os.listdir(metadata_dir) if re.fullmatch(r"v\d+\.metadata\.json", f)), key=lambda f: int(f[1:].split(".")[0]))
with open(os.path.join(metadata_dir, latest), encoding="utf-8") as fh:
    metadata = json.load(fh)
schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
print(ascii([f["name"] for f in schema["fields"]]))
print(ascii({k: v for k, v in sorted(metadata.get("properties", {}).items()) if k in ("escaped", "ünïcødé ключ")}))
for key, value in sorted(metadata["x-clickhouse-test"].items()):
    print(key, type(value).__name__, repr(value))
PY
}

read_table()
{
    ${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "DESCRIBE TABLE ${TABLE} SETTINGS describe_compact_output = 1"
    ${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE} ORDER BY c0"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(is_current_ancestor)
        FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
}

echo "--- escaped as \\uXXXX"
publish ascii
read_table

echo "--- raw UTF-8"
publish utf8
read_table

echo "--- written by INSERT"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --iceberg_metadata_staleness_ms=0 --query "INSERT INTO ${TABLE} VALUES (2, 'b')"
show_latest
read_table

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
