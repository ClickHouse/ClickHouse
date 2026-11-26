#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.
# Tag no-replicated-database: IcebergLocal is not replicated.

# Iceberg `list` and `map` types are written with only the members the spec defines, and two copies of one schema-id
# that differ only in a member the spec does not define are the same schema: https://github.com/ClickHouse/ClickHouse/issues/131156

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}"
cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

# `members` prints the member names of every list and map type of the latest metadata version. Other modes are applied
# in order and publish the next version: `add-members` / `drop-members` add / remove a non-spec member in every list
# (`required`) and map (`element-required`); `flip-arr` / `flip-m` toggle `element-required` of `arr` / `value-required` of `m`.
metadata() {
    python3 - "${TABLE_PATH}metadata" "$@" <<'PY'
import json, os, re, sys

metadata_dir, modes = sys.argv[1], sys.argv[2:]
versions = [int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f))]
latest = max(versions)
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as fh:
    metadata = json.load(fh)


def types(t):
    if isinstance(t, dict):
        yield t
        for field in t.get("fields", []):
            yield from types(field["type"])
        for key in ("element", "key", "value"):
            if key in t:
                yield from types(t[key])


nested = [t for schema in metadata["schemas"] for t in types(schema) if t["type"] in ("list", "map")]
if modes == ["members"]:
    print("\n".join(sorted({t["type"] + " " + ",".join(sorted(t)) for t in nested})))
    sys.exit(0)

for mode in modes:
    for t in nested:
        member = "required" if t["type"] == "list" else "element-required"
        if mode == "add-members":
            t[member] = False
        elif mode == "drop-members":
            t.pop(member, None)
    for schema in metadata["schemas"]:
        for field in schema["fields"]:
            if field["name"] == "arr" and mode == "flip-arr":
                field["type"]["element-required"] = not field["type"]["element-required"]
            elif field["name"] == "m" and mode == "flip-m":
                field["type"]["value-required"] = not field["type"]["value-required"]

metadata["last-updated-ms"] += 60000
tmp = os.path.join(metadata_dir, ".tmp_next")
with open(tmp, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp, os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"))
PY
}

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64, arr Array(Nullable(Int64)), m Map(String, Array(Int64))) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, [1, NULL], {'a': [2]})"
metadata members

# What earlier versions wrote, then what PyIceberg writes back, for the same schema-id.
metadata add-members
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}"
metadata drop-members
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}"

# A change in a member the spec defines is still a different schema, for a list and for a map.
metadata flip-arr
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}" 2>&1 | grep -o -m1 ICEBERG_SPECIFICATION_VIOLATION
metadata flip-arr flip-m
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}" 2>&1 | grep -o -m1 ICEBERG_SPECIFICATION_VIOLATION
