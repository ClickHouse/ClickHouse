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

# `members` prints the member names of every list and map type of the latest metadata version. The other modes
# publish the next version: `add-required` / `drop-required` add / remove a `required` member in every list type,
# `flip` changes `element-required` of `arr`.
metadata() {
    python3 - "$1" "${TABLE_PATH}metadata" <<'PY'
import json, os, re, sys

mode, metadata_dir = sys.argv[1], sys.argv[2]
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
if mode == "members":
    print("\n".join(sorted({t["type"] + " " + ",".join(sorted(t)) for t in nested})))
    sys.exit(0)

for t in nested:
    if t["type"] == "list" and mode == "add-required":
        t["required"] = False
    elif t["type"] == "list" and mode == "drop-required":
        t.pop("required", None)
for schema in metadata["schemas"]:
    for field in schema["fields"]:
        if field["name"] == "arr" and mode == "flip":
            field["type"]["element-required"] = True

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
metadata add-required
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}"
metadata drop-required
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}"

# A change in a member the spec defines is still a different schema.
metadata flip
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --query "SELECT * FROM ${TABLE}" 2>&1 | grep -o -m1 ICEBERG_SPECIFICATION_VIOLATION
