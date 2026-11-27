#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# Mutations of format-version 3 Iceberg tables whose data files predate the current table state:
# - the first `UPDATE` after an upgrade from format-version 2 reads rows without `_row_id`;
# - a deletion vector of a data file written under an older partition spec, whose source column the current schema dropped,
#   must keep matching that data file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_v3_mutation_evolution"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

SETTINGS="--allow_insert_into_iceberg=1 --max_insert_threads=1 --max_threads=1"

# Writes the next metadata file, changed by the Python code in $2, which gets the parsed metadata as `metadata`.
evolve_metadata()
{
    python3 - "$1/metadata" "$2" <<'PY'
import glob, json, os, re, sys
directory, code = sys.argv[1], sys.argv[2]
version = lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1))
path = max(glob.glob(os.path.join(directory, "v*.metadata.json")), key=version)
with open(path) as f:
    metadata = json.load(f)
exec(code)
with open(os.path.join(directory, f"v{version(path) + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)
PY
}

echo "-- the first UPDATE after an upgrade from format-version 2"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE u (k Int32, v String) ENGINE = IcebergLocal('${LAKE}/u') SETTINGS iceberg_format_version = 2;
    INSERT INTO u SELECT number, toString(number) FROM numbers(5);"
evolve_metadata "${LAKE}/u" '
metadata["format-version"] = 3
metadata["next-row-id"] = 0'
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "ALTER TABLE u UPDATE v = 'updated' WHERE k = 2 SETTINGS mutations_sync = 1;"
${CLICKHOUSE_CLIENT} --query "SELECT k, v FROM icebergLocal('${LAKE}/u') ORDER BY k"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(_row_id), countIf(_row_id IS NULL) FROM icebergLocal('${LAKE}/u')"

echo "-- deletes of a data file of an older partition spec whose source column was dropped"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE p (k Int32, p String) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (p) SETTINGS iceberg_format_version = 3;
    INSERT INTO p SELECT number, if(number % 2 = 0, 'even', 'odd') FROM numbers(10);"
evolve_metadata "${LAKE}/p" '
schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
new_schema_id = max(s["schema-id"] for s in metadata["schemas"]) + 1
metadata["schemas"].append(dict(schema, **{"schema-id": new_schema_id, "fields": [f for f in schema["fields"] if f["name"] != "p"]}))
metadata["current-schema-id"] = new_schema_id
new_spec_id = max(s["spec-id"] for s in metadata["partition-specs"]) + 1
metadata["partition-specs"].append({"spec-id": new_spec_id, "fields": []})
metadata["default-spec-id"] = new_spec_id'
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE p_evolved ENGINE = IcebergLocal('${LAKE}/p');
    DELETE FROM p_evolved WHERE k IN (1, 2);"
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(k) FROM (SELECT k FROM icebergLocal('${LAKE}/p') ORDER BY k)"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "DELETE FROM p_evolved WHERE k IN (3, 4);"
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(k) FROM (SELECT k FROM icebergLocal('${LAKE}/p') ORDER BY k)"

rm -rf "${LAKE}"
