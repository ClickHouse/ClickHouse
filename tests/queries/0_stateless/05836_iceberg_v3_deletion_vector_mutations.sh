#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# DELETE and UPDATE of a format-version 3 Iceberg table write deletion vectors. A snapshot holds at most one deletion
# vector per data file, so a second mutation of a data file merges its previous deletes into a new deletion vector and
# marks the previous one DELETED in a rewritten delete manifest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE_RELATIVE="lakehouses/${CLICKHOUSE_DATABASE}_v3_deletion_vectors"
LAKE="${CLICKHOUSE_USER_FILES}/${LAKE_RELATIVE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

SETTINGS="--allow_insert_into_iceberg=1 --max_insert_threads=1 --max_threads=1"

latest_metadata()
{
    python3 - "$1/metadata" <<'PY'
import glob, os, re, sys
print(max(glob.glob(os.path.join(sys.argv[1], "v*.metadata.json")),
          key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1))))
PY
}

# The summary of the current snapshot, and the entries of its delete manifests.
print_deletes()
{
    local table="$1"
    local metadata
    metadata=$(latest_metadata "${LAKE}/${table}")
    local manifest_list
    manifest_list=$(python3 - "${metadata}" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    metadata = json.load(f)
snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
summary = snapshot["summary"]
keys = ["operation", "added-delete-files", "removed-delete-files", "added-position-deletes", "removed-position-deletes",
        "total-delete-files", "total-position-deletes"]
print(" ".join(f"{key}={summary.get(key, '-')}" for key in keys), file=sys.stderr)
print(snapshot["manifest-list"].rsplit("/", 1)[-1])
PY
)
    local delete_manifests
    delete_manifests=$(${CLICKHOUSE_CLIENT} --query "
        SELECT replaceRegexpOne(manifest_path, '.*/', '')
        FROM file('${LAKE_RELATIVE}/${table}/metadata/${manifest_list}', 'Avro') WHERE content = 1")
    for manifest in ${delete_manifests}; do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT status, data_file.file_format, data_file.record_count, ifNull(data_file.referenced_data_file, '')
            FROM file('${LAKE_RELATIVE}/${table}/metadata/${manifest}', 'Avro') FORMAT TSV"
    done | python3 -c '
import collections, sys
entries = collections.defaultdict(lambda: [0, 0])
live_vectors = collections.Counter()
for line in sys.stdin:
    status, file_format, record_count, referenced_data_file = line.rstrip("\n").split("\t")
    entries[(int(status), file_format)][0] += 1
    entries[(int(status), file_format)][1] += int(record_count)
    if file_format == "puffin" and status != "2":
        live_vectors[referenced_data_file] += 1
for (status, file_format), (count, rows) in sorted(entries.items()):
    print("entries", status, file_format, count, rows, sep="\t")
print("max live vectors per data file", max(live_vectors.values(), default=0), sep="\t")
'
}

echo "-- two DELETEs of the same data file"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE t (k Int32, v String) ENGINE = IcebergLocal('${LAKE}/t') SETTINGS iceberg_format_version = 3;
    INSERT INTO t SELECT number, toString(number) FROM numbers(10);
    INSERT INTO t SELECT number, toString(number) FROM numbers(100, 10);
    DELETE FROM t WHERE k < 3;"
print_deletes t 2>&1
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "DELETE FROM t WHERE k IN (5, 7, 105);"
print_deletes t 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(k) FROM (SELECT k FROM icebergLocal('${LAKE}/t') ORDER BY k)"

echo "-- a DELETE that matches nothing commits nothing"
before=$(latest_metadata "${LAKE}/t")
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "DELETE FROM t WHERE k = 1000;"
[ "$(latest_metadata "${LAKE}/t")" = "${before}" ] && echo "no new metadata"

echo "-- UPDATE"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "ALTER TABLE t UPDATE v = 'updated' WHERE k IN (4, 106) SETTINGS mutations_sync = 1;"
print_deletes t 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT k, v, _row_id, _last_updated_sequence_number FROM icebergLocal('${LAKE}/t') WHERE k IN (3, 4, 5, 6, 105, 106, 107) ORDER BY k"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${LAKE}/t')"

echo "-- partitioned"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE p (k Int32, r String) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r) SETTINGS iceberg_format_version = 3;
    INSERT INTO p SELECT number, if(number % 2 = 0, 'even', 'odd') FROM numbers(20);
    DELETE FROM p WHERE k < 4;
    DELETE FROM p WHERE k IN (4, 5, 11);"
print_deletes p 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT r, groupArray(k) FROM (SELECT r, k FROM icebergLocal('${LAKE}/p') ORDER BY k) GROUP BY r ORDER BY r"
${CLICKHOUSE_CLIENT} --query "SELECT r, groupArray(k) FROM (SELECT r, k FROM icebergLocal('${LAKE}/p') WHERE r = 'odd' ORDER BY k) GROUP BY r"

echo "-- position delete files of a table upgraded from format-version 2 are merged into the deletion vector"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE u (k Int32) ENGINE = IcebergLocal('${LAKE}/u') SETTINGS iceberg_format_version = 2;
    INSERT INTO u SELECT number FROM numbers(10);
    DELETE FROM u WHERE k < 2;
    DELETE FROM u WHERE k = 9;"
python3 - "$(latest_metadata "${LAKE}/u")" <<'PY'
import json, os, re, sys
path = sys.argv[1]
version = int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1))
with open(path) as f:
    metadata = json.load(f)
metadata["format-version"] = 3
metadata["next-row-id"] = 0
with open(os.path.join(os.path.dirname(path), f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)
PY
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "DELETE FROM u WHERE k = 5;"
print_deletes u 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(k) FROM (SELECT k FROM icebergLocal('${LAKE}/u') ORDER BY k)"

rm -rf "${LAKE}"
