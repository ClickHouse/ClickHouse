#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-fasttest: Iceberg is not in fast test
# no-random-settings: the memory limit depends on the settings

# A `Parquet` data file of an Iceberg table read under its own older schema gets a field added later from the schema
# transform of the source: its `initial-default` is materialized for every chunk that the reader returns. With the
# columns cache, the reader counts these columns too when it decides whether to read a whole chunk of 65536 rows, so the
# query succeeds with the cache as it does without it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

TABLE="${TMP_DIR}/lake"

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --max_insert_threads=1 -q "
    CREATE TABLE lake (id Int64) ENGINE = IcebergLocal('${TABLE}/', 'Parquet');
    INSERT INTO lake SELECT number FROM numbers(65536)"

# A required string field with an `initial-default` of 1 KiB, added the way another engine adds it, because ClickHouse
# cannot add a field with a default.
python3 - "${TABLE}" <<'PY'
import copy, json, os, re, sys

metadata_dir = os.path.join(sys.argv[1], "metadata")
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = copy.deepcopy(metadata["schemas"][0])
field_id = metadata["last-column-id"] + 1
default = "x" * 1024
schema["fields"].append({"id": field_id, "name": "missing", "required": True, "type": "string",
                         "initial-default": default, "write-default": default})
schema["schema-id"] = 1
metadata["schemas"].append(schema)
metadata["current-schema-id"] = 1
metadata["last-column-id"] = field_id
metadata["last-updated-ms"] += 1
with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY

# Only a file that has not been modified for a few seconds is cached.
find "${TABLE}/data" -type f -exec touch -d "1 minute ago" {} +

# The default alone takes 64 MiB for a chunk of 65536 rows.
QUERY="SELECT count(), sum(id), sum(length(missing)) FROM icebergLocal('${TABLE}/')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    max_memory_usage = '48Mi', max_untracked_memory = 65536"
${CLICKHOUSE_LOCAL} -q "${QUERY}, use_columns_cache = 0"
${CLICKHOUSE_LOCAL} -q "${QUERY}, use_columns_cache = 1"
