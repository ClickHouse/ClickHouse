#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet and Iceberg support is not built in fast-test images

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. The size of a chunk must
# not be underestimated for the columns that are formed without a column of the file that bounds them. Each query below
# succeeds with the cache as it does without it:
# - a field of an Iceberg table that is missing in the data file is filled with its `initial-default`, which may be much
#   larger than the default value of the type. The block size that `input_format_parquet_prefer_block_bytes` asks for
#   counts it as well;
# - a variant read as a `Variant` type takes as much as its requested members, e.g. a `FixedString(256)` for a string of
#   one byte, so its chunk is of unknown size, also when the variant is inside a tuple. The read scheduler counts the
#   widest member as well, so that the blocks are not all decoded ahead of a slow consumer.
# Each case runs in one `clickhouse-local`, which has a columns cache of its own, empty at the start.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

function local_query()
{
    ${CLICKHOUSE_LOCAL} "${@:2}" -q "$1" -- --user_files_path="${WORK_DIR}"
}

# The entries of the files read by table functions belong to no table, so their database is empty.
ENTRIES="SELECT column, sum(rows) FROM system.columns_cache WHERE database = '' GROUP BY column ORDER BY column"
# The blocks that the settings ask for are much smaller than a chunk, and a whole chunk does not fit the memory limit.
BLOCKS="max_threads = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_max_block_size = 1000,
    input_format_parquet_prefer_block_bytes = 0, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_untracked_memory = 65536"
# The settings ask for whole chunks, without a memory limit of the query.
CHUNKS="max_threads = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_max_block_size = 65536,
    input_format_parquet_prefer_block_bytes = 0"

# $1: the query, $2: the memory limit. Runs the query in blocks without the cache and with it, then in whole chunks,
# which fills the cache, then in blocks with the chunks served from the cache, and shows what the cache holds.
function run_query()
{
    local_query "
        $1 SETTINGS ${BLOCKS}, max_memory_usage = '$2', use_columns_cache = 0;
        $1 SETTINGS ${BLOCKS}, max_memory_usage = '$2', use_columns_cache = 1;
        SELECT 'cached', count() FROM system.columns_cache WHERE database = '';
        $1 SETTINGS ${CHUNKS}, use_columns_cache = 1;
        ${ENTRIES};
        $1 SETTINGS ${BLOCKS}, max_memory_usage = '$2', use_columns_cache = 1"
}

echo "--- a field of Iceberg with a long initial-default, missing in the file"
# 65536 rows, then a required field is added the way another engine adds it, because ClickHouse cannot add a field with
# a default: its `initial-default` is a string of 1 KiB, which takes 64 MiB in the rows of the file.
TABLE="${WORK_DIR}/lake"
local_query "
    CREATE TABLE lake (id Int64) ENGINE = IcebergLocal('${TABLE}/', 'Parquet');
    INSERT INTO lake SELECT number FROM numbers(65536)" --allow_insert_into_iceberg=1 --max_insert_threads=1
python3 - "${TABLE}" <<'EOF'
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
# The snapshot is bound to the schema with the field, as after its manifest is rewritten under it: then the data file is
# read under this schema, and the field is filled by the reader of the file.
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = 1
metadata["last-column-id"] = field_id
metadata["last-updated-ms"] += 1
with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
EOF
run_query "SELECT count(), sum(id), sum(length(missing)) FROM icebergLocal('${TABLE}/')" 48Mi

echo "--- its size is counted in the block size in bytes"
local_query "
    SELECT max(blockSize()) < 2000, count(), any(length(missing)) FROM icebergLocal('${TABLE}/') WHERE id >= 0
    SETTINGS max_threads = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_prefer_block_bytes = 1000000,
        use_columns_cache = 0"

# 65536 rows of an unshredded variant, recognized by its structure: a string of one byte, `x`. Also inside a tuple.
FILE="${WORK_DIR}/variant.parquet"
local_query "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'v Tuple(metadata String, value String), t Tuple(v Tuple(metadata String, value String))')
    SELECT (unhex('010000'), unhex('0578')) AS v, tuple(v) FROM numbers(65536)
    SETTINGS output_format_parquet_string_as_string = 0, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE}"

echo "--- a variant read as Variant with a member much wider than the value"
run_query "SELECT count(), sum(length(variantElement(v, 'FixedString(256)')))
    FROM file('${FILE}', Parquet, 'v Variant(FixedString(256), UInt8)')" 16Mi

echo "--- the same variant inside a tuple"
run_query "SELECT count(), sum(length(variantElement(t.v, 'FixedString(256)')))
    FROM file('${FILE}', Parquet, 't Tuple(v Variant(FixedString(256), UInt8))')" 16Mi

echo "--- the blocks of the variant are not decoded ahead of a slow consumer"
# The read scheduler counts the widest member of the `Variant` in the size of a block. Otherwise it takes the blocks for
# small and decodes all of them, 16 MiB, while the query processes the first ones. The delay is per block.
local_query "
    SELECT count(), sum(length(variantElement(v, 'FixedString(256)')))
    FROM file('${FILE}', Parquet, 'v Variant(FixedString(256), UInt8)') WHERE NOT ignore(sleep(0.005))
    SETTINGS ${BLOCKS}, max_memory_usage = '16Mi', use_columns_cache = 0;
    SELECT count(), sum(length(variantElement(t.v, 'FixedString(256)')))
    FROM file('${FILE}', Parquet, 't Tuple(v Variant(FixedString(256), UInt8))') WHERE NOT ignore(sleep(0.005))
    SETTINGS ${BLOCKS}, max_memory_usage = '16Mi', use_columns_cache = 0"
