#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows

# `remove_orphan_files` on local storage: it deletes the unreferenced files older than `older_than`,
# keeps every file a snapshot references, honours `dry_run` and `location`, and tallies each file
# under its category. The same scenarios on S3 are in 05703_iceberg_remove_orphan_files_s3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./iceberg_orphan_files.lib
. "$CUR_DIR"/iceberg_orphan_files.lib

check_remove_orphan_files local

echo "--- default older_than comes from iceberg_orphan_files_older_than_seconds"
populate local default_older_than 1
add_orphan data/orphan-default.parquet
sleep 2
remove_orphans "" "--iceberg_orphan_files_older_than_seconds=1"
metric deleted_data_files_count
exists data/orphan-default.parquet
data

echo "--- location rejects escapes, accepts a leading ./"
populate local location_validation 1
for bad in ../escape /absolute/path; do
    $CH -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '$($CH -q "SELECT toString(now())")', location = '${bad}')" 2>&1 \
        | grep -o -m1 BAD_ARGUMENTS | head -1
done
add_orphan data/orphan-dotslash.parquet
remove_orphans "older_than = '$(now_ts)', location = './data/'"
metric deleted_data_files_count

echo "--- format version 1 is rejected"
FORMAT_VERSION=1 populate local v1 1
out=$($CH -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files()" 2>&1)
grep -o -m1 BAD_ARGUMENTS <<<"${out}" | head -1
grep -o -i -m1 "format version" <<<"${out}" | head -1
$CH -q "DROP TABLE ${TABLE}"
