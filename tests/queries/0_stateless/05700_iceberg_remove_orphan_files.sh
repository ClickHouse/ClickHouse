#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergLocal / IcebergS3 (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows

# `remove_orphan_files` on local and S3 storage: it deletes the unreferenced files older than `older_than`,
# keeps every file a snapshot references, honours `dry_run` and `location`, and tallies each file
# under its category.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./iceberg_orphan_files.lib
. "$CUR_DIR"/iceberg_orphan_files.lib

for storage in local s3; do
    echo "=== ${storage}"

    echo "--- basic: orphans are deleted, committed data is kept"
    populate "${storage}" basic 3
    add_orphan data/orphan-data-001.parquet
    add_orphan data/orphan-data-002.parquet
    exists data/orphan-data-001.parquet
    exists data/orphan-data-002.parquet
    remove_orphans "older_than = '$(now_ts)'"
    echo "metrics: $(wc -l <<<"${OUT}")"
    metric deleted_data_files_count
    exists data/orphan-data-001.parquet
    exists data/orphan-data-002.parquet
    data

    echo "--- no orphans: nothing but superseded metadata files is deleted"
    populate "${storage}" no_orphans 2
    remove_orphans "older_than = '$(now_ts)'"
    awk -F'\t' '$1 != "deleted_metadata_files_count" && $1 != "skipped_missing_metadata_count" && $2 != 0' <<<"${OUT}"
    data

    echo "--- older_than spares newer orphans"
    populate "${storage}" older_than 1
    past=$(now_ts)
    sleep 1.1
    add_orphan data/orphan-new.parquet
    remove_orphans "older_than = '${past}'"
    exists data/orphan-new.parquet
    remove_orphans "older_than = '$(now_ts)'"
    metric deleted_data_files_count
    exists data/orphan-new.parquet
    data

    echo "--- dry_run reports without deleting"
    populate "${storage}" dry_run 1
    add_orphan data/orphan-dry.parquet
    ts=$(now_ts)
    remove_orphans "older_than = '${ts}', dry_run = 1"
    metric deleted_data_files_count
    exists data/orphan-dry.parquet
    remove_orphans "older_than = '${ts}', dry_run = 0"
    metric deleted_data_files_count
    exists data/orphan-dry.parquet

    # The scan covers only the given subdirectory, nested directories included.
    echo "--- location restricts the scan"
    populate "${storage}" location 1
    add_orphan data/orphan-data.parquet
    add_orphan data/subdir/orphan-nested.parquet
    add_orphan metadata/v0.metadata.json
    ts=$(now_ts)
    remove_orphans "older_than = '${ts}', location = 'data/'"
    metric deleted_data_files_count
    exists data/orphan-data.parquet
    exists metadata/v0.metadata.json
    remove_orphans "older_than = '${ts}', location = 'metadata/'"
    metric deleted_metadata_files_count
    exists metadata/v0.metadata.json
    data

    echo "--- many orphans in one run"
    populate "${storage}" many 1
    for i in $(seq -f '%03g' 0 9); do add_orphan "data/orphan-par-$i.parquet"; done
    remove_orphans "older_than = '$(now_ts)'"
    metric deleted_data_files_count
    list_files | grep -c 'orphan-par-'
    data

    echo "--- files referenced by any snapshot are kept, positional older_than"
    populate "${storage}" positional 3
    add_orphan data/orphan-pos.parquet
    remove_orphans "'$(now_ts)'"
    metric deleted_data_files_count
    exists data/orphan-pos.parquet
    data

    echo "--- delete files are tallied under their own category"
    populate "${storage}" delete_categories 1
    add_orphan data/eq-delete-001.parquet
    add_orphan data/00000-0-eq-del-00001.parquet
    add_orphan data/00000-0-deletes.parquet
    add_orphan data/00000-0-delete-00001.parquet
    remove_orphans "older_than = '$(now_ts)'"
    metric deleted_equality_delete_files_count
    metric deleted_position_delete_files_count

    # The hint is a fixed object, not a path stored in metadata; deleting it stops the table reading.
    echo "--- version-hint.text is reachable"
    populate "${storage}" version_hint 2 ", iceberg_use_version_hint = true"
    exists metadata/version-hint.text
    add_orphan data/orphan-version-hint.parquet
    remove_orphans "older_than = '$(now_ts)'"
    exists metadata/version-hint.text
    data
    exists data/orphan-version-hint.parquet
done

echo "=== local only"

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
