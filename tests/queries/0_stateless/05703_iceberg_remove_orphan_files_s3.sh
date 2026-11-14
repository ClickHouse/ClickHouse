#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergS3 (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows

# `remove_orphan_files` on S3 storage: it deletes the unreferenced files older than `older_than`,
# keeps every file a snapshot references, honours `dry_run` and `location`, and tallies each file
# under its category. 05700_iceberg_remove_orphan_files runs them on local storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./iceberg_orphan_files.lib
. "$CUR_DIR"/iceberg_orphan_files.lib

check_remove_orphan_files s3
