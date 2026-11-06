#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Change Data Feed (`delta_lake_snapshot_start_version` / `delta_lake_snapshot_end_version`) over the
# tables of delta-kernel-rs `kernel/tests/data` (see `data_delta_lake/kernel_data/README.md`).
# The expected rows are those of `kernel/tests/integration/features/cdf.rs`. The tables cover
# inserts, updates (`update_preimage` / `update_postimage`, and the legacy `update_pre` /
# `update_post` names), deletes by deletion vectors and by CDC files, `dataChange = false` commits,
# partitioned tables, column names with dots and backticks, and column mapping in `name` and `id`
# modes. `_commit_timestamp` is not printed: it is the file modification time of the commits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data"

# cdf <table> <columns> <start version> [<end version>]
QUERIES=""
cdf() {
    local table=$1 columns=$2 start=$3 end=${4:-}
    local settings="delta_lake_snapshot_start_version = ${start}"
    [[ -n "${end}" ]] && settings+=", delta_lake_snapshot_end_version = ${end}"
    QUERIES+="
SELECT '--- ${table} [${start}, ${end:-latest}]';
SELECT ${columns}, _change_type, _commit_version FROM deltaLakeLocal('${DATA}/${table}')
ORDER BY _commit_version, _change_type, ${columns} SETTINGS ${settings};"
}

cdf cdf-table 'id, name, birthday' 0
cdf cdf-table-non-partitioned 'id, name, birthday, long_field, boolean_field, double_field, smallint_field' 0
cdf cdf-table-with-dv 'value' 0
cdf cdf-table-with-cdc-and-dvs 'id, comment' 0
cdf cdf-table-simple 'id' 0 0
cdf cdf-table-simple 'id' 1 1
cdf cdf-table-simple 'id' 2 2
cdf cdf-table-simple 'id' 0 2
cdf cdf-table-update-ops 'id' 0 2
cdf cdf-table-data-change 'id' 0 1
cdf cdf-table-partitioned 'id, text, part' 0 2
# shellcheck disable=SC2016 # Backticks quote the SQL identifiers.
cdf cdf-table-backtick-column-names '`id.num`, `id.num\`s`, struct_col' 0
cdf cdf-table-delete-unconditional 'id' 0
cdf cdf-table-delete-conditional-all-rows 'id' 0
cdf cdf-table-delete-conditional-two-rows 'id' 0
# These tables enable CDF only in version 1.
cdf cdf-column-mapping-name-mode 'id, name, value' 1
cdf cdf-column-mapping-name-mode-3-7 'id, name, value' 1
cdf cdf-column-mapping-id-mode 'id, name, value' 1

$CLICKHOUSE_LOCAL -q "
SELECT '--- cdf-table-with-cdc-and-dvs: empty comments';
SELECT id, _change_type, _commit_version FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs')
WHERE comment = '' ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 0;
SELECT '--- cdf-table: schema';
DESCRIBE deltaLakeLocal('${DATA}/cdf-table') SETTINGS delta_lake_snapshot_start_version = 0;
SELECT '--- cdf-table: filter on the CDF columns';
SELECT id, _change_type FROM deltaLakeLocal('${DATA}/cdf-table')
WHERE _commit_version = 2 AND _change_type = 'update_postimage' ORDER BY id
SETTINGS delta_lake_snapshot_start_version = 1;
${QUERIES}"

echo '--- invalid ranges'
# The end version is before the start version.
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table-simple')
SETTINGS delta_lake_snapshot_start_version = 1, delta_lake_snapshot_end_version = 0" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
# The start version is after the last version of the table.
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table-simple')
SETTINGS delta_lake_snapshot_start_version = 3, delta_lake_snapshot_end_version = 4" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR'
