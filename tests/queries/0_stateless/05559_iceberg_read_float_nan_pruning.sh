#!/usr/bin/env bash
# Tags: no-fasttest
# Fails until https://github.com/ClickHouse/ClickHouse/pull/126078 is merged: Iceberg float bounds
# exclude NaN, and without that PR min/max pruning drops a file whose only matching row is NaN.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
ROOT="${USER_FILES_PATH}/${PREFIX}"
CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"
TABLES=()
QUERIES=0

cleanup()
{
    for table in "${TABLES[@]}"; do
        ${CLIENT} --query "DROP TABLE IF EXISTS ${PREFIX}_${table}"
    done
    rm -rf "${ROOT}"
}
trap cleanup EXIT

# Reads go through the `icebergLocal` table function: an `IcebergLocal` table keeps the format
# settings of its CREATE, so Parquet pushdown could not be turned off per query. Parquet row-group
# pruning drops NaN rows on its own (https://github.com/ClickHouse/ClickHouse/issues/118110), so it is
# turned off to see what Iceberg pruning alone does; the query condition cache is off too, as it would
# reuse a wrong answer.
READ_SETTINGS="input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_page_filter_push_down = 0, use_query_condition_cache = 0"

insert() { ${CLIENT} --query "INSERT INTO ${PREFIX}_$1 VALUES $2"; }

# new_table <name> <type> <values> [<header note>]: creates `f <type>`, inserts the values, prints the header.
new_table()
{
    local name="$1" type="$2" values="$3"
    echo "--- ${name}: ${type}, values=${values}${4:+, $4}"
    TABLES+=("${name}")
    ${CLIENT} --query "CREATE TABLE ${PREFIX}_${name} (f ${type}) ENGINE = IcebergLocal('${ROOT}/${name}/', 'Parquet')"
    insert "${name}" "${values}"
}

# check <name> <condition>: prints the matching rows and how many files min/max pruning skipped.
check()
{
    local query_id="${PREFIX}_$((++QUERIES))" rows pruned
    rows=$(${CLIENT} --query_id="${query_id}" --query "
        SELECT count() FROM icebergLocal('${ROOT}/$1/') WHERE $2 SETTINGS ${READ_SETTINGS}")
    ${CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    pruned=$(${CLIENT} --query "
        SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
            AND query_id = '${query_id}'")
    echo "WHERE $2 -> rows=${rows} pruned_files=${pruned}"
}

# latest_manifests <name>: the manifest files of the current snapshot.
latest_manifests()
{
    local manifest_list
    manifest_list=$(ls -t "${ROOT}/$1/metadata"/snap-*.avro | head -1)
    ${CLIENT} --query "SELECT manifest_path FROM file('${manifest_list}', Avro)" \
        | xargs -n1 basename | sed "s|^|${ROOT}/$1/metadata/|"
}

# Case A/B: a file holding NaN is not pruned for predicates that NaN satisfies. A positive comparison,
# which NaN never satisfies, still prunes.
for args in "A Float32" "B Float64"; do
    read -r name type <<< "${args}"
    new_table "${name}" "${type}" "(1.5), (nan)"
    for condition in "f != 1.5" "NOT (f <= 1.5)" "NOT (f < 2)" "f > 2"; do
        check "${name}" "${condition}"
    done
done

# Case C: a file without NaN (`nan_value_counts` = 0) is still pruned for the same predicates.
new_table C Float64 "(1.5), (1.5)"
for condition in "f != 1.5" "NOT (f <= 1.5)" "NOT (f < 2)"; do
    check C "${condition}"
done

# Case D: `OPTIMIZE TABLE ... MANIFEST` copies the statistics instead of recomputing them, so it must
# carry `nan_value_counts` over, or the NaN file loses its count and is pruned again.
new_table D Float64 "(1.5), (nan)" "then (2.5), (3.5), then OPTIMIZE TABLE ... MANIFEST"
insert D "(2.5), (3.5)"
# The cloud build has no manifest compaction, so there case D only checks the counts written by INSERT.
if [[ $(${CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'") != 1 ]]; then
    ${CLIENT} --allow_experimental_iceberg_compaction=1 --query "
        OPTIMIZE TABLE ${PREFIX}_D MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1"
    if [[ $(latest_manifests D | wc -l) != 1 ]]; then
        echo "OPTIMIZE did not merge the manifests"
    fi
fi
for manifest in $(latest_manifests D); do
    ${CLIENT} --query "SELECT 'nan_value_counts=' || toString(tupleElement(data_file, 'nan_value_counts')) FROM file('${manifest}', Avro)"
done | sort
check D "f != 1.5"

# Case E: `Float32` -> `Float64` evolution: the file written before the ALTER has 4-byte `Float32`
# bounds and a NaN, the one after has 8-byte `Float64` bounds; both are decoded and pruned correctly.
new_table E Float32 "(1.5), (nan)" "then MODIFY COLUMN f Float64, then (10.5), (20.5)"
${CLIENT} --query "ALTER TABLE ${PREFIX}_E MODIFY COLUMN f Float64"
insert E "(10.5), (20.5)"
for condition in "f != 1.5" "f < 2" "f > 15"; do
    check E "${condition}"
done
