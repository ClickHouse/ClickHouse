#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# The column statistics an INSERT writes into the manifest prune data files by min/max. Four
# INSERTs write one data file each; a filter matching one file's values prunes the other three.
# With `use_iceberg_partition_pruning = 0` nothing is pruned and the result is the same.
# Both format versions, local, S3 and Azure storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
trap 'rm -rf "${LAKE}"' EXIT

PARQUET_OFF="input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_filter_push_down = 0"

FILTERS=(
    ""
    "WHERE date <= '2024-01-25'"
    "WHERE ts <= timestamp('2024-03-20 14:00:00.000000')"
    "WHERE tag == 1"
    "WHERE tag <= 1"
    "WHERE name == 'vasilisa'"
    "WHERE name < 'kek'"
    "WHERE number == 8"
    "WHERE number <= 5"
)
for n in "${!FILTERS[@]}"; do echo "filter ${n}: ${FILTERS[n]:-none}"; done

# Runs every filter with pruning off and on, tagged with the log comment
# <table>-<filter>-<use_iceberg_partition_pruning>, and checks both return the same rows.
run()
{
    local table="$1" engine="$2" format_version="$3" n pruning queries=""
    for n in "${!FILTERS[@]}"; do
        for pruning in 0 1; do
            queries+="SELECT groupArray(tuple(*)) FROM (SELECT * FROM ${table} ${FILTERS[n]} ORDER BY ALL)
                SETTINGS use_iceberg_partition_pruning = ${pruning}, ${PARQUET_OFF}, log_comment = '${table}-${n}-${pruning}';"
        done
    done

    local results
    mapfile -t results < <(${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
        DROP TABLE IF EXISTS ${table};
        CREATE TABLE ${table} (tag Int32, date Date32, ts DateTime, name String, number Int64)
            ENGINE = ${engine} SETTINGS iceberg_format_version = ${format_version};
        INSERT INTO ${table} VALUES (1, '2024-01-20', '2024-02-20 10:00:00', 'vasya', 5);
        INSERT INTO ${table} VALUES (2, '2024-02-20', '2024-03-20 15:00:00', 'vasilisa', 6);
        INSERT INTO ${table} VALUES (3, '2025-03-20', '2024-04-30 14:00:00', 'icebreaker', 7);
        INSERT INTO ${table} VALUES (4, '2025-04-20', '2024-05-30 14:00:00', 'iceberg', 8);
        ${queries}
        DROP TABLE ${table};")

    for n in "${!FILTERS[@]}"; do
        [ "${results[2 * n]}" == "${results[2 * n + 1]}" ] \
            || echo "results differ for ${table} '${FILTERS[n]}': '${results[2 * n]}' vs '${results[2 * n + 1]}'"
    done
}

AZURE_URL='http://localhost:10000/devstoreaccount1'
AZURE_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='

for storage in local s3 azure; do
    for fv in 1 2; do
        table="t_${storage}_v${fv}"
        case "${storage}" in
            local) engine="IcebergLocal('${LAKE}/${table}/')" ;;
            s3) engine="IcebergS3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}/${table}/')" ;;
            azure) engine="IcebergAzure('${AZURE_URL}', 'cont', '${CLICKHOUSE_TEST_UNIQUE_NAME}/${table}/', 'devstoreaccount1', '${AZURE_KEY}')" ;;
        esac
        run "${table}" "${engine}" "${fv}"
    done
done

# Files pruned per table and filter, with pruning off and on.
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT id[1] AS table, toUInt32(id[2]) AS filter,
        sumIf(pruned, id[3] = '0') AS pruned_off, sumIf(pruned, id[3] = '1') AS pruned_on
    FROM
    (
        SELECT splitByChar('-', log_comment) AS id, ProfileEvents['IcebergMinMaxIndexPrunedFiles'] AS pruned
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, 't_')
    )
    GROUP BY table, filter
    ORDER BY table, filter"
