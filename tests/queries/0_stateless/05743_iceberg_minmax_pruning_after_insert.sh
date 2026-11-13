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

# Runs every filter with pruning off and on under the query id <database>-<table>-<filter>-<off|on>, and
# checks both return the same rows.
run()
{
    local table="$1" engine="$2" format_version="$3" n query off on
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
        DROP TABLE IF EXISTS ${table};
        CREATE TABLE ${table} (tag Int32, date Date32, ts DateTime, name String, number Int64)
            ENGINE = ${engine} SETTINGS iceberg_format_version = ${format_version};
        INSERT INTO ${table} VALUES (1, '2024-01-20', '2024-02-20 10:00:00', 'vasya', 5);
        INSERT INTO ${table} VALUES (2, '2024-02-20', '2024-03-20 15:00:00', 'vasilisa', 6);
        INSERT INTO ${table} VALUES (3, '2025-03-20', '2024-04-30 14:00:00', 'icebreaker', 7);
        INSERT INTO ${table} VALUES (4, '2025-04-20', '2024-05-30 14:00:00', 'iceberg', 8);"

    for n in "${!FILTERS[@]}"; do
        query="SELECT * FROM ${table} ${FILTERS[n]} ORDER BY ALL"
        off=$(${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}-${table}-${n}-off" --query "${query} SETTINGS use_iceberg_partition_pruning = 0, ${PARQUET_OFF}")
        on=$(${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}-${table}-${n}-on" --query "${query} SETTINGS use_iceberg_partition_pruning = 1, ${PARQUET_OFF}")
        [ "${off}" == "${on}" ] || echo "results differ for ${table} '${FILTERS[n]}': '${off}' vs '${on}'"
    done

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
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
    SELECT id[2] AS table, toUInt32(id[3]) AS filter,
        sumIf(pruned, id[4] = 'off') AS pruned_off, sumIf(pruned, id[4] = 'on') AS pruned_on
    FROM
    (
        SELECT splitByChar('-', query_id) AS id, ProfileEvents['IcebergMinMaxIndexPrunedFiles'] AS pruned
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(query_id, '${CLICKHOUSE_DATABASE}-')
    )
    GROUP BY table, filter
    ORDER BY table, filter"
