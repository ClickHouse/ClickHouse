#!/usr/bin/env bash

set -e -o pipefail

run_lance_local_test()
{
    local test_name="$1"
    local source_dir
    source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
    local fixture_dir="${CLICKHOUSE_USER_FILES_UNIQUE}/data_lance"

    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
    mkdir -p "${fixture_dir}"
    cp -R "${source_dir}"/*.lance "${fixture_dir}/"
    trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"' EXIT

    local sql_file="${source_dir}/sql/${test_name}.sql"
    sed "s|tests/queries/0_stateless/data_lance|${fixture_dir}|g" "${sql_file}" \
        | ${CLICKHOUSE_CLIENT} --multiquery

    case "${test_name}" in
        04545_lance_local_schema_validation)
            ${CLICKHOUSE_CLIENT} --query "
                SELECT throwIf(count() != 0, 'Mismatched Lance schema was accepted')
                FROM system.tables
                WHERE database = currentDatabase() AND name = 'lance_local_mismatch'
                FORMAT Null"
            ;;
        04546_lance_local_unsupported_type)
            ${CLICKHOUSE_CLIENT} --query "
                SELECT throwIf(count() != 0, 'Unsupported Lance schema was accepted')
                FROM system.tables
                WHERE database = currentDatabase() AND name = 'lance_local_unsupported_type'
                FORMAT Null"
            ;;
    esac
}
