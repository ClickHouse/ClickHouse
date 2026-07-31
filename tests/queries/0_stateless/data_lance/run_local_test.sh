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
    if grep -q '{ *serverError ' "${sql_file}"
    then
        local error_file="${CLICKHOUSE_TMP}/lance_${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"
        sed "s|tests/queries/0_stateless/data_lance|${fixture_dir}|g" "${sql_file}" \
            | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error 2>"${error_file}"

        diff -u \
            <(grep -oE 'serverError [A-Z_]+' "${sql_file}" | awk '{print $2}' | sort) \
            <(grep -oE '\([A-Z][A-Z_]+\)' "${error_file}" | tr -d '()' | sort)
        rm -f "${error_file}"

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
    else
        sed "s|tests/queries/0_stateless/data_lance|${fixture_dir}|g" "${sql_file}" \
            | ${CLICKHOUSE_CLIENT} --multiquery
    fi
}
