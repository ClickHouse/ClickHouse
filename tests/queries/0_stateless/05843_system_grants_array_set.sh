#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The grantless role never reaches the final `IN` filter, which casts role names to array keys.
${CLICKHOUSE_LOCAL} --multiquery <<'SQL'
CREATE ROLE `[[1]]`, grants_filter_empty;
GRANT SELECT ON system.one TO `[[1]]`;

-- Skip conversion for NULL role names from user grants.
SELECT role_name FROM system.grants
WHERE role_name IN (SELECT [[1]])
SETTINGS transform_null_in = 0, rewrite_in_to_join = 0,
    short_circuit_function_evaluation_for_nulls = 1,
    short_circuit_function_evaluation_for_nulls_threshold = 0;
SQL
