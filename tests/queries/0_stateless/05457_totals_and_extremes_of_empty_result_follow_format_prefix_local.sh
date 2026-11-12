#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The queries of 05457_totals_and_extremes_of_empty_result_follow_format_prefix.sql, through clickhouse-local.

$CLICKHOUSE_LOCAL --query "
SET output_format_write_statistics = 0;

SELECT number % 2 AS k, count() AS c FROM numbers(3) GROUP BY k WITH TOTALS HAVING k > 10 FORMAT JSONCompact;
SELECT number % 2 AS k, count() AS c FROM numbers(3) GROUP BY k WITH TOTALS HAVING k > 10 FORMAT JSONColumnsWithMetadata;
SELECT number % 2 AS k, count() AS c FROM numbers(3) GROUP BY k WITH TOTALS HAVING k > 10 FORMAT TSVWithNames;

SELECT number FROM numbers(3) SETTINGS extremes = 1, offset = 10 FORMAT JSONCompact;
SELECT number FROM numbers(3) SETTINGS extremes = 1, offset = 10 FORMAT JSONColumnsWithMetadata;
SELECT number FROM numbers(3) SETTINGS extremes = 1, offset = 10 FORMAT TSVWithNames;
"
