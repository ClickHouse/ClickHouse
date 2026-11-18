#!/usr/bin/env bash
# Arrow output writes the whole dictionary of a `LowCardinality` column. The dictionary of a `MergeTree` part
# holds the values of every row of the part, also of the rows that a row policy hides from the reader, so the
# writers must export only the values that the output rows reference.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_USER="${CLICKHOUSE_DATABASE}_user"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS policy_keys;
CREATE TABLE policy_keys (tenant UInt8, k LowCardinality(String), v UInt64)
ENGINE = MergeTree ORDER BY tenant SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO policy_keys VALUES (1, 'allowed_key', 1), (2, 'policy_secret_for_other_tenant', 2);
DROP USER IF EXISTS ${TEST_USER};
CREATE USER ${TEST_USER} IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.policy_keys TO ${TEST_USER};
DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON policy_keys;
CREATE ROW POLICY ${TEST_POLICY} ON policy_keys USING tenant = 1 TO ${TEST_USER};
"

SETTINGS="max_threads=1, optimize_aggregation_in_order=0, use_columns_cache=0, output_format_arrow_low_cardinality_as_dictionary=1, output_format_arrow_compression_method='none'"

for query in \
    "SELECT k FROM policy_keys" \
    "SELECT k, sum(v) FROM policy_keys GROUP BY k" \
    "SELECT [k] AS a FROM policy_keys"
do
    for format in Arrow ArrowStream
    do
        output=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${TEST_USER}" --data-binary "${query} SETTINGS ${SETTINGS} FORMAT ${format}" | tr -d '\0')
        echo "${format}: ${query}: allowed $(grep -a -c allowed_key <<< "${output}"), hidden $(grep -a -c policy_secret_for_other_tenant <<< "${output}")"
    done
done

$CLICKHOUSE_CLIENT -m --query "
DROP ROW POLICY ${TEST_POLICY} ON policy_keys;
DROP USER ${TEST_USER};
DROP TABLE policy_keys;
"
