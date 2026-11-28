#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A filtered LowCardinality column keeps its whole dictionary. COLUMNAR_V1 sends the dictionary
# to the guest, so it must be compacted to the rows the guest was given: otherwise a guest can read
# a value from a row that a row policy or WHERE removed. lc_dict_rows_col returns the dictionary
# row count it received; the compacted dictionary holds the default '' and 'public' (2 rows), the
# uncompacted one also holds 'secret' (3 rows). hex_lc_col returns the row's value, 'public'.

MODULE="columnar_wide_abi_${CLICKHOUSE_DATABASE}"
FN_ROWS="wasm_lc_dict_rows_${CLICKHOUSE_DATABASE}"
FN_HEX="wasm_lc_hex_${CLICKHOUSE_DATABASE}"
TEST_USER="user_${CLICKHOUSE_DATABASE}"
TEST_POLICY="policy_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/columnar_wide_abi.wasm"

${CLICKHOUSE_CLIENT} << EOF
CREATE OR REPLACE FUNCTION ${FN_ROWS}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'lc_dict_rows_col'
    ARGUMENTS (s LowCardinality(String)) RETURNS UInt64 DETERMINISTIC;
CREATE OR REPLACE FUNCTION ${FN_HEX}
    LANGUAGE WASM ABI COLUMNAR_V1 FROM '${MODULE}' :: 'hex_lc_col'
    ARGUMENTS (s LowCardinality(String)) RETURNS String DETERMINISTIC;

DROP TABLE IF EXISTS lc_policy;
CREATE TABLE lc_policy (allowed UInt8, s LowCardinality(String)) ENGINE = Memory;
-- One INSERT, one block: both values share one dictionary.
INSERT INTO lc_policy VALUES (1, 'public'), (0, 'secret');
-- Before the row policy exists: a policy for another user would deny the default user.
SELECT 'where', ${FN_ROWS}(s), ${FN_HEX}(s) FROM lc_policy WHERE allowed = 1;

DROP USER IF EXISTS ${TEST_USER};
CREATE USER ${TEST_USER} IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.lc_policy TO ${TEST_USER};
DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.lc_policy;
CREATE ROW POLICY ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.lc_policy FOR SELECT USING allowed = 1 TO ${TEST_USER};

EOF

${CLICKHOUSE_CLIENT} --user "${TEST_USER}" --query "SELECT 'row policy', ${FN_ROWS}(s), ${FN_HEX}(s) FROM lc_policy"

${CLICKHOUSE_CLIENT} << EOF
DROP ROW POLICY ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.lc_policy;
DROP USER ${TEST_USER};
DROP TABLE lc_policy;
DROP FUNCTION ${FN_ROWS};
DROP FUNCTION ${FN_HEX};
EOF

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
