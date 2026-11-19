#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification:
#   no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `predictXGBoost` needs `dictGet` on the model dictionary. It must be checked before the definition is read and before
# the dictionary is loaded, because loading trains the model on the source, and a missing dictionary must look the same.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --enable_xgboost 1 -m --query "
    DROP USER IF EXISTS ${username};
    CREATE TABLE training (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO training SELECT number AS x1, number * 2 AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);
    CREATE DICTIONARY model (x1 Float64, x2 Float64, y Float64)
    PRIMARY KEY (x1, x2)
    SOURCE(CLICKHOUSE(TABLE 'training' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(XGBOOST(num_iterations 10))
    LIFETIME(0);
    CREATE USER ${username} NOT IDENTIFIED;
"

function query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"
}

function status()
{
    query "SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model'"
}

function as_user()
{
    echo "$1" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
    local output
    output=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${username}&enable_xgboost=1" --data-binary "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}"
    fi
}

query "SYSTEM UNLOAD DICTIONARY model"

echo "--- no grants"
as_user "SELECT predictXGBoost('model', 1.0, 2.0)"
status
as_user "SELECT predictXGBoost('${CLICKHOUSE_DATABASE}.model', 1.0, 2.0)"
status
as_user "SELECT predictXGBoost('model', toFloat64(number), toFloat64(number)) FROM numbers(0)"
status
as_user "SELECT predictXGBoost('no_such_model', 1.0, 2.0)"

echo "--- dictGet"
query "GRANT dictGet ON ${CLICKHOUSE_DATABASE}.* TO ${username}"
as_user "SELECT isFinite(predictXGBoost('model', 1.0, 2.0))"
status
as_user "SELECT predictXGBoost('no_such_model', 1.0, 2.0)" | grep -o -E "^SELECT.*|not found" | head -2

${CLICKHOUSE_CLIENT} --query "DROP USER ${username}"
