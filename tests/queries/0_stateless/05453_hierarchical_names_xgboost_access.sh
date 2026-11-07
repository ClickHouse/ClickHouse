#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `predictXGBoost` checks the `dictGet` access (see 05267_xgboost_access) for the dictionary it loads. A dictionary name
# is hierarchical (see 05077_hierarchical_names): in a session with the current database `db`, `ns.model` is the dictionary `db`.`ns.model` when
# there is no database `ns`, so a grant on the phantom database `ns` does not allow using it.

user="${CLICKHOUSE_DATABASE}_xgb_user_$RANDOM$RANDOM"

$CLICKHOUSE_CLIENT --enable_xgboost=1 -m -q "
DROP USER IF EXISTS ${user};

CREATE TABLE xgb_src (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO xgb_src SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY \`ns.model\` (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'xgb_src' DB currentDatabase()))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

CREATE TABLE flat_src (id UInt64, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO flat_src VALUES (1, 1.0);

CREATE DICTIONARY \`ns.flat\` (id UInt64, value Float64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'flat_src' DB currentDatabase()))
LAYOUT(FLAT())
LIFETIME(0);

CREATE USER ${user};
GRANT dictGet ON ns.* TO ${user};
"

function denied()
{
    $CLICKHOUSE_CLIENT --user "${user}" --enable_xgboost=1 -m -q "$1" 2>&1 \
        | grep -o -m1 -E "ACCESS_DENIED|BAD_ARGUMENTS|UNKNOWN_[A-Z_]+"
}

# The owner can predict with a namespace-relative name.
$CLICKHOUSE_CLIENT --enable_xgboost=1 -m -q "SELECT isFinite(predictXGBoost('ns.model', 1.0, 2.0))"

echo "With a grant on the phantom database ns only"
denied "SELECT predictXGBoost('ns.model', 1.0, 2.0)"
# A dictionary with another layout: denied, not reported as having the wrong layout.
denied "SELECT predictXGBoost('ns.flat', 1.0)"

echo "With the grant on the dictionary"
$CLICKHOUSE_CLIENT -q "GRANT dictGet ON ${CLICKHOUSE_DATABASE}.\`ns.model\` TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" --enable_xgboost=1 -m -q "SELECT isFinite(predictXGBoost('ns.model', 1.0, 2.0))"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
