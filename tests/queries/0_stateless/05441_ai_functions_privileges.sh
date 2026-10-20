#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="${CLICKHOUSE_DATABASE}_user_05441"
definer="${CLICKHOUSE_DATABASE}_definer_05441"
nc="${CLICKHOUSE_DATABASE}_nc_05441"
udf="${CLICKHOUSE_DATABASE}_udf_05441"

$CLICKHOUSE_CLIENT -q "
DROP NAMED COLLECTION IF EXISTS $nc;
DROP USER IF EXISTS $user, $definer;
DROP FUNCTION IF EXISTS $udf;

CREATE NAMED COLLECTION $nc AS
    provider = 'openai',
    endpoint = 'http://localhost:1/v1/chat/completions',
    model = 'test-model',
    api_key = 'fake-key';

CREATE USER $user, $definer IDENTIFIED WITH no_password;
GRANT NAMED COLLECTION ON $nc TO $user, $definer;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;
GRANT AI FUNCTIONS ON *.* TO $definer;

CREATE VIEW ${CLICKHOUSE_DATABASE}.v_definer DEFINER = $definer SQL SECURITY DEFINER
    AS SELECT aiGenerate(x, map('credentials', '$nc')) FROM (SELECT '' AS x WHERE 0);
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker SQL SECURITY INVOKER
    AS SELECT aiGenerate(x, map('credentials', '$nc')) FROM (SELECT '' AS x WHERE 0);
CREATE FUNCTION $udf AS x -> aiGenerate(x, map('credentials', '$nc'));
"

# All queries read zero rows, so no outbound request is made.
function check()
{
    $CLICKHOUSE_CLIENT --user "$user" -q "$1" 2>&1 | grep -q ACCESS_DENIED && echo "$2: ACCESS_DENIED" || echo "$2: OK"
}

function check_all()
{
    echo "--- $1"
    check "SELECT aiGenerate(x, map('credentials', '$nc')) FROM (SELECT '' AS x WHERE 0)" aiGenerate
    check "SELECT aiClassify(x, ['a', 'b'], map('credentials', '$nc')) FROM (SELECT '' AS x WHERE 0)" aiClassify
    check "SELECT aiEmbed(x, 'test-model', map('credentials', '$nc')) FROM (SELECT '' AS x WHERE 0)" aiEmbed
    check "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer" v_definer
    check "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker" v_invoker
    check "SELECT $udf(x) FROM (SELECT '' AS x WHERE 0)" udf
}

check_all "no grant"

$CLICKHOUSE_CLIENT -q "GRANT aiGenerate ON *.* TO $user"
check_all "GRANT aiGenerate"

$CLICKHOUSE_CLIENT -q "GRANT AI FUNCTIONS ON *.* TO $user"
check_all "GRANT AI FUNCTIONS"

$CLICKHOUSE_CLIENT -q "REVOKE aiClassify ON *.* FROM $user"
check_all "REVOKE aiClassify"

$CLICKHOUSE_CLIENT -q "
DROP FUNCTION IF EXISTS $udf;
DROP USER IF EXISTS $user, $definer;
DROP NAMED COLLECTION IF EXISTS $nc;
"
