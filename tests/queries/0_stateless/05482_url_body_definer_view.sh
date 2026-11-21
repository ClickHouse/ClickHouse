#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `SQL SECURITY DEFINER` view in the query of `body(...)` of the `url` table function is read with the definer's
# rights, also when the analyzer inlines the view. The user can read the view, but not the table behind it.
# The request is sent to the HTTP interface of the server itself, which executes the body as a query.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery "
DROP USER IF EXISTS ${user};
CREATE TABLE source (q String) ENGINE = Memory;
INSERT INTO source VALUES ('SELECT 42 FORMAT TSV');
CREATE VIEW definer_view DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT q FROM source;
CREATE USER ${user};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.definer_view TO ${user};
GRANT READ ON URL TO ${user};
GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};
"

for inline_views in 0 1
do
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "
        SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT q FROM ${CLICKHOUSE_DATABASE}.definer_view FORMAT TSVRaw))
        SETTINGS enable_analyzer = 1, analyzer_inline_views = ${inline_views}"
done

# The table behind the view is still not readable directly.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT q FROM ${CLICKHOUSE_DATABASE}.source FORMAT TSVRaw))
    SETTINGS enable_analyzer = 1" 2>&1 | grep -o -m1 'ACCESS_DENIED'

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
