#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QAI_VICTIM_USER="qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_victim"
export QAI_VICTIM_USER

qai_user_identifier()
{
    python3 - "$1" <<'PY_QUOTE'
import sys
print("`" + sys.argv[1].replace("`", "``") + "`", end="")
PY_QUOTE
}

render_sql_input()
{
    python3 - "$1" "$CLICKHOUSE_TEST_UNIQUE_NAME" <<'PY_RENDER'
from pathlib import Path
import re
import sys

text = Path(sys.argv[1]).read_text()
unique_name = sys.argv[2]
for old_user in sorted(set(re.findall(r"user_[0-9]{5}_[A-Za-z0-9_]+", text)), key=len, reverse=True):
    suffix = old_user.split("_", 2)[2]
    user_name = f"qai_{unique_name}_{suffix}"
    quoted_identifier = "`" + user_name.replace("`", "``") + "`"
    quoted_literal = "'" + user_name.replace("\\", "\\\\").replace("'", "\\'") + "'"
    text = text.replace("'" + old_user + "'", quoted_literal)
    text = text.replace(old_user, quoted_identifier)
print(text, end="")
PY_RENDER
}

cleanup()
{
    remote_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_remote")
    victim_user=$(qai_user_identifier "$QAI_VICTIM_USER")
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${remote_user}" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${victim_user}" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_05397_assertions" >/dev/null 2>&1 || true
}
trap cleanup EXIT

remote_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_remote")
victim_user=$(qai_user_identifier "$QAI_VICTIM_USER")
render_sql_input "$CUR_DIR"/05397_user_query_log_reader_filters.sql.in | $CLICKHOUSE_CLIENT --multiquery
$CLICKHOUSE_CLIENT --query "CREATE USER ${victim_user} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_05397_assertions (value UInt8) ENGINE = Null"
$CLICKHOUSE_CLIENT --query "GRANT INSERT ON t_05397_assertions TO ${victim_user}"
python3 "$CUR_DIR"/05397_user_query_log_reader_filters.python
