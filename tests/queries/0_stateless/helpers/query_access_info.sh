# Shared SQL rendering and access-denial helpers for the split query-access-info tests.

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

qai_user_identifier()
{
    python3 - "$1" <<'PY_QUOTE'
import sys
print("`" + sys.argv[1].replace("`", "``") + "`", end="")
PY_QUOTE
}

run_sql_input()
{
    render_sql_input "$1" | $CLICKHOUSE_CLIENT --multiquery
}

expect_access_denied()
{
    local output
    if output=$( $CLICKHOUSE_CLIENT --query "$1" 2>&1 ); then
        echo "Expected ACCESS_DENIED, but query succeeded" >&2
        return 1
    fi
    if [[ "$output" != *ACCESS_DENIED* && "$output" != *"Code: 497"* ]]; then
        echo "$output" >&2
        echo "Expected ACCESS_DENIED from query" >&2
        return 1
    fi
}


qai_cleanup_users()
{
    local user_suffix user_id
    for user_suffix in "$@"; do
        user_id=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_${user_suffix}")
        $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_id}" >/dev/null 2>&1 || true
    done
}
