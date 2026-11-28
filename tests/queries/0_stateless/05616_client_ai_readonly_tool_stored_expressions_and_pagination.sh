#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In batch mode a `?` line is an error, not a silently skipped question.
$CLICKHOUSE_LOCAL --query '? hello' >/dev/null 2>&1
echo "batch_question_exit_code_is_nonzero: $(( $? != 0 ))"

python3 "$CUR_DIR"/05616_client_ai_readonly_tool_stored_expressions_and_pagination.python
