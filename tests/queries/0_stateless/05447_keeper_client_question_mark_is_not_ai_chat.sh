#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `keeper-client` shares the line reader of `clickhouse-client`, whose AI-chat mode takes a leading
# `?` away from the line. `keeper-client` has no AI chat, so it must get the `?` and reject the
# line, instead of running `? rm <path>` as `rm <path>`.
path="/test-keeper-client-question-mark-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/victim' 'x'"

python3 "$CUR_DIR"/05447_keeper_client_question_mark_is_not_ai_chat.python "$path/victim"

$CLICKHOUSE_KEEPER_CLIENT -q "exists '$path/victim'"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null
