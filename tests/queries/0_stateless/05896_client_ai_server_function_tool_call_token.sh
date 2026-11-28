#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With the server-side `aiGenerate` function as the model backend, only a block with the token of
# the conversation in its tags is a tool call; a bare `<tool_call>` block is text.
python3 "$CUR_DIR"/05896_client_ai_server_function_tool_call_token.python
