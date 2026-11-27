#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The server closes the connection of the client in the middle of a turn of the AI agent, and the
# client connects again. The turn stops instead of going on with the new connection.
python3 "$CUR_DIR"/05895_client_ai_turn_stops_on_reconnect.python
