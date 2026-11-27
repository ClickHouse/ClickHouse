#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - <<EOF
import importlib.util
from importlib.machinery import SourceFileLoader

INTERFACE_TCP_INTERSERVER = 7

loader = SourceFileLoader("raw", "$CUR_DIR/05061_query_log_interface_enum_unknown.python")
raw = importlib.util.module_from_spec(importlib.util.spec_from_loader("raw", loader))
loader.exec_module(raw)
raw.runQuery("forged interface", "SELECT 1 SETTINGS log_comment = 'forged_interserver_interface'", INTERFACE_TCP_INTERSERVER, 0)
EOF

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT interface FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = 'forged_interserver_interface' AND type = 'QueryStart';
"
