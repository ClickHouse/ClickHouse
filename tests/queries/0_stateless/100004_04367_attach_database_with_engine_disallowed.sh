#!/usr/bin/env bash
# ATTACH DATABASE/TABLE with a full definition must not create new objects in Cloud mode.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_attach_04367"

# Prepare a detached database and table before enabling cloud_mode.
# (CREATE DATABASE/TABLE ENGINE=Memory are blocked in cloud_mode.)
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB}"
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE attach_tbl_04367 (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE attach_tbl_04367"

# DATABASE: explicit engine on a non-existing database is rejected.
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH DATABASE ${DB}_nonexistent ENGINE = Log"    2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH DATABASE ${DB}_nonexistent ENGINE = Memory" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH DATABASE ${DB}_nonexistent ENGINE = Atomic" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"

# Short syntax on a non-existing database: different error (existing behaviour unchanged).
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH DATABASE ${DB}_nonexistent" 2>&1 | grep -om1 "UNKNOWN_DATABASE_ENGINE"

# TABLE: full definition is rejected (inline and FROM-path variant).
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH TABLE attach_tbl_nonexistent_04367 (x UInt32) ENGINE = Log"                        2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH TABLE attach_tbl_nonexistent_04367 (x UInt32) ENGINE = Memory"                     2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH TABLE attach_tbl_nonexistent_04367 FROM '/nonexistent' (x UInt32) ENGINE = Log"    2>&1 | grep -om1 "SUPPORT_IS_DISABLED"

# Short-syntax reattach of previously detached objects still works under cloud_mode.
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH DATABASE ${DB}"
${CLICKHOUSE_CLIENT} --query "SET cloud_mode=1; ATTACH TABLE attach_tbl_04367"

# Disable cloud_mode before cleanup: DROP DATABASE in cloud_mode requires ON CLUSTER
# which is not available in the stateless-test environment.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE attach_tbl_04367"
