#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "SELECT 1" -- --max_prefixes_deserialization_thread_pool_size=0 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
