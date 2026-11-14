#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT that names `input` but never reads it fails only after the query is over:
# clickhouse-local must report the error, not abort.
output=$(echo 1 | ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION null('x UInt64') WITH unused AS (SELECT * FROM input('x UInt64')) SELECT 1 FORMAT CSV" 2>&1)
rc=$?
echo "$output" | grep -o -m1 'UNEXPECTED_PACKET_FROM_SERVER' | head -n1
[ "$rc" -lt 128 ] && echo "no abort" || echo "aborted with exit code $rc"
