#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The message of the `TOO_DEEP_RECURSION` exception thrown by `checkStackSize` is sent to the client,
# so it must not contain the addresses of the stack and the frame of the server.
# Two `Merge` tables reading from each other recurse until the stack check fires.

output=$(${CLICKHOUSE_LOCAL} --query "
CREATE TABLE merge1 (x UInt64) ENGINE = Merge(currentDatabase(), '^merge\\\\d\$');
CREATE TABLE merge2 (x UInt64) ENGINE = Merge(currentDatabase(), '^merge\\\\d\$');
SELECT * FROM merge1;
" 2>&1)

# `grep -c` exits with code 1 when the count is zero, so do not let it fail the test.
grep -c -F 'Stack size too large' <<< "$output" ||:
grep -c -F 'address' <<< "$output" ||:
