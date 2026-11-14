#!/usr/bin/env bash

# `leader_election` is rejected for a table with a `UNIQUE KEY`: the loading of such a table can repair a
# dense index without holding the lease. The check runs before the parts are loaded, so it fires before
# the storage policy is validated, even on the default local disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_unique_key 1 -q "
    CREATE TABLE t_leader_election_unique_key (id UInt64, v String)
    ENGINE = MergeTree
    UNIQUE KEY (id)
    ORDER BY id
    SETTINGS leader_election = 1
" 2>&1 | grep -o 'The `leader_election` setting cannot be used with a table that has a `UNIQUE KEY`' | head -n 1

$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_leader_election_unique_key"
