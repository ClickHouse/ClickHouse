#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-local` does not listen on any port, so an address without a port that points to the
# local host is the `clickhouse-local` process itself. `port_offset` must not change it: the table
# exists only inside this `clickhouse-local`, so reading it proves that no connection was made.

${CLICKHOUSE_LOCAL} --multiquery -q "
    CREATE TABLE only_in_local (x UInt8) ENGINE = Memory;
    INSERT INTO only_in_local VALUES (42);
    SELECT 'localhost', * FROM remote('localhost', default.only_in_local);
    SELECT '127.0.0.1', * FROM remote('127.0.0.1', default.only_in_local);
" -- --port_offset=100
