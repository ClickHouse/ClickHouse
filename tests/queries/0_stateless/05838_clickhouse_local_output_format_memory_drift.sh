#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The output format is allocated inside the query, so it must be released inside it, not settled as drift afterwards.
${CLICKHOUSE_LOCAL} --max_threads 4 --output_format_parallel_formatting 1 --query "
    SELECT number, toString(number), repeat('x', 50) FROM numbers(3000000) FORMAT TSV;
    SELECT if(value < 8388608, 'OK', 'settled ' || toString(value) || ' bytes')
    FROM (SELECT sum(value) AS value FROM system.events WHERE event = 'QueryMemoryDriftSettledBytes');
" | tail -n 1

# The same for the output of the logs, which queries without a result also create (about 1 MiB each).
${CLICKHOUSE_LOCAL} --send_logs_level trace --query "
    $(for i in $(seq 1 10); do echo "CREATE TABLE t_$i (x UInt8) ENGINE = Memory;"; done)
    SELECT if(value < 4194304, 'OK', 'settled ' || toString(value) || ' bytes')
    FROM (SELECT sum(value) AS value FROM system.events WHERE event = 'QueryMemoryDriftSettledBytes');
" 2>/dev/null | tail -n 1

# The same for the pager, which keeps buffers of its own until the output is released.
${CLICKHOUSE_LOCAL} --pager cat --query "
    SELECT 1; SELECT 1; SELECT 1;
    SELECT if(value < 4194304, 'OK', 'settled ' || toString(value) || ' bytes')
    FROM (SELECT sum(value) AS value FROM system.events WHERE event = 'QueryMemoryDriftSettledBytes');
" | tail -n 1
