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
