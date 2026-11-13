#!/usr/bin/env bash
# Tags: linux, no-asan, no-tsan, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local starts with a descriptor table of min(4096, soft RLIMIT_NOFILE) entries (not in sanitizer builds).
(ulimit -Sn 1024 && ${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(extract(line, '[0-9]+')) >= 1024 FROM file('/proc/self/status', LineAsString) WHERE startsWith(line, 'FDSize')")
(ulimit -Sn 16384 && ${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(extract(line, '[0-9]+')) BETWEEN 4096 AND 16383 FROM file('/proc/self/status', LineAsString) WHERE startsWith(line, 'FDSize')")
