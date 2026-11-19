#!/usr/bin/env bash
# Tags: no-fasttest, no-llvm-coverage
# no-fasttest: waits 7 s for the update backoff; no-llvm-coverage: the file size limit below would also cut the coverage profile.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ssd_cache` dictionaries whose cache file cannot grow for the next partition stay usable after the failed update,
# and the next update fails the same way. The file size limit lets the first 64 KiB of each cache file be allocated,
# but not the next 64 KiB, while 100 values of 1.2 KB need more than that.

dir="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
rm -rf "${dir:?}"
mkdir -p "$dir"
trap 'rm -rf "${dir:?}"' EXIT

query="
CREATE TABLE src (k UInt64, v String) ENGINE = Memory;
INSERT INTO src SELECT number, toString(number) || ':' || repeat('x', 1200) FROM numbers(100);

CREATE DICTIONARY d_simple (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(MIN 1000 MAX 1000)
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 4 PATH '$dir/d_simple'));

CREATE DICTIONARY d_complex (k UInt64, v String DEFAULT '') PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(MIN 1000 MAX 1000)
LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 65536 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 4 PATH '$dir/d_complex'));

-- Both updates fail, because the cache files cannot grow.
SELECT dictGet('d_simple', 'v', number) FROM numbers(100) FORMAT Null;
SELECT dictGet('d_complex', 'v', tuple(number)) FROM numbers(100) FORMAT Null;

-- Wait for the 5-6 s backoff after a failed update, so that the next lookups update the dictionaries again.
SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT sleep(1) FORMAT Null;

SELECT dictGet('d_simple', 'v', number) FROM numbers(100) FORMAT Null;
SELECT dictGet('d_complex', 'v', tuple(number)) FROM numbers(100) FORMAT Null;

SELECT name, status FROM system.dictionaries ORDER BY name;
SELECT dictGet('d_simple', 'v', toUInt64(0)) = '0:' || repeat('x', 1200), dictGet('d_complex', 'v', tuple(toUInt64(0))) = '0:' || repeat('x', 1200);
SELECT 'alive';
"

(
    cd "$dir" || exit 1
    ulimit -c 0
    trap '' XFSZ
    ulimit -f 100
    $CLICKHOUSE_LOCAL --ignore-error --query "$query" 2>"$dir/stderr"
)

# Each of the four failed updates reports the failed allocation, and none of them was refused by the backoff.
grep -o -F 'Cannot preallocate space' "$dir/stderr" | wc -l
grep -o -F 'nearest update is scheduled' "$dir/stderr" | wc -l
