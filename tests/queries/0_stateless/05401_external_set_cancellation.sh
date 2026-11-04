#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-cancellation.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# The failpoint makes the first check of the time limit by the builder of a set on disk find the limit
# reached, as in the `break` overflow mode, so the builder throws `QUERY_WAS_CANCELLED`. Each case runs one
# query in a fresh process, with a set of 3 million keys that switches to disk while it is built. It prints
# the error, whether the set switched to disk, the temporary files that the set created, and the files that
# it completed with their runs, which show where the query stopped.
run()
{
    local name="$1"
    local threshold="$2"
    local log="${LOCAL_DIR}/${name}.log"
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_bytes_before_external_set "${threshold}" \
        --max_bytes_ratio_before_external_set 0 --send_logs_level trace --ignore-error --multiquery \
        > "${LOCAL_DIR}/${name}.out" 2> "${log}" <<'SQL'
SYSTEM ENABLE FAILPOINT disk_set_builder_reach_time_limit;
SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(3000000));
SELECT sum(value) FROM system.events WHERE event = 'ExternalSetWritePart';
SQL

    local error switched completed
    error=$( (grep -m1 'Code: [0-9]*\. DB::Exception' "${log}" || true) | grep -o '([A-Z_]*)' | head -1 | tr -d '()')
    switched=$(grep -c 'Switching the set of IN to external mode while it is built' "${log}" || true)
    completed=$(grep -c 'Wrote .* runs with .* keys of a set of IN to temporary file' "${log}" || true)
    printf '%s\t%s\tswitched %d\tfiles %s\tcompleted files %d\n' \
        "${name}" "${error}" "${switched}" "$(cat "${LOCAL_DIR}/${name}.out")" "${completed}"
}

# With a threshold of 8 MiB, the set switches to disk with about 200,000 keys in memory, fewer than a run of
# 8 MiB holds. The conversion adds these keys in columns, and the check after the first column stops the query
# before a run is complete.
run conversion 8388608

# With a threshold of 1 byte, the set switches to disk before its first keys, and each column of keys is a
# run, which the sorter writes to a temporary file at once. The check after the first batch of the write stops
# the query before the file is complete.
run write 1
