#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads invalid golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md),
# which must be rejected with an exception.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOLDEN="$CUR_DIR/data_delta_lake/kernel_golden"

TABLES=(
    # The protocol has reader features, but its minimum reader version is not 3.
    deltalog-invalid-protocol-version
    # The log or the checkpoint has no metadata or no protocol.
    deltalog-state-reconstruction-from-checkpoint-missing-metadata
    deltalog-state-reconstruction-from-checkpoint-missing-protocol
    deltalog-state-reconstruction-without-metadata
    deltalog-state-reconstruction-without-protocol
    # Data files without `_delta_log`.
    no-delta-log-folder
    # A commit file is missing in the middle of the log.
    versions-not-contiguous
    # The log adds and then removes a file referred to by an absolute path outside of the table,
    # spelled differently in the add and remove actions (e.g. `file:/some/path` and `/some/path`).
    # The table is supposed to be empty, but the kernel does not canonicalize the paths and
    # returns the file (delta-kernel-rs skips these tables for this reason).
    # ClickHouse must refuse to read a file outside of the table directory.
    canonicalized-paths-normal-a
    canonicalized-paths-normal-b
    canonicalized-paths-special-a
    canonicalized-paths-special-b
)

# All tables are read in one `clickhouse-local` process, because starting it is slow in sanitizer builds.
# The name of each table is printed before its query, and the exception of the query follows it.
# For each table, print the error code and, for errors from the Delta Lake kernel, the kind of the error.
QUERIES=""
for table in "${TABLES[@]}"
do
    QUERIES+="SELECT '@@ $table' FORMAT TSVRaw; SELECT count() FROM deltaLakeLocal('$GOLDEN/$table/delta') FORMAT Null;"
done

$CLICKHOUSE_LOCAL --ignore-error -q "$QUERIES" 2>&1 | awk '
    function flush() { if (table != "") print table, kind, code }
    /^@@ / { flush(); table = substr($0, 4); kind = ""; code = ""; next }
    kind == "" && match($0, /kernel error [A-Za-z]+/) { kind = substr($0, RSTART, RLENGTH) }
    code == "" && match($0, /\([A-Z_]+\)$/) { code = substr($0, RSTART, RLENGTH) }
    END { flush() }
'
