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

# Prints the error code and, for errors from the Delta Lake kernel, the kind of the error.
function expect_error()
{
    local table="$1"
    local output
    output=$($CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$GOLDEN/$table/delta')" 2>&1)
    echo "$table" \
        "$(echo "$output" | grep -o -m1 -E 'kernel error [A-Za-z]+')" \
        "$(echo "$output" | grep -o -m1 -E '\([A-Z_]+\)$')"
}

# The protocol has reader features, but its minimum reader version is not 3.
expect_error deltalog-invalid-protocol-version
# The log or the checkpoint has no metadata or no protocol.
expect_error deltalog-state-reconstruction-from-checkpoint-missing-metadata
expect_error deltalog-state-reconstruction-from-checkpoint-missing-protocol
expect_error deltalog-state-reconstruction-without-metadata
expect_error deltalog-state-reconstruction-without-protocol
# Data files without `_delta_log`.
expect_error no-delta-log-folder
# A commit file is missing in the middle of the log.
expect_error versions-not-contiguous

# The log adds and then removes a file referred to by an absolute path outside of the table,
# spelled differently in the add and remove actions (e.g. `file:/some/path` and `/some/path`).
# The table is supposed to be empty, but the kernel does not canonicalize the paths and
# returns the file (delta-kernel-rs skips these tables for this reason).
# ClickHouse must refuse to read a file outside of the table directory.
expect_error canonicalized-paths-normal-a
expect_error canonicalized-paths-normal-b
expect_error canonicalized-paths-special-a
expect_error canonicalized-paths-special-b
