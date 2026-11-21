#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The schema-mismatch explanation of an INSERT parse error is optional. A failure while it is
# prepared (the failpoint stands in for exceeding the memory limit while the captured data is
# copied) must not replace the original parse error: the explanation is just omitted.
# The failpoint is enabled in a separate `clickhouse-local` process, so it does not affect other tests.

PHRASE="does not match the structure expected by the query"
QUERY='CREATE TABLE t (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\tpage_view\t/users/profile\n'

report() {
    local output
    output=$(cat)
    if echo "$output" | grep -q "CANNOT_PARSE"; then echo "original error present"; else echo "original error missing"; fi
    if echo "$output" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
    if echo "$output" | grep -q -e "MEMORY_LIMIT_EXCEEDED" -e "Failpoint"; then echo "diagnostic failure leaked"; fi
}

echo "-- without the failpoint"
printf "$QUERY" | $CLICKHOUSE_LOCAL 2>&1 | report

echo "-- with a failure while preparing the explanation"
printf "SYSTEM ENABLE FAILPOINT insert_schema_mismatch_diagnostic_fail; $QUERY" | $CLICKHOUSE_LOCAL 2>&1 | report
