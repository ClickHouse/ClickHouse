#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs failpoints, which are only available in a full build.
# no-parallel: the failpoints are server wide, so they would make inserts of concurrent tests fail.

# The failed rollbacks are logged, including the one while attaching the table, which is attached anyway.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The rollback of a failed insert into a Log family table truncates the files back, and the truncation
# can fail as well: for example, an encrypted disk rewrites the retained prefix of the file, which
# needs free space. The files then keep the bytes of the failed insert. These bytes must never
# become visible: neither by reading the table, nor after reloading it, nor by the next insert,
# which has to finish the rollback before it records the sizes of the files.
# `file_checker_repair_fail_truncating` makes the truncation in `FileChecker::repair` fail.

function show()
{
    # Rows in [500, 560) belong to the insert that is made to fail.
    $CLICKHOUSE_CLIENT -q "
        SELECT '$1: ' || toString(count()), sum(a), countIf(a >= 500 AND a < 560) FROM $tbl;
        SELECT 'arrays readable: ' || toString(count()) FROM (SELECT a, arr FROM $tbl ORDER BY a DESC LIMIT 10) WHERE length(arr) > 0"
}

for engine in Log TinyLog StripeLog; do
    echo "--- $engine"
    tbl="t_${engine}"

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $tbl;
        CREATE TABLE $tbl (a UInt64, arr Array(Array(Int64))) ENGINE = $engine;
        INSERT INTO $tbl SELECT number, [[number, number + 1], [number + 2]] FROM numbers(100);
        SYSTEM ENABLE FAILPOINT file_checker_update_and_save_fail_persisting;
        SYSTEM ENABLE FAILPOINT file_checker_repair_fail_truncating"

    $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(500, 60)" 2>&1 \
        | grep -q -F "FAULT_INJECTED" && echo "failed insert was rejected: 1" || echo "failed insert was rejected: 0"

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT file_checker_update_and_save_fail_persisting"
    show "after the failed rollback"

    # The rollback still cannot be finished, so the insert is refused instead of adopting the leftovers.
    $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(7000, 10)" 2>&1 \
        | grep -q -F "FAULT_INJECTED" && echo "insert without a rollback was rejected: 1" || echo "insert without a rollback was rejected: 0"
    show "after the rejected insert"

    # The rollback fails while the table is being attached as well.
    $CLICKHOUSE_CLIENT -q "DETACH TABLE $tbl; ATTACH TABLE $tbl"
    show "after the reload"

    $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(7000, 10)" 2>&1 \
        | grep -q -F "FAULT_INJECTED" && echo "insert after the reload was rejected: 1" || echo "insert after the reload was rejected: 0"

    # Now the truncation works, and the next insert finishes the rollback first.
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT file_checker_repair_fail_truncating;
        INSERT INTO $tbl SELECT number, [[number]] FROM numbers(9000, 40)"
    show "after the retry"

    $CLICKHOUSE_CLIENT -q "
        CHECK TABLE $tbl SETTINGS check_query_single_value_result = 1;
        DETACH TABLE $tbl;
        ATTACH TABLE $tbl;
        CHECK TABLE $tbl SETTINGS check_query_single_value_result = 1"
    show "after the second reload"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $tbl"
done

$CLICKHOUSE_CLIENT -q "
    SYSTEM DISABLE FAILPOINT file_checker_update_and_save_fail_persisting;
    SYSTEM DISABLE FAILPOINT file_checker_repair_fail_truncating"
