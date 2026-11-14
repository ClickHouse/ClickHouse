#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs failpoints, which are only available in a full build.
# no-parallel: the failpoints are server wide, so they would make inserts of concurrent tests fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The rollback of a failed insert into a Log family table must not be interrupted by the memory
# limit: the files must be truncated back and the metadata held in memory must be restored, so the
# table stays consistent, the next insert lands, and reloading the table does not change its data.
# `file_checker_repair_memory_limit_exceeded` behaves like an allocation in `FileChecker::repair`
# that hits the memory limit.

for engine in Log TinyLog StripeLog; do
    echo "--- $engine"
    tbl="t_${engine}"

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $tbl;
        DROP TABLE IF EXISTS ${tbl}_before;
        CREATE TABLE $tbl (a UInt64, arr Array(Array(Int64))) ENGINE = $engine;
        INSERT INTO $tbl SELECT number, [[number, number + 1], [number + 2]] FROM numbers(100);
        SYSTEM ENABLE FAILPOINT file_checker_update_and_save_fail_persisting;
        SYSTEM ENABLE FAILPOINT file_checker_repair_memory_limit_exceeded"

    # Rows in [500, 560) belong to the insert that is made to fail, so they must never appear.
    $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(500, 60)" 2>&1 \
        | grep -q -F "FAULT_INJECTED" && echo "failed insert was rejected: 1" || echo "failed insert was rejected: 0"

    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT file_checker_update_and_save_fail_persisting;
        SYSTEM DISABLE FAILPOINT file_checker_repair_memory_limit_exceeded"

    $CLICKHOUSE_CLIENT -q "CHECK TABLE $tbl SETTINGS check_query_single_value_result = 1"

    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $tbl SELECT number, [[number]] FROM numbers(9000, 40);
        SELECT 'rows of the failed insert: ' || toString(count()) FROM $tbl WHERE a >= 500 AND a < 560;
        SELECT 'rows: ' || toString(count()), sum(a) FROM $tbl;
        SELECT 'arrays readable: ' || toString(count()) FROM (SELECT a, arr FROM $tbl ORDER BY a DESC LIMIT 10) WHERE length(arr) > 0;
        CREATE TABLE ${tbl}_before ENGINE = Log AS SELECT count() AS c, sum(a) AS s FROM $tbl;
        DETACH TABLE $tbl;
        ATTACH TABLE $tbl;
        CHECK TABLE $tbl SETTINGS check_query_single_value_result = 1;
        SELECT 'reload keeps the same data: ' || toString(toUInt8(
            (SELECT c FROM ${tbl}_before) = (SELECT count() FROM $tbl)
            AND (SELECT s FROM ${tbl}_before) = (SELECT sum(a) FROM $tbl)));
        SELECT 'rows of the failed insert after reload: ' || toString(count()) FROM $tbl WHERE a >= 500 AND a < 560;
        DROP TABLE $tbl;
        DROP TABLE ${tbl}_before"
done
