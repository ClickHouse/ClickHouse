#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `File` table keeps the format settings, and so the zone, of its `CREATE`. A date/time string inferred
# into a `JSON` path must be checked against the `DateTime` range in the zone it is then parsed in: in UTC
# this value is past the `DateTime` range and needs `DateTime64`, while in Tokyo it would fit `DateTime`.
# Dynamic paths and shared data must agree.

FILE="${CLICKHOUSE_DATABASE}/05873_data.jsonl"

$CLICKHOUSE_CLIENT -q "
    SET session_timezone = 'UTC';
    INSERT INTO FUNCTION file('${FILE}', 'LineAsString', 's String')
        SELECT '{\"j\": {\"ts\": \"2106-02-07 07:00:00\", \"arr\": [\"2106-02-07 07:00:00\"]}}'
        SETTINGS engine_file_truncate_on_insert = 1;

    CREATE TABLE t_dynamic_paths (j JSON) ENGINE = File(JSONEachRow, '${FILE}')
        SETTINGS input_format_try_infer_datetimes = 1, input_format_try_infer_datetimes_only_datetime64 = 0;
    CREATE TABLE t_shared_data (j JSON(max_dynamic_paths = 0)) ENGINE = File(JSONEachRow, '${FILE}')
        SETTINGS input_format_try_infer_datetimes = 1, input_format_try_infer_datetimes_only_datetime64 = 0;

    SELECT 'dynamic paths', dynamicType(j.ts), toUInt64(j.ts), dynamicType(j.arr) FROM t_dynamic_paths
        SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT 'shared data', dynamicType(j.ts), toUInt64(j.ts), dynamicType(j.arr) FROM t_shared_data
        SETTINGS session_timezone = 'Asia/Tokyo';

    DROP TABLE t_dynamic_paths;
    DROP TABLE t_shared_data;
"
