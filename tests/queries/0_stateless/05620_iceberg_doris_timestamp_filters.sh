#!/usr/bin/env bash
# Tags: no-fasttest

# Filters on timestamps with different sub-second precision in Iceberg tables from the Apache Doris tests,
# see `data_iceberg/doris/README.md`.
# The expected results are taken from `regression-test/data/external_table_p0/iceberg/test_iceberg_filter.out`
# and `test_iceberg_predicate_conversion.out`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` in `clickhouse-local` only reads paths inside the current directory.
cd "$CUR_DIR/data_iceberg/doris" || exit 1

# `ts` is a `timestamp` (without time zone, `TIMESTAMP_NTZ` in Spark), one row per commit (seven data files):
#     insert into tb_ts_filter values (1, timestamp '2024-05-30 20:34:56');
#     insert into tb_ts_filter values (2, timestamp '2024-05-30 20:34:56.1');
#     ...
#     insert into tb_ts_filter values (7, timestamp '2024-05-30 20:34:56.123456');
# The values are compared with literals of different precision, so the min/max statistics of the data files
# are used for pruning. A `timestamp` without time zone is read as `DateTime64(6)` in the session time zone,
# so the time zone is set to UTC.
echo "tb_ts_filter"
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.1' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.10' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.100' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.123' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.1230' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.123400' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.123456' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts < '2024-05-30 20:34:56.12' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts > '2024-05-30 20:34:56.12' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts < '2024-05-30 20:34:56.1200' ORDER BY id;
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts > '2024-05-30 20:34:56.1200' ORDER BY id;
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts < '2024-05-30 20:34:56';
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts > '2024-05-30 20:34:56.1234';
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts > '2024-05-30 20:34:56.0';
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts < '2024-05-30 20:34:56.123456';
    SELECT count() FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts > '2024-05-30 20:34:56.123456';
"

# One row `2024-06-11 12:34:56.123456` in a `timestamp` column, the table has nine metadata versions.
echo "tb_ts_ntz_filter"
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts = '2024-06-11 12:34:56.123456';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts > '2024-06-11 12:34:56.123456';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts < '2024-06-11 12:34:56.123456';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts > '2024-06-11 12:34:56.12345';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts < '2024-06-11 12:34:56.123466';
"

# The same in the `Asia/Shanghai` session time zone: a `timestamp` without time zone is a wall-clock value,
# so the values and the filter results must not depend on the session time zone.
# Currently the values are shifted by the session time zone offset (`20:34:56.123456`) and the filters
# return nothing (https://github.com/ClickHouse/ClickHouse/issues/130025).
echo "tb_ts_ntz_filter Asia/Shanghai"
$CLICKHOUSE_LOCAL --session_timezone Asia/Shanghai -q "
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter');
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts = '2024-06-11 12:34:56.123456';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts > '2024-06-11 12:34:56.123456';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts < '2024-06-11 12:34:56.123456';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts > '2024-06-11 12:34:56.12345';
    SELECT * FROM icebergLocal('warehouse/wh/test_db/tb_ts_ntz_filter') WHERE ts < '2024-06-11 12:34:56.123466';
    SELECT * FROM icebergLocal('warehouse/wh/multi_catalog/tb_ts_filter') WHERE ts = '2024-05-30 20:34:56.1' ORDER BY id;
"

# Predicates on string, `timestamptz` and date columns, eight rows in eight data files:
#     insert into tb_predict values ('2023-03-08', timestamp '2023-03-07 20:35:59.123456', date '1997-05-19', 'MAIL');
#     ...
# The timestamps were written by Spark in the `Asia/Shanghai` time zone.
echo "tb_predict"
$CLICKHOUSE_LOCAL -q "
    SELECT glue_varchar FROM icebergLocal('warehouse/wh/multi_catalog/tb_predict') WHERE glue_varchar > '2023-03-07' ORDER BY ALL;
    SELECT l_shipdate FROM icebergLocal('warehouse/wh/multi_catalog/tb_predict') WHERE l_shipdate IN ('1997-05-18', '1996-05-06') ORDER BY ALL;
    SELECT l_shipdate, l_shipmode FROM icebergLocal('warehouse/wh/multi_catalog/tb_predict') WHERE l_shipdate IN ('1997-05-18', '1996-05-06') AND l_shipmode = 'MAIL' ORDER BY ALL;
    SELECT l_shipdate, l_shipmode FROM icebergLocal('warehouse/wh/multi_catalog/tb_predict') WHERE l_shipdate IN ('1997-05-18', '1996-05-06') OR NOT (l_shipmode = 'MAIL') ORDER BY l_shipdate, l_shipmode LIMIT 10;
    SELECT toTimeZone(glue_timstamp, 'Asia/Shanghai') FROM icebergLocal('warehouse/wh/multi_catalog/tb_predict')
    WHERE glue_timstamp > toDateTime64('2023-03-07 20:35:59', 6, 'Asia/Shanghai') ORDER BY glue_timstamp LIMIT 5;
"
