#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Every scalar Iceberg type in a format version 3 table written by the Apache Iceberg 1.11.0 Java API, partitioned by
# identity(ts), with two partition-scoped equality delete files (keyed on the uuid of id 2 and on the decimal of id 7).
# Every value is a function of the row id: see the table in `data_iceberg/tileverse/README.md`.
# The tables come from the parquetry test kit, see `data_iceberg/tileverse/README.md`.
# The metadata is rooted at `file:///iceberg-<corpus>/<table>`, which is resolved relative to the table directory;
# the queries run with `data_iceberg/tileverse` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/tileverse" || exit 1


$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT id, ts, tstz, ts_ns, tstz_ns, t, dec, bigdec, u, hex(fx), bin, d, label FROM icebergLocal('iceberg-scalar-types/scalars') ORDER BY id;

-- Predicates on the partition column and on columns with manifest bounds of each encoding.
SELECT 'filters';
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE ts = '2024-06-15 12:30:45.123456' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE ts >= '2025-01-01' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE dec < 0 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE bigdec > 1000000000000000 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE u = '00000000-0000-0005-0000-0000004c4b4f');
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE u = '00000000-0000-0002-0000-0000001e8486');
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE d BETWEEN '2024-01-05' AND '2024-01-10' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE tstz_ns > '2024-01-01 00:00:00.000000010' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE fx = unhex('0306090C'));
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('iceberg-scalar-types/scalars') WHERE label IN ('row-2', 'row-7', 'row-8') ORDER BY id);
"
