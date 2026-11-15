#!/bin/bash
set -e

WORK_TREE="$1"
(
  cd $WORK_TREE || exit 1

  echo "$PWD"

  cat << EOF > /tmp/query.sql
WITH
    1600000000 AS start_uts,
    1600000450 AS end_uts,
    8 AS step_sec
SELECT *
FROM
(
    WITH
        120 AS vector_range_sec,
        0 AS offset,
        toUInt32(start_uts - offset) AS start_uts_with_offset, -- This start_uts_with_offset is a UInt32, different from the inner WITH clause
        toUInt32(end_uts - offset) AS end_uts_with_offset -- This end_uts_with_offset is a UInt32, different from the inner WITH clause
    SELECT *
    FROM
    (
        WITH
            toUInt32(ceiling((start_uts_with_offset - vector_range_sec) / step_sec) * step_sec) AS start_uts,
            toUInt32(end_uts_with_offset) AS end_uts,
            15 AS step_sec
        SELECT *
        FROM
        (
            WITH
                180 AS vector_range_sec,
                0 AS offset,
                toDateTime(start_uts, 'UTC') - toIntervalSecond(offset) AS start_uts_with_offset, -- This start_uts_with_offset is a DateTime, different from the outer WITH clause
                toDateTime(end_uts, 'UTC') - toIntervalSecond(offset) AS end_uts_with_offset -- This end_uts_with_offset is a DateTime, different from the outer WITH clause
            SELECT
                start_uts,
                end_uts
        )
    )
)
EOF

  # CH_PATH is the path to downloaded binary
  $CH_PATH client --queries-file /tmp/query.sql
)
