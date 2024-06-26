#!/bin/bash

# This scripts transforms the output of clang's -ftime-trace JSON files into a format to upload to ClickHouse

# Example:
#   mkdir time_trace
#   utils/prepare-time-trace/prepare-time-trace.sh build time_trace

# See also https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview

true<<///
CREATE TABLE build_time_trace
(
    -- extra columns here
    file String,
    library LowCardinality(String),
    date Date DEFAULT toDate(time),
    time DateTime64(6),

    pid UInt32,
    tid UInt32,
    ph Enum8('B', 'E', 'X', 'i', 'I', 'C', 'b', 'n', 'e', 'S', 'T', 'p', 'F', 's', 't', 'f', 'P', 'N', 'O', 'D', 'M'),
    ts UInt64,
    dur UInt64,
    cat LowCardinality(String),
    name LowCardinality(String),
    detail String,
    count UInt64,
    avgMs UInt64,
    args_name LowCardinality(String),
    is_total Bool DEFAULT name LIKE 'Total %'
)
ENGINE = MergeTree ORDER BY (date, file, name, args_name);
///

INPUT_DIR=$1
OUTPUT_DIR=$2

find "$INPUT_DIR" -name '*.json' | grep -P '\.(c|cpp|cc|cxx)\.json$' | xargs -P "$(nproc)" -I{} bash -c "

    ORIGINAL_FILENAME=\$(echo '{}' | sed -r -e 's!\.json\$!!; s!/CMakeFiles/[^/]+\.dir!!')
    LIBRARY_NAME=\$(echo '{}' | sed -r -e 's!^.*/CMakeFiles/([^/]+)\.dir/.*\$!\1!')
    START_TIME=\$(jq '.beginningOfTime' '{}')

    jq -c '.traceEvents[] | [\"'\"\$ORIGINAL_FILENAME\"'\", \"'\"\$LIBRARY_NAME\"'\", '\$START_TIME', .pid, .tid, .ph, .ts, .dur, .cat, .name, .args.detail, .args.count, .args[\"avg ms\"], .args.name]' '{}' > \"${OUTPUT_DIR}/\$\$\"
"

# Now you can upload it as follows:

#cat "$OUTPUT_DIR"/* | clickhouse-client --progress --query "INSERT INTO build_time_trace (extra_column_names, file, library, time, pid, tid, ph, ts, dur, cat, name, detail, count, avgMs, args_name) FORMAT JSONCompactEachRow"
