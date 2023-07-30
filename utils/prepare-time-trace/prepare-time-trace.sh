#!/bin/bash

# This scripts transforms the output of clang's -ftime-trace JSON files into a format to upload to ClickHouse

# Example:
#   mkdir time_trace
#   utils/prepare-time-trace/prepare-time-trace.sh build time_trace

INPUT_DIR=$1
OUTPUT_DIR=$2
EXTRA_COLUMN_VALUES=$3

find "$INPUT_DIR" -name '*.json' | grep -P '\.(c|cpp|cc|cxx)\.json$' | xargs -P $(nproc) -I{} bash -c "

    ORIGINAL_FILENAME=\$(echo '{}' | sed -r -e 's!\.json\$!!; s!/CMakeFiles/[^/]+\.dir!!')
    LIBRARY_NAME=\$(echo '{}' | sed -r -e 's!^.*/CMakeFiles/([^/]+)\.dir/.*\$!\1!')
    START_TIME=\$(jq '.beginningOfTime' '{}')

    jq -c '.traceEvents[] | [${EXTRA_COLUMN_VALUES} \"'\"\$ORIGINAL_FILENAME\"'\", \"'\"\$LIBRARY_NAME\"'\", '\$START_TIME', .pid, .tid, .ph, .ts, .dur, .cat, .name, .args.detail, .args.count, .args[\"avg ms\"], .args.name]' '{}' > \"${OUTPUT_DIR}/\$\$\"
"

# Now you can upload it as follows:

#cat "$OUTPUT_DIR"/* | clickhouse-client --progress --query "INSERT INTO build_time_trace (extra_column_names, file, library, time, pid, tid, ph, ts, dur, cat, name, detail, count, avgMs, args_name) FORMAT JSONCompactEachRow"
