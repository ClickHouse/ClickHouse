#!/bin/bash

# This scripts transforms the output of clang's -ftime-trace JSON files into a format to upload to ClickHouse

# Example:
#   mkdir time_trace
#   utils/prepare-time-trace/prepare-time-trace.sh build time_trace

# See also https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview

true<<///
CREATE TABLE build_time_trace
(
    -- Extra columns:
    pull_request_number UInt32,
    commit_sha String,
    check_start_time DateTime,
    check_name LowCardinality(String),
    instance_type LowCardinality(String),
    instance_id String,

    -- Normal columns:
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
ENGINE = MergeTree
ORDER BY (date, file, name, args_name);
///

INPUT_DIR=$1
OUTPUT_DIR=$2

find "$INPUT_DIR" -name '*.json' -or -name '*.time-trace' | grep -P '\.(c|cpp|cc|cxx)\.json|\.time-trace$' | xargs -P "$(nproc)" -I{} bash -c "

    ORIGINAL_FILENAME=\$(echo '{}' | sed -r -e 's!\.(json|time-trace)\$!!; s!/CMakeFiles/[^/]+\.dir!!')
    LIBRARY_NAME=\$(echo '{}' | sed -r -e 's!^.*/CMakeFiles/([^/]+)\.dir/.*\$!\1!')
    START_TIME=\$(jq '.beginningOfTime' '{}')

    jq -c '.traceEvents[] | [\"'\"\$ORIGINAL_FILENAME\"'\", \"'\"\$LIBRARY_NAME\"'\", '\$START_TIME', .pid, .tid, .ph, .ts, .dur, .cat, .name, .args.detail, .args.count, .args[\"avg ms\"], .args.name]' '{}' > \"${OUTPUT_DIR}/\$\$\"
"

# Now you can upload it as follows:

#cat "$OUTPUT_DIR"/* | clickhouse-client --progress --query "INSERT INTO build_time_trace (extra_column_names, file, library, time, pid, tid, ph, ts, dur, cat, name, detail, count, avgMs, args_name) FORMAT JSONCompactEachRow"

# Additionally, collect information about the sizes of translation units

true<<///
CREATE TABLE binary_sizes
(
    -- Extra columns:
    pull_request_number UInt32,
    commit_sha String,
    check_start_time DateTime,
    check_name LowCardinality(String),
    instance_type LowCardinality(String),
    instance_id String,

    -- Normal columns:
    file LowCardinality(String),
    library LowCardinality(String) DEFAULT extract(file, 'CMakeFiles/([^/]+)\.dir/'),
    size UInt64,
    date Date DEFAULT toDate(time),
    time DateTime64(6) DEFAULT now64()
)
ENGINE = MergeTree
ORDER BY (date, file, pull_request_number, commit_sha, check_name);
///

find "$INPUT_DIR" -type f -executable -or -name '*.o' -or -name '*.a' | grep -v cargo | xargs wc -c | grep -v 'total' > "${OUTPUT_DIR}/binary_sizes.txt"

# Additionally, collect information about the symbols inside translation units
true<<///
CREATE TABLE binary_symbols
(
   -- Extra columns:
   pull_request_number UInt32,
   commit_sha String,
   check_start_time DateTime,
   check_name LowCardinality(String),
   instance_type LowCardinality(String),
   instance_id String,

   -- Normal columns:
   file LowCardinality(String),
   library LowCardinality(String) DEFAULT extract(file, 'CMakeFiles/([^/]+)\.dir/'),
   address UInt64,
   size UInt64,
   type FixedString(1),
   symbol LowCardinality(String),
   date Date DEFAULT toDate(time),
   time DateTime64(6) DEFAULT now64()
)
ENGINE = MergeTree
ORDER BY (date, file, symbol, pull_request_number, commit_sha, check_name);
///

# nm does not work with LTO
if ! grep -q -- '-flto' compile_commands.json
then
    # Find the best alternative of nm
    for name in llvm-nm-{30..18} llvm-nm nm
    do
        NM=$(command -v ${name})
        [[ -n "${NM}" ]] && break
    done

    find "$INPUT_DIR" -type f -name '*.o' | grep -v cargo | find . -name '*.o' | xargs -P $(nproc) -I {} bash -c "
      ${NM} --demangle --defined-only --print-size '{}' | grep -v -P '[0-9a-zA-Z] r ' | sed 's@^@{} @' > '{}.symbols'
    "

    find "$INPUT_DIR" -type f -name '*.o.symbols' | xargs cat > "${OUTPUT_DIR}/binary_symbols.txt"
fi
