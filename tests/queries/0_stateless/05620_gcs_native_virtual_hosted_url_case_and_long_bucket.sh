#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `use_native_gcs = 1`:
# - the virtual-hosted form of the default GCS host is recognized whatever the case of the scheme and the host:
#   the bucket is taken from the host, not from the first path component. A 2-character bucket in the host
#   shows which one was taken: it is rejected, while the path component `bucket-b` would be a valid bucket;
# - the bucket follows the GCS naming rules: a dotted name may have up to 222 characters, each dot-separated
#   component at most 63, while an undotted name has at most 63 characters.
# `EXPLAIN` with an explicit structure only builds the configuration, so no request is sent to GCS.
#
# The native backend only exists when the google-cloud-cpp SDK is compiled in; without it, emit the
# expected output directly.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

a40=$(printf 'a%.0s' {1..40})
b30=$(printf 'b%.0s' {1..30})
a64=$(printf 'a%.0s' {1..64})
a70=$(printf 'a%.0s' {1..70})

function check()
{
    local url=$1
    local expected=$2
    if [ "$native_gcs_available" != "1" ]; then
        echo "$expected"
        return
    fi
    local output
    if output=$(${CLICKHOUSE_CLIENT} --query "EXPLAIN SELECT * FROM gcs('${url}', NOSIGN, 'TSV', 'x UInt8') SETTINGS use_native_gcs = 1" 2>&1); then
        echo "ok"
    elif grep -q -F "Bucket name length is out of bounds in GCS URI" <<< "$output"; then
        echo "rejected"
    else
        echo "$output"
    fi
}

check "HTTPS://ab.STORAGE.GOOGLEAPIS.COM/bucket-b/file.tsv" "rejected"
check "https://AB.storage.googleapis.com:443/bucket-b/file.tsv" "rejected"
check "Https://Bucket-A.Storage.GoogleAPIs.com/bucket-b/file.tsv" "ok"
check "https://${a40}.${b30}.storage.googleapis.com/file.tsv" "ok"
check "https://storage.googleapis.com/${a40}.${b30}/file.tsv" "ok"
check "gs://${a40}.${b30}/file.tsv" "ok"
check "https://storage.googleapis.com/${a70}/file.tsv" "rejected"
check "https://storage.googleapis.com/${a64}.${b30}/file.tsv" "rejected"
