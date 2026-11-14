#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `use_native_gcs = 1`, a URL on the virtual-hosted form of the default GCS host
# (`https://<bucket>.storage.googleapis.com/<key>`) takes the bucket from the host and the key from the
# whole path. The shared `S3::URI` parser does not know this host and, with `s3_uri_style = 'AUTO'`, used
# to take the first path component as the bucket and validate it as one, rejecting a valid URL whose
# first path component is shorter than 3 or longer than 63 bytes. `EXPLAIN` with an explicit structure
# only builds the configuration, so no request is sent to GCS.
#
# The native backend only exists when the google-cloud-cpp SDK is compiled in; without it, emit the
# expected output directly.
native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

urls=(
    "https://my-bucket.storage.googleapis.com/a/b"
    "https://my.dotted.bucket.storage.googleapis.com/a/b"
    "https://my-bucket.storage.googleapis.com/$(printf 'x%.0s' {1..70})/b"
)

for url in "${urls[@]}"
do
    if [ "$native_gcs_available" != "1" ]; then
        echo "ok"
        continue
    fi
    ${CLICKHOUSE_CLIENT} --query "EXPLAIN SELECT * FROM gcs('${url}', NOSIGN, 'TSV', 'x UInt8') SETTINGS use_native_gcs = 1" > /dev/null && echo "ok"
done
