#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

# The key of a `Lance` dataset on S3 must reach the `Lance` object store verbatim. As a URL path, the key
# `hash#data.lance` would be cut at `#` and name the dataset `hash` instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="http://localhost:11111/test/${CLICKHOUSE_DATABASE}"

upload_dataset()
{
    local source="$CUR_DIR/data_lance/$1"
    local target="$2"
    (cd "$source" && find . -type f | sed 's|^\./||') | while read -r file; do
        $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${PREFIX}/${target}/${file}', 'test', 'testtest', 'RawBLOB') FORMAT RawBLOB" < "$source/$file"
    done
}

upload_dataset multi_frag.lance 'hash%23data.lance'
upload_dataset basic.lance hash

for url in "${PREFIX}/hash%23data.lance" "${PREFIX}/hash%23data.lance/" "${PREFIX}/hash"; do
    $CLICKHOUSE_CLIENT --allow_experimental_lance 1 -q "
        SELECT count(), any(_file) FROM lanceS3('${url}', 'test', 'testtest')"
done
