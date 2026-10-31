#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

# With `http_client = 'gcp_oauth'`, the S3 client authenticates with a bearer token added by its own HTTP
# client. The `Lance` object store is a separate client without it, so such reads must be rejected instead
# of being sent anonymously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COLLECTION="nc_gcp_oauth_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${COLLECTION} AS
        url = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05458.lance',
        http_client = 'gcp_oauth',
        google_adc_client_id = 'id',
        google_adc_client_secret = 'secret',
        google_adc_refresh_token = 'token'"

$CLICKHOUSE_CLIENT --allow_experimental_lance 1 -q "SELECT count() FROM lanceS3(${COLLECTION})" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${COLLECTION}"
