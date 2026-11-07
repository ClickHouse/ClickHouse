#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the url/urlCluster table functions and the URL table engine are not in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The request payload of `body(...)` can carry credentials, and its subquery form can embed literals or nested
# table functions with secrets, so the whole argument is hidden. The output parses again.
while read -r query; do
    formatted=$($CLICKHOUSE_FORMAT --oneline --query "$query")
    echo "$formatted"
    $CLICKHOUSE_FORMAT --oneline --query "$formatted" > /dev/null 2>&1 || echo "Does not parse: $formatted"
done <<'QUERIES'
SELECT * FROM url('https://example.com/api', 'JSONLines', headers('Content-Type' = 'application/json'), body('{"token":"plain_token"}'))
SELECT * FROM url('https://example.com/api', 'JSONLines', body((SELECT 'plain_subquery_secret' AS token), 'TabSeparated'))
SELECT * FROM url('https://example.com/api', 'JSONLines', body((SELECT * FROM s3('https://bucket/file', 'AKID', 'plain_secret_key'))))
SELECT * FROM urlCluster('test_cluster', 'https://example.com/api', 'JSONLines', body('plain_cluster_body'))
SELECT * FROM url(url_creds, body('plain_named_collection_body'))
CREATE TABLE test_url (key UInt64) ENGINE = URL('https://example.com/api', 'JSONLines', body('plain_engine_body'))
QUERIES
