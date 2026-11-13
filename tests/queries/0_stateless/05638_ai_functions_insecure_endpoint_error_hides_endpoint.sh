#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The insecure-endpoint error does not contain the endpoint of the named collection.

COLLECTION="ai_insecure_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${COLLECTION};
    CREATE NAMED COLLECTION ${COLLECTION} AS
        provider = 'openai', endpoint = 'http://ai.example.com/v1/chat?access_token=ENDPOINT_TOKEN_SECRET', api_key = 'fake-key';
"

error=$($CLICKHOUSE_CLIENT -q "SELECT aiGenerate('ping', map('credentials', '${COLLECTION}'))" 2>&1)

echo "$error" | grep -c "BAD_ARGUMENTS"
echo "$error" | grep -c "ENDPOINT_TOKEN_SECRET"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${COLLECTION}"
