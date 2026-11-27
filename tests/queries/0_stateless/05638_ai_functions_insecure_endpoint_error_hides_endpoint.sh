#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Errors about the endpoint of an AI named collection do not contain the endpoint.

INSECURE="ai_insecure_${CLICKHOUSE_DATABASE}"
MALFORMED="ai_malformed_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION IF EXISTS ${INSECURE};
    DROP NAMED COLLECTION IF EXISTS ${MALFORMED};
    CREATE NAMED COLLECTION ${INSECURE} AS
        provider = 'openai', endpoint = 'http://ai.example.com/v1/chat?access_token=ENDPOINT_TOKEN_SECRET', api_key = 'fake-key';
    CREATE NAMED COLLECTION ${MALFORMED} AS
        provider = 'openai', endpoint = 'https://ai.example.com/ENDPOINT_TOKEN_SECRET%', api_key = 'fake-key';
"

for collection in "${INSECURE}" "${MALFORMED}"
do
    error=$($CLICKHOUSE_CLIENT -q "SELECT aiGenerate('ping', map('credentials', '${collection}'))" 2>&1)
    echo "$error" | grep -q "BAD_ARGUMENTS" && echo "BAD_ARGUMENTS"
    echo "$error" | grep -q "ENDPOINT_TOKEN_SECRET" && echo "endpoint leaked"
done

$CLICKHOUSE_CLIENT -q "
    DROP NAMED COLLECTION ${INSECURE};
    DROP NAMED COLLECTION ${MALFORMED};
"
