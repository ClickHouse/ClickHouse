#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --oneline --query "$(cat <<'SQL'
CREATE TABLE t (x UInt8) ENGINE = Kafka(c,
    `kafka.sasl_oauthbearer_client_secret` = 'secret1',
    `kafka.consumer.sasl.oauthbearer.client.credentials.client.secret` = 'secret2',
    `kafka.producer.sasl_oauthbearer_assertion_private_key_passphrase` = concat('secret', '3'),
    `kafka.sasl_oauthbearer_token_endpoint_url` = 'https://issuer.example/token?key=secret4',
    `kafka.client_id` = 'visible')
SQL
)"
