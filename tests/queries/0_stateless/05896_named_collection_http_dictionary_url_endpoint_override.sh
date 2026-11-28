#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc_endpoint="${CLICKHOUSE_TEST_UNIQUE_NAME}_endpoint"
nc_url="${CLICKHOUSE_TEST_UNIQUE_NAME}_url"
nc_locked="${CLICKHOUSE_TEST_UNIQUE_NAME}_locked"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"

base="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

function cleanup()
{
    # Neither ignore nor fuzz these drops: each dictionary must be gone before the named collection it references.
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SET ast_fuzzer_any_query = 0, ignore_drop_queries_probability = 0;
        DROP DICTIONARY IF EXISTS dict_url_over_endpoint;
        DROP DICTIONARY IF EXISTS dict_endpoint_over_url;
        DROP DICTIONARY IF EXISTS dict_url_over_locked_endpoint;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS $nc_endpoint;
        DROP NAMED COLLECTION IF EXISTS $nc_url;
        DROP NAMED COLLECTION IF EXISTS $nc_locked;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE NAMED COLLECTION $nc_endpoint AS
        endpoint = '/?query=SELECT+1,100+FORMAT+TSV', user = 'default', password = '', format = 'TSV';
    CREATE NAMED COLLECTION $nc_url AS
        url = '$base', user = 'default', password = '', format = 'TSV';
    CREATE NAMED COLLECTION $nc_locked AS
        endpoint = '/?query=SELECT+1,100+FORMAT+TSV' NOT OVERRIDABLE, user = 'default', password = '', format = 'TSV';

    CREATE USER $user;
    GRANT SOURCES ON *.* TO $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY, dictGet ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON $nc_endpoint TO $user;
    GRANT NAMED COLLECTION ON $nc_url TO $user;
    GRANT NAMED COLLECTION ON $nc_locked TO $user;
"

echo 'A url over a stored endpoint, and an endpoint over a stored url, require the secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_url_over_endpoint (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(HTTP(NAME $nc_endpoint URL '$base')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError ACCESS_DENIED }
    CREATE DICTIONARY dict_endpoint_over_url (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(HTTP(NAME $nc_url ENDPOINT '/?query=SELECT+1,200+FORMAT+TSV')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError ACCESS_DENIED }
"

echo 'With the secrets privilege they are allowed'
${CLICKHOUSE_CLIENT} --multiquery --query "
    GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc_endpoint TO $user;
    GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc_url TO $user;
    GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc_locked TO $user;
"
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_url_over_endpoint (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(HTTP(NAME $nc_endpoint URL '$base')) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_url_over_endpoint', 'value', toUInt64(1));
    CREATE DICTIONARY dict_endpoint_over_url (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(HTTP(NAME $nc_url ENDPOINT '/?query=SELECT+1,200+FORMAT+TSV')) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_endpoint_over_url', 'value', toUInt64(1));
"

echo 'A url over a NOT OVERRIDABLE endpoint is rejected even with the secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_url_over_locked_endpoint (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(HTTP(NAME $nc_locked URL '$base')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
"
