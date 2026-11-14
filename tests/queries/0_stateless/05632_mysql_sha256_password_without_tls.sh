#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user with `sha256_password` logs in over the MySQL protocol without TLS: the client encrypts the password
# with the RSA public key of the server, which the server obtains on the first such login and reuses afterwards.

user="user_sha256_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}; CREATE USER ${user} IDENTIFIED WITH sha256_password BY 'secret'"

# MySQL 8 clients disable TLS with --ssl-mode=DISABLED, MariaDB clients with --skip-ssl.
if ${MYSQL_CLIENT_BINARY} --help 2>/dev/null | grep -q -- '--ssl-mode'; then
    no_tls="--ssl-mode=DISABLED"
else
    no_tls="--skip-ssl"
fi

mysql_as_user()
{
    ${MYSQL_CLIENT_BINARY} --protocol tcp --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_MYSQL}" \
        "${no_tls}" --user "${user}" --password="$1" --batch --skip-column-names --execute "SELECT $2" 2>&1 \
        | grep -v -F 'Using a password on the command line interface can be insecure'
}

mysql_as_user secret 1
mysql_as_user secret 2
mysql_as_user wrong 3 | grep -c -F "${user}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
