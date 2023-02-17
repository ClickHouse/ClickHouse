#!/usr/bin/env bash
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function query { $CLICKHOUSE_CLIENT --query "$1" $2; }
function user_query { $CLICKHOUSE_CLIENT --user u_25337 --pass pass --query "$1" $2; }

pass_hash='A7EEC567280387F6DF7E13B0DB58D4F23AC3B9C2B93A8F2ECD71EB24E349F793'
pass_salt='F3764AB20F28FE947C30D6CEE4C4D911A84BA6A20EF4D86E4AAA324950E543E9'

query "DROP USER IF EXISTS u_25337"
query "CREATE USER u_25337 IDENTIFIED WITH sha256_hash BY '$pass_hash' SALT '$pass_salt'"
query "GRANT SHOW USERS ON *.* TO u_25337"
query "GRANT SHOW COLUMNS ON *.* TO u_25337"

function run_cases {
    query "SHOW CREATE $1" --display_secrets_in_show_and_select_query=1 # setting, rights
    query "SHOW CREATE $1" # no setting, rights

    user_query "SHOW CREATE $1" # no setting, no rights
    user_query "SHOW CREATE $1" --display_secrets_in_show_and_select_query=1 # setting, no rights
    query "GRANT displaySecretsInShowSelect ON *.* TO u_25337"
    user_query "SHOW CREATE $1" --display_secrets_in_show_and_select_query=1 # setting, rights
    user_query "SHOW CREATE $1" # no setting, rights
    query "REVOKE displaySecretsInShowSelect ON *.* FROM u_25337"
    user_query "SHOW CREATE $1" # no setting, no rights
}

run_cases "USER u_25337"

pass_2_hash='AC2842359DAC91AD4330876D2FF9326BA4A241B07EADDCF113D7455CF34EBFD9'
pass_2_salt='64D3F610C43CCCB1609FD27304BD94CB316B1DB1ACCFCB022D519A4074E59A07'

query "ALTER USER u_25337 IDENTIFIED WITH sha256_hash BY '$pass_2_hash' SALT '$pass_2_salt'"
query "SHOW CREATE USER u_25337"
query "SHOW CREATE USER u_25337" --display_secrets_in_show_and_select_query=1
query "ALTER USER u_25337 IDENTIFIED WITH sha256_hash BY '$pass_hash' SALT '$pass_salt'"
query "SHOW CREATE USER u_25337" --display_secrets_in_show_and_select_query=1

query "DROP TABLE IF EXISTS t_25337"
query "CREATE TABLE t_25337 (n Int32) ENGINE MySQL('mysql53:1234', 'db', 'table', 'user', 'pass')"
run_cases "TABLE t_25337"

query "DROP TABLE t_25337"
query "DROP USER u_25337"
