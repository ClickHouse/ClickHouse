#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, use-mysql
# Tag no-parallel: default/u_25337 queries may interfere, this is a purely sequential test
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function query { $CLICKHOUSE_CLIENT -n --query="$1"; }
function user_query { $CLICKHOUSE_CLIENT -n --user u_25337 --pass pass --query="$1"; }

pass_hash='A7EEC567280387F6DF7E13B0DB58D4F23AC3B9C2B93A8F2ECD71EB24E349F793'
pass_salt='F3764AB20F28FE947C30D6CEE4C4D911A84BA6A20EF4D86E4AAA324950E543E9'
show_secrets="SETTINGS display_secrets_in_show_and_select_query=1"

query "
    DROP USER IF EXISTS u_25337;
    CREATE USER u_25337 IDENTIFIED WITH sha256_hash BY '$pass_hash' SALT '$pass_salt';
    GRANT SHOW USERS ON *.* TO u_25337;
    GRANT SHOW COLUMNS ON *.* TO u_25337"

function run_cases {
    query "SHOW CREATE $1 $show_secrets; SHOW CREATE $1"
    user_query "SHOW CREATE $1; SHOW CREATE $1 $show_secrets"
    query "GRANT displaySecretsInShowSelect ON *.* TO u_25337"
    user_query "SHOW CREATE $1 $show_secrets; SHOW CREATE $1"
    query "REVOKE displaySecretsInShowSelect ON *.* FROM u_25337"
    user_query "SHOW CREATE $1" # no setting, no rights
}

run_cases "USER u_25337"

pass_2_hash='AC2842359DAC91AD4330876D2FF9326BA4A241B07EADDCF113D7455CF34EBFD9'
pass_2_salt='64D3F610C43CCCB1609FD27304BD94CB316B1DB1ACCFCB022D519A4074E59A07'

query "
    ALTER USER u_25337 IDENTIFIED WITH sha256_hash BY '$pass_2_hash' SALT '$pass_2_salt';
    SHOW CREATE USER u_25337;
    SHOW CREATE USER u_25337 $show_secrets;
    ALTER USER u_25337 IDENTIFIED WITH sha256_hash BY '$pass_hash' SALT '$pass_salt';
    SHOW CREATE USER u_25337 $show_secrets;

    DROP TABLE IF EXISTS t_25337;
    CREATE TABLE t_25337 (n Int32) ENGINE MySQL('mysql53:1234', 'db', 'table', 'user', 'pass')"

run_cases "TABLE t_25337"

query "
    SELECT create_table_query, engine_full FROM system.tables WHERE name='t_25337' $show_secrets;
    DROP TABLE t_25337;
    DROP USER u_25337"
