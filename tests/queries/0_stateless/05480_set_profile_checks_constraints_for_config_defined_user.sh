#!/usr/bin/env bash
# Selecting a settings profile in a session is checked against the constraints in force there, also for a user
# defined in the server config, which is what the stateless tests run as.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PIN_PROFILE="profile_pin_$CLICKHOUSE_DATABASE"
OTHER_PROFILE="profile_other_$CLICKHOUSE_DATABASE"
BIG_PROFILE="profile_big_$CLICKHOUSE_DATABASE"
FITS_PROFILE="profile_fits_$CLICKHOUSE_DATABASE"
LIFT_PROFILE="profile_lift_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -m -q "
DROP SETTINGS PROFILE IF EXISTS $PIN_PROFILE, $OTHER_PROFILE, $BIG_PROFILE, $FITS_PROFILE, $LIFT_PROFILE;
CREATE SETTINGS PROFILE $PIN_PROFILE SETTINGS max_execution_time = 300 CONST, max_result_rows = 1000 MAX 1000;
CREATE SETTINGS PROFILE $OTHER_PROFILE SETTINGS max_execution_time = 600;
CREATE SETTINGS PROFILE $BIG_PROFILE SETTINGS max_result_rows = 5000;
CREATE SETTINGS PROFILE $FITS_PROFILE SETTINGS max_execution_time = 300 CONST, max_result_rows = 500;
CREATE SETTINGS PROFILE $LIFT_PROFILE SETTINGS max_execution_time WRITABLE;
"

echo '-- SET profile'
# One session on purpose: a rejected statement must leave it untouched, which the statements after it prove.
$CLICKHOUSE_CLIENT -m -q "
SET profile = '$PIN_PROFILE';
SET profile = '$OTHER_PROFILE'; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET profile = '$BIG_PROFILE'; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET profile = '$LIFT_PROFILE'; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SET max_execution_time = 600; -- { serverError SETTING_CONSTRAINT_VIOLATION }
SELECT getSetting('max_execution_time'), getSetting('max_result_rows');
SET profile = '$FITS_PROFILE';
SELECT getSetting('max_execution_time'), getSetting('max_result_rows');
"

echo '-- HTTP profile parameter and SETTINGS clause'
SESSION_ID="session_$CLICKHOUSE_DATABASE"
function run_in_session()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=$SESSION_ID$1" -d "$2"
}
run_in_session "" "SET profile = '$PIN_PROFILE'"
run_in_session "&profile=$OTHER_PROFILE" "SELECT getSetting('max_execution_time')" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
run_in_session "" "SELECT getSetting('max_execution_time') SETTINGS profile = '$OTHER_PROFILE'" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
run_in_session "&profile=$BIG_PROFILE" "SELECT getSetting('max_result_rows')" | grep -o -m1 SETTING_CONSTRAINT_VIOLATION
run_in_session "" "SELECT getSetting('max_execution_time'), getSetting('max_result_rows')"
run_in_session "&profile=$FITS_PROFILE" "SELECT getSetting('max_execution_time'), getSetting('max_result_rows')"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PIN_PROFILE, $OTHER_PROFILE, $BIG_PROFILE, $FITS_PROFILE, $LIFT_PROFILE"
