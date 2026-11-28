#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
suffix="${db}_05872"

invoker="u_invoker_${suffix}"
definer="u_definer_${suffix}"
outer_definer="u_outer_definer_${suffix}"
r_user="r_user_${suffix}"
r_some="r_some_${suffix}"
r_inherited="r_inherited_${suffix}"
r_definer="r_definer_${suffix}"
r_outer="r_outer_${suffix}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $invoker, $definer, $outer_definer"
    ${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS $r_user, $r_some, $r_inherited, $r_definer, $r_outer"
}

cleanup

${CLICKHOUSE_CLIENT} <<EOF2
CREATE ROLE $r_user, $r_some, $r_inherited, $r_definer, $r_outer;
GRANT $r_inherited TO $r_some;
CREATE USER $invoker, $definer, $outer_definer;
GRANT $r_user, $r_some TO $invoker;
GRANT $r_definer TO $definer;
GRANT $r_outer TO $outer_definer;

CREATE VIEW $db.v_definer DEFINER = $definer SQL SECURITY DEFINER
AS SELECT currentRoles() AS cur, enabledRoles() AS en, currentInvokerRoles() AS cur_inv, enabledInvokerRoles() AS en_inv;

CREATE VIEW $db.v_none SQL SECURITY NONE
AS SELECT currentInvokerRoles() AS cur_inv, enabledInvokerRoles() AS en_inv;

CREATE VIEW $db.v_invoker SQL SECURITY INVOKER
AS SELECT currentRoles() AS cur, enabledRoles() AS en, currentInvokerRoles() AS cur_inv, enabledInvokerRoles() AS en_inv;

CREATE VIEW $db.v_outer DEFINER = $outer_definer SQL SECURITY DEFINER
AS SELECT * FROM $db.v_definer;

CREATE VIEW $db.v_definer_over_invoker DEFINER = $definer SQL SECURITY DEFINER
AS SELECT * FROM $db.v_invoker;

GRANT SELECT ON $db.v_definer TO $invoker, $outer_definer;
GRANT SELECT ON $db.v_none, $db.v_invoker, $db.v_outer, $db.v_definer_over_invoker TO $invoker;
GRANT SELECT ON $db.v_invoker TO $definer;
EOF2

function run()
{
    ${CLICKHOUSE_CLIENT} --user "$invoker" --query "$1" | sed "s/_${suffix}//g"
}

echo "--- no view"
run "SET ROLE $r_user; SELECT currentInvokerRoles(), enabledInvokerRoles()"
run "SET ROLE $r_user, $r_some; SELECT currentInvokerRoles(), enabledInvokerRoles()"

echo "--- definer view"
run "SET ROLE $r_user; SELECT * FROM $db.v_definer"
run "SET ROLE $r_user, $r_some; SELECT * FROM $db.v_definer"

echo "--- none view"
run "SET ROLE $r_user, $r_some; SELECT * FROM $db.v_none"

echo "--- invoker view"
run "SET ROLE $r_user, $r_some; SELECT * FROM $db.v_invoker"

echo "--- definer view inside definer view"
run "SET ROLE $r_user, $r_some; SELECT * FROM $db.v_outer"

echo "--- invoker view inside definer view"
run "SET ROLE $r_user, $r_some; SELECT * FROM $db.v_definer_over_invoker"

cleanup
