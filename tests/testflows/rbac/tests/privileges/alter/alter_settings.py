import json

from multiprocessing.dummy import Pool

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors
from rbac.helper.tables import table_types

aliases = {"ALTER SETTINGS", "ALTER SETTING", "ALTER MODIFY SETTING", "MODIFY SETTING"}

def check_alter_settings_when_privilege_is_granted(table, user, node):
    """Ensures ADD SETTINGS runs as expected when the privilege is granted to the specified user
    """
    with Given("I check that the modified setting is not already in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert "merge_with_ttl_timeout = 5" not in output['statement'], error()

    with And(f"I modify settings"):
        node.query(f"ALTER TABLE {table} MODIFY SETTING merge_with_ttl_timeout=5",
            settings = [("user", user)])

    with Then("I verify that the setting is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert "SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 5" in output['statement'], error()

def check_alter_settings_when_privilege_is_not_granted(table, user, node):
    """Ensures CLEAR SETTINGS runs as expected when the privilege is granted to the specified user
    """
    with When("I try to use ALTER SETTING, has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} MODIFY SETTING merge_with_ttl_timeout=5",
            settings = [("user", user)], exitcode=exitcode, message=message)

@TestScenario
def user_with_privileges(self, privilege, table_type, node=None):
    """Check that user with ALTER SETTINGS privilege is able
    to alter the table
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    with table(node, table_name, table_type), user(node, user_name):
        with Given("I first grant the privilege"):
            node.query(f"GRANT {privilege} ON {table_name} TO {user_name}")

        with Then(f"I try to ALTER SETTINGS"):
            check_alter_settings_when_privilege_is_granted(table_name, user_name, node)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterSettings_Revoke("1.0"),
)
def user_with_revoked_privileges(self, privilege, table_type, node=None):
    """Check that user is unable to alter settingss on table after ALTER SETTINGS privilege
    on that table has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    with table(node, table_name, table_type), user(node, user_name):
        with Given("I first grant the privilege"):
            node.query(f"GRANT {privilege} ON {table_name} TO {user_name}")

        with And("I then revoke the privileges"):
            node.query(f"REVOKE {privilege} ON {table_name} FROM {user_name}")

        with When(f"I try to ALTER SETTINGS"):
            check_alter_settings_when_privilege_is_not_granted(table_name, user_name, node)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterSettings_Grant("1.0"),
)
def role_with_some_privileges(self, privilege, table_type, node=None):
    """Check that user can alter settings on a table after it is granted a role that
    has the alter settings privilege for that table.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    with table(node, table_name, table_type), user(node, user_name), role(node, role_name):
        with Given("I grant the alter settings privilege to a role"):
            node.query(f"GRANT {privilege} ON {table_name} TO {role_name}")

        with And("I grant role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        with Then(f"I try to ALTER SETTINGS"):
            check_alter_settings_when_privilege_is_granted(table_name, user_name, node)

@TestScenario
def user_with_revoked_role(self, privilege, table_type, node=None):
    """Check that user with a role that has alter settings privilege on a table is unable to
    alter settings from that table after the role with privilege has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    with table(node, table_name, table_type), user(node, user_name), role(node, role_name):
        with When("I grant privileges to a role"):
            node.query(f"GRANT {privilege} ON {table_name} TO {role_name}")

        with And("I grant the role to a user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        with And("I revoke the role from the user"):
            node.query(f"REVOKE {role_name} FROM {user_name}")

        with And("I alter settings on the table"):
            check_alter_settings_when_privilege_is_not_granted(table_name, user_name, node)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterSettings_Cluster("1.0"),
)
def user_with_privileges_on_cluster(self, privilege, table_type, node=None):
    """Check that user is able to alter settings on a table with
    privilege granted on a cluster.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    with When(f"granted=ALTER SETTINGS"):
        with table(node, table_name, table_type):
            try:
                with Given("I have a user on a cluster"):
                    node.query(f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster")

                with When("I grant alter settings privileges on a cluster"):
                    node.query(f"GRANT ON CLUSTER sharded_cluster ALTER SETTINGS ON {table_name} TO {user_name}")

                with Then(f"I try to ALTER SETTINGS"):
                    check_alter_settings_when_privilege_is_granted(table_name, user_name, node)

                with When("I revoke alter settings privileges on a cluster"):
                    node.query(f"REVOKE ON CLUSTER sharded_cluster ALTER SETTINGS ON {table_name} FROM {user_name}")

                with Then(f"I try to ALTER SETTINGS"):
                    check_alter_settings_when_privilege_is_not_granted(table_name, user_name, node)
            finally:
                with Finally("I drop the user on a cluster"):
                    node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")

@TestSuite
def scenario_parallelization(self, table_type, privilege):
    """Runs all scenarios in parallel for a given privilege.
    """
    pool = Pool(4)
    try:
        tasks = []
        try:
            for scenario in loads(current_module(), Scenario):
                run_scenario(pool, tasks, Scenario(test=scenario), {"table_type": table_type, "privilege": privilege})
        finally:
            join(tasks)
    finally:
        pool.close()

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterSettings("1.0"),
    RQ_SRS_006_RBAC_Privileges_AlterSettings_TableEngines("1.0")
)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Name("alter settings")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Runs test suites above which check correctness over scenarios and permutations
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        table_type, = example

        if table_type != "MergeTree" and not self.context.stress:
            continue

        with Example(str(example)):
            pool = Pool(4)
            try:
                tasks = []
                try:
                    for alias in aliases:
                        run_scenario(pool, tasks, Suite(test=scenario_parallelization, name=alias, setup=instrument_clickhouse_server_log), {"table_type": table_type, "privilege": alias})
                finally:
                    join(tasks)
            finally:
                pool.close()
