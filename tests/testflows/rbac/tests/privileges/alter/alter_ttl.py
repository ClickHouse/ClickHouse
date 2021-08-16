import json

from testflows.core import *
from testflows.core import threading
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors
from rbac.helper.tables import table_types

subprivileges = {
    "TTL" : 1 << 0,
    "MATERIALIZE TTL" : 1 << 1,
}

aliases = {
    "TTL" : ["ALTER TTL", "ALTER MODIFY TTL", "MODIFY TTL"],
    "MATERIALIZE TTL": ["ALTER MATERIALIZE TTL", "MATERIALIZE TTL", "ALL"],
}

permutation_count = (1 << len(subprivileges))

def permutations():
    """Returns list of all permutations to run.
    Currently includes NONE, TTL, MATERIALIZE TTL, and both
    """
    return [*range(permutation_count)]

def alter_ttl_privileges(grants: int):
    """Takes in an integer, and returns the corresponding set of tests to grant and
    not grant using the binary string. Each integer corresponds to a unique permutation
    of grants.
    """
    note(grants)
    privileges = []

    if grants==0: # No privileges
        privileges.append("NONE")
    else:
        if (grants & subprivileges["TTL"]):
            privileges.append(f"ALTER TTL")
        if (grants & subprivileges["MATERIALIZE TTL"]):
            privileges.append(f"ALTER MATERIALIZE TTL")

    note(f"Testing these privileges: {privileges}")
    return ', '.join(privileges)

def alter_ttl_privilege_handler(grants, table, user, node):
    """For all 2 subprivileges, if the privilege is granted: run test to ensure correct behavior,
    and if the privilege is not granted, run test to ensure correct behavior there as well
    """

    if (grants & subprivileges["TTL"]):
        with When("I check ttl when privilege is granted"):
            check_ttl_when_privilege_is_granted(table, user, node)
    else:
        with When("I check ttl when privilege is not granted"):
            check_ttl_when_privilege_is_not_granted(table, user, node)
    if (grants & subprivileges["MATERIALIZE TTL"]):
        with When("I check materialize ttl when privilege is granted"):
            check_materialize_ttl_when_privilege_is_granted(table, user, node)
    else:
        with When("I check materialize ttl when privilege is not granted"):
            check_materialize_ttl_when_privilege_is_not_granted(table, user, node)

def check_ttl_when_privilege_is_granted(table, user, node):
    """Ensures ALTER TTL runs as expected when the privilege is granted to the specified user
    """
    with Given(f"I modify TTL"):
        node.query(f"ALTER TABLE {table} MODIFY TTL d + INTERVAL 1 DAY;",
            settings = [("user", user)])

    with Then("I verify that the TTL clause is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert "TTL d + toIntervalDay(1)" in output['statement'], error()

def check_materialize_ttl_when_privilege_is_granted(table, user, node):
    """Ensures MATERIALIZE TTL runs as expected when the privilege is granted to the specified user
    """
    with Given("I modify TTL so it exists"):
        node.query(f"ALTER TABLE {table} MODIFY TTL d + INTERVAL 1 MONTH;")

    with Then("I materialize the TTL"):
        node.query(f"ALTER TABLE {table} MATERIALIZE TTL IN PARTITION 2",
            settings = [("user", user)])

    with Then("I verify that the TTL clause is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert "TTL d + toIntervalMonth(1)" in output['statement'], error()

def check_ttl_when_privilege_is_not_granted(table, user, node):
    """Ensures ALTER TTL errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} MODIFY TTL d + INTERVAL 1 DAY;",
                    settings = [("user", user)], exitcode=exitcode, message=message)

def check_materialize_ttl_when_privilege_is_not_granted(table, user, node):
    """Ensures MATERIALIZE TTL errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} MATERIALIZE TTL IN PARTITION 4",
                    settings = [("user", user)], exitcode=exitcode, message=message)

@TestScenario
def user_with_some_privileges(self, table_type, node=None):
    """Check that user with any permutation of ALTER TTL subprivileges is able
    to alter the table for privileges granted, and not for privileges not granted.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations():
        privileges = alter_ttl_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name):
                with Given("I first grant the privileges"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

                with Then(f"I try to ALTER TTL"):
                    alter_ttl_privilege_handler(permutation, table_name, user_name, node)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterTTL_Revoke("1.0"),
)
def user_with_revoked_privileges(self, table_type, node=None):
    """Check that user is unable to ALTER TTLs on table after ALTER TTL privilege
    on that table has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations():
        privileges = alter_ttl_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name):
                with Given("I first grant the privileges"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

                with And("I then revoke the privileges"):
                    node.query(f"REVOKE {privileges} ON {table_name} FROM {user_name}")

                with When(f"I try to ALTER TTL"):
                    # Permutation 0: no privileges
                    alter_ttl_privilege_handler(0, table_name, user_name, node)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterTTL_Grant("1.0"),
)
def role_with_some_privileges(self, table_type, node=None):
    """Check that user can ALTER TTL on a table after it is granted a role that
    has the ALTER TTL privilege for that table.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    for permutation in permutations():
        privileges = alter_ttl_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name), role(node, role_name):
                with Given("I grant the ALTER TTL privilege to a role"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

                with And("I grant role to the user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with Then(f"I try to ALTER TTL"):
                    alter_ttl_privilege_handler(permutation, table_name, user_name, node)

@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has ALTER TTL privilege on a table is unable to
    ALTER TTL from that table after the role with privilege has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    for permutation in permutations():
        privileges = alter_ttl_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name), role(node, role_name):
                with When("I grant privileges to a role"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

                with And("I grant the role to a user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with And("I revoke the role from the user"):
                    node.query(f"REVOKE {role_name} FROM {user_name}")

                with And("I ALTER TTL on the table"):
                    # Permutation 0: no privileges for any permutation
                    alter_ttl_privilege_handler(0, table_name, user_name, node)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterTTL_Cluster("1.0"),
)
def user_with_privileges_on_cluster(self, table_type, node=None):
    """Check that user is able to ALTER TTL on a table with
    privilege granted on a cluster.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations():
        privileges = alter_ttl_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type):
                try:
                    with Given("I have a user on a cluster"):
                        node.query(f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster")

                    with When("I grant ALTER TTL privileges on a cluster"):
                        node.query(f"GRANT ON CLUSTER sharded_cluster {privileges} ON {table_name} TO {user_name}")

                    with Then(f"I try to ALTER TTL"):
                        alter_ttl_privilege_handler(permutation, table_name, user_name, node)
                finally:
                    with Finally("I drop the user on a cluster"):
                        node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterTTL("1.0"),
    RQ_SRS_006_RBAC_Privileges_AlterTTL_TableEngines("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Name("alter ttl")
def feature(self, node="clickhouse1", stress=None, parallel=None):
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
            with Pool(5) as pool:
                tasks = []
                try:
                    for scenario in loads(current_module(), Scenario):
                        run_scenario(pool, tasks, Scenario(test=scenario, setup=instrument_clickhouse_server_log), {"table_type" : table_type})
                finally:
                    join(tasks)
