from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `DROP ROW POLICY` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=drop_row_policy,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in drop_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `DROP ROW POLICY` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=drop_row_policy,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in drop_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("ACCESS MANAGEMENT",),
    ("DROP ROW POLICY",),
    ("DROP POLICY",),
])
def drop_row_policy(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `DROP ROW POLICY` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("DROP ROW POLICY without privilege"):
        drop_row_policy_name = f"drop_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {drop_row_policy_name} ON {table_name}")

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't drop a row policy"):
                node.query(f"DROP ROW POLICY {drop_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {drop_row_policy_name} ON {table_name}")

    with Scenario("DROP ROW POLICY with privilege"):
        drop_row_policy_name = f"drop_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {drop_row_policy_name} ON {table_name}")

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can drop a row policy"):
                node.query(f"DROP ROW POLICY {drop_row_policy_name} ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {drop_row_policy_name} ON {table_name}")

    with Scenario("DROP ROW POLICY on cluster"):
        drop_row_policy_name = f"drop_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy on a cluster"):
                node.query(f"CREATE ROW POLICY {drop_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}")

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can drop a row policy"):
                node.query(f"DROP ROW POLICY {drop_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the user"):
                node.query(f"DROP ROW POLICY IF EXISTS {drop_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}")

    with Scenario("DROP ROW POLICY with revoked privilege"):
        drop_row_policy_name = f"drop_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {drop_row_policy_name} ON {table_name}")

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the database"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot drop row policy"):
                node.query(f"DROP ROW POLICY {drop_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)
        finally:
             with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {drop_row_policy_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Restriction("1.0")
)
def drop_all_pol_with_conditions(self, node=None):
    """Check that when all policies with conditions are dropped, the table becomes unrestricted.
    """

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy has a condition"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

        with And("I can't see any of the rows on the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

        with When("I drop the row policy"):
            node.query(f"DROP ROW POLICY {pol_name} ON {table_name}")

        with Then("I select all the rows from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output and '2' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0"),
)
def drop_on(self, node=None):
    """Check that when a row policy is dropped, users are able to access rows restricted by that policy.
    """

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    with table(node, table_name):

        with Given("I have a row policy"):
            row_policy(name=pol_name, table=table_name)

        with And("The row policy has a condition"):
            node.query(f"ALTER ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

        with And("The table has some values"):
            node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

        with And("I can't see one of the rows on the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output and '2' not in output, error()

        with When("I drop the row policy"):
            node.query(f"DROP ROW POLICY {pol_name} ON {table_name}")

        with Then("I select all the rows from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output and '2' in output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Drop_OnCluster("1.0"),
)
def drop_on_cluster(self, node=None):
    """Check that when a row policy is dropped on a cluster, it works on all nodes.
    """

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name} FOR SELECT USING 1")

        with And("There are some values on the table on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("There are some values on the table on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with When("I drop the row policy on cluster"):
            node.query(f"DROP ROW POLICY {pol_name} ON {table_name} ON CLUSTER sharded_cluster")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

        with And("I select from another node on the cluster"):
            output = node2.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE {table_name} ON CLUSTER sharded_cluster")

@TestFeature
@Name("drop row policy")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_DropRowPolicy("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of DROP ROW POLICY.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.node2 = self.context.cluster.node("clickhouse2")

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)

    Scenario(run=drop_all_pol_with_conditions, setup=instrument_clickhouse_server_log)
    Scenario(run=drop_on, setup=instrument_clickhouse_server_log)
    Scenario(run=drop_on_cluster, setup=instrument_clickhouse_server_log)
