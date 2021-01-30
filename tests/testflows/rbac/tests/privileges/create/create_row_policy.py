from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `CREATE ROW POLICY` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=create_row_policy, flags=TE,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in create_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `CREATE ROW POLICY` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=create_row_policy, flags=TE,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in create_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ACCESS MANAGEMENT",),
    ("CREATE ROW POLICY",),
    ("CREATE POLICY",),
])
def create_row_policy(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `CREATE ROW POLICY` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("CREATE ROW POLICY without privilege"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When("I check the user can't create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON {table_name}")

    with Scenario("CREATE ROW POLICY with privilege"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON {table_name}")

    with Scenario("CREATE ROW POLICY on cluster"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}")

    with Scenario("CREATE ROW POLICY with revoked privilege"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON {table_name}")

@TestFeature
@Name("create row policy")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_CreateRowPolicy("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of CREATE ROW POLICY.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
