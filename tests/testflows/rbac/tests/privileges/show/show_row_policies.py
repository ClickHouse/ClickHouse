from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@contextmanager
def row_policy(node, name, table):
    try:
        with Given("I have a row policy"):
            node.query(f"CREATE ROW POLICY {name} ON {table}")

        yield

    finally:
        with Finally("I drop the row policy"):
            node.query(f"DROP ROW POLICY IF EXISTS {name} ON {table}")

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SHOW ROW POLICIES` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=check_privilege,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in check_privilege.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SHOW ROW POLICIES` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_privilege,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in check_privilege.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("ACCESS MANAGEMENT",),
    ("SHOW ACCESS",),
    ("SHOW ROW POLICIES",),
    ("SHOW POLICIES",),
    ("SHOW CREATE ROW POLICY",),
    ("SHOW CREATE POLICY",),
])
def check_privilege(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for commands that require SHOW ROW POLICIES privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=show_row_policies)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)
    Suite(test=show_create)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowRowPolicies_RequiredPrivilege("1.0"),
)
def show_row_policies(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SHOW ROW POLICIES` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SHOW ROW POLICIES without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use SHOW ROW POLICIES"):
            node.query(f"SHOW ROW POLICIES", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("SHOW ROW POLICIES with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use SHOW ROW POLICIES"):
            node.query(f"SHOW ROW POLICIES", settings = [("user", f"{user_name}")])

    with Scenario("SHOW ROW POLICIES with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use SHOW ROW POLICIES"):
            node.query(f"SHOW ROW POLICIES", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowCreateRowPolicy_RequiredPrivilege("1.0"),
)
def show_create(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SHOW CREATE ROW POLICY` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SHOW CREATE ROW POLICY without privilege"):
        target_row_policy_name = f"target_row_policy_{getuid()}"
        table_name = f"table_{getuid()}"

        with row_policy(node, target_row_policy_name, table_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't use SHOW CREATE ROW POLICY"):
                node.query(f"SHOW CREATE ROW POLICY {target_row_policy_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("SHOW CREATE ROW POLICY with privilege"):
        target_row_policy_name = f"target_row_policy_{getuid()}"
        table_name = f"table_{getuid()}"

        with row_policy(node, target_row_policy_name, table_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can use SHOW CREATE ROW POLICY"):
                node.query(f"SHOW CREATE ROW POLICY {target_row_policy_name}", settings = [("user", f"{user_name}")])

    with Scenario("SHOW CREATE ROW POLICY with revoked privilege"):
        target_row_policy_name = f"target_row_policy_{getuid()}"
        table_name = f"table_{getuid()}"

        with row_policy(node, target_row_policy_name, table_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot use SHOW CREATE ROW POLICY"):
                node.query(f"SHOW CREATE ROW POLICY {target_row_policy_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("show row policies")
@Requirements(
    RQ_SRS_006_RBAC_ShowRowPolicies_Privilege("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW ROW POLICYS.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
