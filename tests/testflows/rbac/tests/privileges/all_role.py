from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestScenario
def privilege_check(self, node=None):
    '''Check that a role named ALL only grants privileges that it already has.
    '''

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, "ALL"):

        with When("I grant the ALL role to the user"):
            node.query(f"GRANT ALL TO {user_name}")

        with Then("I check the user doesn't have any privileges"):
            output = node.query("SHOW TABLES", settings=[("user",user_name)]).output
            assert output == '', error()

@TestFeature
@Name("all role")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_RoleAll("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of the role 'ALL'.
    """
    self.context.node = self.context.cluster.node(node)

    Scenario(run=privilege_check, setup=instrument_clickhouse_server_log)
