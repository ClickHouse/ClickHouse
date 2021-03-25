from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is only able to execute KILL QUERY when they have required privilege, either directly or via role.
    """
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege"):
        with user(node, user_name):

            with When(f"I run checks that {user_name} is only able to execute KILL QUERY with required privileges"):
                privilege_check(grant_target_name=user_name, user_name=user_name, node=node)

    with Suite("user with privilege via role"):
        with user(node, user_name), role(node, role_name):

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(f"I run checks that {user_name} with {role_name} is only able to execute KILL QUERY with required privileges"):
                privilege_check(grant_target_name=role_name, user_name=user_name, node=node)

def privilege_check(grant_target_name, user_name, node=None):
    """Run scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I attempt to kill a query without privilege"):
            node.query(f"KILL QUERY WHERE user ='default'", settings = [("user", user_name)],
                exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with When("I grant kill query privilege"):
            node.query(f"GRANT KILL QUERY TO {grant_target_name}")

        with Then("I attempt to kill a query"):
            node.query(f"KILL QUERY WHERE 1", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with When("I grant the kill query privilege"):
            node.query(f"GRANT KILL QUERY TO {grant_target_name}")

        with And("I revoke the kill query privilege"):
            node.query(f"REVOKE KILL QUERY TO {grant_target_name}")

        with Then("I attempt to kill a query"):
            node.query(f"KILL QUERY WHERE 1", settings = [("user", user_name)],
                exitcode=exitcode, message=message)

    with Scenario("user with revoked ALL privilege"):

        with When("I grant the kill query privilege"):
            node.query(f"GRANT KILL QUERY TO {grant_target_name}")

        with And("I revoke ALL privilege"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt to kill a query"):
            node.query(f"KILL QUERY WHERE 1", settings = [("user", user_name)],
                exitcode=exitcode, message=message)

    with Scenario("execute on cluster"):

        with When("I grant the truncate privilege"):
            node.query(f"GRANT KILL QUERY TO {grant_target_name}")

        with Then("I attempt to kill a query"):
            node.query(f"KILL QUERY ON CLUSTER WHERE 1'", settings = [("user", user_name)])

    with Scenario("user with ALL privilege"):

        with When("I revoke ALL privilege"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with And("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* ON {grant_target_name}")

        with Then("I attempt to kill a query"):
            node.query(f"KILL QUERY WHERE 1", settings = [("user", user_name)])

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_KillQuery("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
@Name("kill query")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of KILL QUERY.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    with Suite(test=privilege_granted_directly_or_via_role, setup=instrument_clickhouse_server_log):
        privilege_granted_directly_or_via_role()
