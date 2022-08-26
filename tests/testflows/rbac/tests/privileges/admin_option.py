from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to grant role with `ADMIN OPTION` privilege granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(test=grant_role)(grant_target_name=user_name, user_name=user_name)


@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to grant role with `ADMIN OPTION` privilege granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(test=grant_role)(grant_target_name=role_name, user_name=user_name)


@TestSuite
def grant_role(self, grant_target_name, user_name, node=None):
    """Check that user is able to execute to grant roles if and only if they have role with `ADMIN OPTION`."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("Grant role without privilege"):
        grant_role_name = f"grant_role_{getuid()}"
        target_user_name = f"target_user_{getuid()}"

        with user(node, target_user_name), role(node, grant_role_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't grant a role"):
                node.query(
                    f"GRANT {grant_role_name} TO {target_user_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario("Grant role with privilege"):
        grant_role_name = f"grant_role_{getuid()}"
        target_user_name = f"target_user_{getuid()}"

        with user(node, target_user_name), role(node, grant_role_name):

            with When(f"I grant ADMIN OPTION"):
                node.query(
                    f"GRANT {grant_role_name} TO {grant_target_name} WITH ADMIN OPTION"
                )

            with Then("I check the user can grant a role"):
                node.query(
                    f"GRANT {grant_role_name} TO {target_user_name}",
                    settings=[("user", f"{user_name}")],
                )

    with Scenario("Grant role on cluster"):
        grant_role_name = f"grant_role_{getuid()}"
        target_user_name = f"target_user_{getuid()}"

        try:
            with Given("I have a role on a cluster"):
                node.query(f"CREATE ROLE {grant_role_name} ON CLUSTER sharded_cluster")

            with And("I have a user on a cluster"):
                node.query(f"CREATE USER {target_user_name} ON CLUSTER sharded_cluster")

            with When("I grant ADMIN OPTION privilege"):
                node.query(
                    f"GRANT {grant_role_name} TO {grant_target_name} WITH ADMIN OPTION"
                )

            with Then("I check the user can grant a role"):
                node.query(
                    f"GRANT {grant_role_name} TO {target_user_name} ON CLUSTER sharded_cluster",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the user"):
                node.query(
                    f"DROP ROLE IF EXISTS {grant_role_name} ON CLUSTER sharded_cluster"
                )

    with Scenario("Grant role with revoked privilege"):
        grant_role_name = f"grant_role_{getuid()}"
        target_user_name = f"target_user_{getuid()}"

        with user(node, target_user_name), role(node, grant_role_name):

            with When(f"I grant ADMIN OPTION"):
                node.query(
                    f"GRANT {grant_role_name} TO {grant_target_name} WITH ADMIN OPTION"
                )

            with And(f"I revoke ADMIN OPTION"):
                node.query(f"REVOKE {grant_role_name} FROM {grant_target_name}")

            with Then("I check the user cannot grant a role"):
                node.query(
                    f"GRANT {grant_role_name} TO {target_user_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestFeature
@Name("admin option")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AdminOption("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of ADMIN OPTION."""
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
