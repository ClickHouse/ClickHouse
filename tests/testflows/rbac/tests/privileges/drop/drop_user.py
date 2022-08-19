from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def drop_user_granted_directly(self, node=None):
    """Check that a user is able to execute `DROP USER` with privileges are granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=drop_user,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in drop_user.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def drop_user_granted_via_role(self, node=None):
    """Check that a user is able to execute `DROP USER` with privileges are granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=drop_user,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in drop_user.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("ACCESS MANAGEMENT",),
        ("DROP USER",),
    ],
)
def drop_user(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `DROP USER` when they have the necessary privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("DROP USER without privilege"):

        drop_user_name = f"drop_user_{getuid()}"

        with user(node, drop_user_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with When("I check the user can't drop a user"):
                node.query(
                    f"DROP USER {drop_user_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario("DROP USER with privilege"):
        drop_user_name = f"drop_user_{getuid()}"

        with user(node, drop_user_name):
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can drop a user"):
                node.query(
                    f"DROP USER {drop_user_name}", settings=[("user", f"{user_name}")]
                )

    with Scenario("DROP USER on cluster"):
        drop_user_name = f"drop_user_{getuid()}"

        try:
            with Given("I have a user on a cluster"):
                node.query(f"CREATE USER {drop_user_name} ON CLUSTER sharded_cluster")

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can drop a user"):
                node.query(
                    f"DROP USER {drop_user_name} ON CLUSTER sharded_cluster",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the user"):
                node.query(
                    f"DROP USER IF EXISTS {drop_user_name} ON CLUSTER sharded_cluster"
                )

    with Scenario("DROP USER with revoked privilege"):
        drop_user_name = f"drop_user_{getuid()}"

        with user(node, drop_user_name):
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user can't drop a user"):
                node.query(
                    f"DROP USER {drop_user_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestFeature
@Name("drop user")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_DropUser("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of DROP USER."""
    self.context.node = self.context.cluster.node(node)

    Suite(run=drop_user_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=drop_user_granted_via_role, setup=instrument_clickhouse_server_log)
