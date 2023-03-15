from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def create_user_granted_directly(self, node=None):
    """Check that a user is able to execute `CREATE USER` with privileges are granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=create_user,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in create_user.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def create_user_granted_via_role(self, node=None):
    """Check that a user is able to execute `CREATE USER` with privileges are granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=create_user,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in create_user.examples
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
        ("CREATE USER",),
    ],
)
def create_user(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `CREATE USER` when they have the necessary privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("CREATE USER without privilege"):
        create_user_name = f"create_user_{getuid()}"

        try:
            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't create a user"):
                node.query(
                    f"CREATE USER {create_user_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {create_user_name}")

    with Scenario("CREATE USER with privilege"):
        create_user_name = f"create_user_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can create a user"):
                node.query(
                    f"CREATE USER {create_user_name}",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {create_user_name}")

    with Scenario("CREATE USER on cluster"):
        create_user_name = f"create_user_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can create a user"):
                node.query(
                    f"CREATE USER {create_user_name} ON CLUSTER sharded_cluster",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the user"):
                node.query(
                    f"DROP USER IF EXISTS {create_user_name} ON CLUSTER sharded_cluster"
                )

    with Scenario("CREATE USER with revoked privilege"):
        create_user_name = f"create_user_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user can't create a user"):
                node.query(
                    f"CREATE USER {create_user_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {create_user_name}")


@TestSuite
def default_role_granted_directly(self, node=None):
    """Check that a user is able to execute `CREATE USER` with `DEFAULT ROLE` with privileges are granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(test=default_role)(grant_target_name=user_name, user_name=user_name)


@TestSuite
def default_role_granted_via_role(self, node=None):
    """Check that a user is able to execute `CREATE USER` with `DEFAULT ROLE` with privileges are granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(test=default_role)(grant_target_name=role_name, user_name=user_name)


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_CreateUser_DefaultRole("1.0"),
)
def default_role(self, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `CREATE USER` with `DEFAULT ROLE` if and only if the user has
    `CREATE USER` privilege and the role with `ADMIN OPTION`.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("CREATE USER with DEFAULT ROLE without privilege"):
        create_user_name = f"create_user_{getuid()}"
        default_role_name = f"default_role_{getuid()}"

        with role(node, default_role_name):
            try:
                with When(f"I grant CREATE USER"):
                    node.query(f"GRANT CREATE USER ON *.* TO {grant_target_name}")

                with Then("I check the user can't create a user"):
                    node.query(
                        f"CREATE USER {create_user_name} DEFAULT ROLE {default_role_name}",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )

            finally:
                with Finally("I drop the user"):
                    node.query(f"DROP USER IF EXISTS {create_user_name}")

    with Scenario("CREATE USER with DEFAULT ROLE with role privilege"):
        create_user_name = f"create_user_{getuid()}"
        default_role_name = f"default_role_{getuid()}"

        with role(node, default_role_name):
            try:
                with When(f"I grant CREATE USER"):
                    node.query(f"GRANT CREATE USER ON *.* TO {grant_target_name}")

                with And(f"I grant the role with ADMIN OPTION"):
                    node.query(
                        f"GRANT {default_role_name} TO {grant_target_name} WITH ADMIN OPTION"
                    )

                with Then("I check the user can create a user"):
                    node.query(
                        f"CREATE USER {create_user_name} DEFAULT ROLE {default_role_name}",
                        settings=[("user", user_name)],
                    )

            finally:
                with Finally("I drop the user"):
                    node.query(f"DROP USER IF EXISTS {create_user_name}")

    with Scenario("CREATE USER with DEFAULT ROLE on cluster"):
        create_user_name = f"create_user_{getuid()}"
        default_role_name = f"default_role_{getuid()}"

        try:
            with Given("I have role on a cluster"):
                node.query(
                    f"CREATE ROLE {default_role_name} ON CLUSTER sharded_cluster"
                )

            with When(f"I grant CREATE USER"):
                node.query(f"GRANT CREATE USER ON *.* TO {grant_target_name}")

            with And(f"I grant the role with ADMIN OPTION"):
                node.query(
                    f"GRANT {default_role_name} TO {grant_target_name} WITH ADMIN OPTION"
                )

            with Then("I check the user can create a user"):
                node.query(
                    f"CREATE USER {create_user_name} ON CLUSTER sharded_cluster DEFAULT ROLE {default_role_name}",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the user"):
                node.query(
                    f"DROP USER IF EXISTS {create_user_name} ON CLUSTER sharded_cluster"
                )

            with And("I drop the role from the cluster"):
                node.query(f"DROP ROLE {default_role_name} ON CLUSTER sharded_cluster")

    with Scenario("CREATE USER with DEFAULT ROLE with revoked role privilege"):
        create_user_name = f"create_user_{getuid()}"
        default_role_name = f"default_role_{getuid()}"

        with role(node, default_role_name):
            try:
                with When(f"I grant CREATE USER"):
                    node.query(f"GRANT CREATE USER ON *.* TO {grant_target_name}")

                with And(f"I grant the role with ADMIN OPTION"):
                    node.query(
                        f"GRANT {default_role_name} TO {grant_target_name} WITH ADMIN OPTION"
                    )

                with And(f"I revoke the role"):
                    node.query(f"REVOKE {default_role_name} FROM {grant_target_name}")

                with Then("I check the user can't create a user"):
                    node.query(
                        f"CREATE USER {create_user_name} DEFAULT ROLE {default_role_name}",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )

            finally:
                with Finally("I drop the user"):
                    node.query(f"DROP USER IF EXISTS {create_user_name}")

    with Scenario("CREATE USER with DEFAULT ROLE with ACCESS MANAGEMENT privilege"):
        create_user_name = f"create_user_{getuid()}"
        default_role_name = f"default_role_{getuid()}"

        with role(node, default_role_name):
            try:
                with When(f"I grant ACCESS MANAGEMENT "):
                    node.query(
                        f"GRANT ACCESS MANAGEMENT  ON *.* TO {grant_target_name}"
                    )

                with Then("I check the user can create a user"):
                    node.query(
                        f"CREATE USER {create_user_name} DEFAULT ROLE {default_role_name}",
                        settings=[("user", user_name)],
                    )

            finally:
                with Finally("I drop the user"):
                    node.query(f"DROP USER IF EXISTS {create_user_name}")


@TestFeature
@Name("create user")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_CreateUser("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of CREATE USER."""
    self.context.node = self.context.cluster.node(node)

    Suite(run=create_user_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=create_user_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=default_role_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=default_role_granted_via_role, setup=instrument_clickhouse_server_log)
