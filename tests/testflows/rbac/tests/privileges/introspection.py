from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@contextmanager
def allow_introspection_functions(node):
    setting = ("allow_introspection_functions", 1)
    default_query_settings = None

    try:
        with Given("I add allow_introspection_functions to the default query settings"):
            default_query_settings = getsattr(
                current().context, "default_query_settings", []
            )
            default_query_settings.append(setting)
        yield
    finally:
        with Finally(
            "I remove allow_introspection_functions from the default query settings"
        ):
            if default_query_settings:
                try:
                    default_query_settings.pop(default_query_settings.index(setting))
                except ValueError:
                    pass


@TestSuite
def addressToLine_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `addressToLine` with privileges are granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=addressToLine,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in addressToLine.examples
                ],
                args=Args(name="privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def addressToLine_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `addressToLine` with privileges are granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=addressToLine,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in addressToLine.examples
                ],
                args=Args(name="privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("INTROSPECTION",),
        ("INTROSPECTION FUNCTIONS",),
        ("addressToLine",),
    ],
)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Introspection_addressToLine("1.0"),
)
def addressToLine(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `addressToLine` when they have the necessary privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("addressToLine without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use addressToLine"):
            node.query(
                f"WITH addressToLine(toUInt64(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", user_name)],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("addressToLine with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use addressToLine"):
            node.query(
                f"WITH addressToLine(toUInt64(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", f"{user_name}")],
            )

    with Scenario("addressToLine with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use addressToLine"):
            node.query(
                f"WITH addressToLine(toUInt64(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", user_name)],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def addressToSymbol_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `addressToSymbol` with privileges are granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=addressToSymbol,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in addressToSymbol.examples
                ],
                args=Args(name="privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def addressToSymbol_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `addressToSymbol` with privileges are granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=addressToSymbol,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in addressToSymbol.examples
                ],
                args=Args(name="privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("INTROSPECTION",),
        ("INTROSPECTION FUNCTIONS",),
        ("addressToSymbol",),
    ],
)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Introspection_addressToSymbol("1.0"),
)
def addressToSymbol(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `addressToSymbol` when they have the necessary privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("addressToSymbol without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use addressToSymbol"):
            node.query(
                f"WITH addressToSymbol(toUInt64(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", user_name)],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("addressToSymbol with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use addressToSymbol"):
            node.query(
                f"WITH addressToSymbol(toUInt64(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", f"{user_name}")],
            )

    with Scenario("addressToSymbol with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use addressToSymbol"):
            node.query(
                f"WITH addressToSymbol(toUInt64(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", user_name)],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def demangle_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `demangle` with privileges are granted directly."""

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=demangle,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in demangle.examples
                ],
                args=Args(name="privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def demangle_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `demangle` with privileges are granted through a role."""

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=demangle,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in demangle.examples
                ],
                args=Args(name="privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("INTROSPECTION",),
        ("INTROSPECTION FUNCTIONS",),
        ("demangle",),
    ],
)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Introspection_demangle("1.0"),
)
def demangle(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `demangle` when they have the necessary privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("demangle without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use demangle"):
            node.query(
                f"WITH demangle(toString(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", user_name)],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("demangle with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use demangle"):
            node.query(
                f"WITH demangle(toString(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", f"{user_name}")],
            )

    with Scenario("demangle with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use demangle"):
            node.query(
                f"WITH demangle(toString(dummy)) AS addr SELECT 1 WHERE addr = ''",
                settings=[("user", user_name)],
                exitcode=exitcode,
                message=message,
            )


@TestFeature
@Name("introspection")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Introspection("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of INTROSPECTION."""
    self.context.node = self.context.cluster.node(node)

    with allow_introspection_functions(self.context.node):
        Suite(
            run=addressToLine_privileges_granted_directly,
            setup=instrument_clickhouse_server_log,
        )
        Suite(
            run=addressToLine_privileges_granted_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Suite(
            run=addressToSymbol_privileges_granted_directly,
            setup=instrument_clickhouse_server_log,
        )
        Suite(
            run=addressToSymbol_privileges_granted_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Suite(
            run=demangle_privileges_granted_directly,
            setup=instrument_clickhouse_server_log,
        )
        Suite(
            run=demangle_privileges_granted_via_role,
            setup=instrument_clickhouse_server_log,
        )
