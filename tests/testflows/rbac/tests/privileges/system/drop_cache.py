from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def dns_cache_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM DROP DNS CACHE` if and only if
    they have `SYSTEM DROP DNS CACHE` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=dns_cache,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in dns_cache.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def dns_cache_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM DROP DNS CACHE` if and only if
    they have `SYSTEM DROP DNS CACHE` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=dns_cache,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in dns_cache.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_DropCache_DNS("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM DROP CACHE",),
        ("SYSTEM DROP DNS CACHE",),
        ("DROP CACHE",),
        ("DROP DNS CACHE",),
        ("SYSTEM DROP DNS",),
        ("DROP DNS",),
    ],
)
def dns_cache(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM DROP DNS CACHE` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM DROP DNS CACHE without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM DROP DNS CACHE"):
            node.query(
                "SYSTEM DROP DNS CACHE",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM DROP DNS CACHE with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user is bale to execute SYSTEM DROP DNS CACHE"):
            node.query("SYSTEM DROP DNS CACHE", settings=[("user", f"{user_name}")])

    with Scenario("SYSTEM DROP DNS CACHE with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM DROP DNS CACHE"):
            node.query(
                "SYSTEM DROP DNS CACHE",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def mark_cache_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM DROP MARK CACHE` if and only if
    they have `SYSTEM DROP MARK CACHE` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=mark_cache,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in mark_cache.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def mark_cache_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM DROP MARK CACHE` if and only if
    they have `SYSTEM DROP MARK CACHE` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=mark_cache,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in mark_cache.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_DropCache_Mark("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM DROP CACHE",),
        ("SYSTEM DROP MARK CACHE",),
        ("DROP CACHE",),
        ("DROP MARK CACHE",),
        ("SYSTEM DROP MARK",),
        ("DROP MARKS",),
    ],
)
def mark_cache(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM DROP MARK CACHE` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM DROP MARK CACHE without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM DROP MARK CACHE"):
            node.query(
                "SYSTEM DROP MARK CACHE",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM DROP MARK CACHE with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user is bale to execute SYSTEM DROP MARK CACHE"):
            node.query("SYSTEM DROP MARK CACHE", settings=[("user", f"{user_name}")])

    with Scenario("SYSTEM DROP MARK CACHE with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM DROP MARK CACHE"):
            node.query(
                "SYSTEM DROP MARK CACHE",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def uncompressed_cache_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM DROP UNCOMPRESSED CACHE` if and only if
    they have `SYSTEM DROP UNCOMPRESSED CACHE` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=uncompressed_cache,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in uncompressed_cache.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def uncompressed_cache_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM DROP UNCOMPRESSED CACHE` if and only if
    they have `SYSTEM DROP UNCOMPRESSED CACHE` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=uncompressed_cache,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in uncompressed_cache.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_DropCache_Uncompressed("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM DROP CACHE",),
        ("SYSTEM DROP UNCOMPRESSED CACHE",),
        ("DROP CACHE",),
        ("DROP UNCOMPRESSED CACHE",),
        ("SYSTEM DROP UNCOMPRESSED",),
        ("DROP UNCOMPRESSED",),
    ],
)
def uncompressed_cache(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM DROP UNCOMPRESSED CACHE` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM DROP UNCOMPRESSED CACHE without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then(
            "I check the user is unable to execute SYSTEM DROP UNCOMPRESSED CACHE"
        ):
            node.query(
                "SYSTEM DROP UNCOMPRESSED CACHE",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM DROP UNCOMPRESSED CACHE with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user is bale to execute SYSTEM DROP UNCOMPRESSED CACHE"):
            node.query(
                "SYSTEM DROP UNCOMPRESSED CACHE", settings=[("user", f"{user_name}")]
            )

    with Scenario("SYSTEM DROP UNCOMPRESSED CACHE with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then(
            "I check the user is unable to execute SYSTEM DROP UNCOMPRESSED CACHE"
        ):
            node.query(
                "SYSTEM DROP UNCOMPRESSED CACHE",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestFeature
@Name("system drop cache")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_DropCache("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM DROP CACHE."""
    self.context.node = self.context.cluster.node(node)

    Suite(
        run=dns_cache_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=dns_cache_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=mark_cache_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=mark_cache_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=uncompressed_cache_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=uncompressed_cache_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
