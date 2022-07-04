from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@contextmanager
def dict_setup(node, table_name, dict_name):
    """Setup and teardown of table and dictionary needed for the tests."""

    try:
        with Given("I have a table"):
            node.query(
                f"CREATE TABLE {table_name} (key UInt64, val UInt64) Engine=Memory()"
            )

        with And("I have a dictionary"):
            node.query(
                f"CREATE DICTIONARY {dict_name} (key UInt64 DEFAULT 0, val UInt64 DEFAULT 10) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE '{table_name}' PASSWORD '' DB 'default')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"
            )

        yield

    finally:
        with Finally("I drop the dictionary", flags=TE):
            node.query(f"DROP DICTIONARY IF EXISTS default.{dict_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestSuite
def config_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD CONFIG` if and only if
    they have `SYSTEM RELOAD CONFIG` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=config,
            examples=Examples(
                "privilege grant_target_name user_name",
                [tuple(list(row) + [user_name, user_name]) for row in config.examples],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def config_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD CONFIG` if and only if
    they have `SYSTEM RELOAD CONFIG` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=config,
            examples=Examples(
                "privilege grant_target_name user_name",
                [tuple(list(row) + [role_name, user_name]) for row in config.examples],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Reload_Config("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM RELOAD",),
        ("SYSTEM RELOAD CONFIG",),
        ("RELOAD CONFIG",),
    ],
)
def config(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM RELOAD CONFIG` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM RELOAD CONFIG without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM RELOAD CONFIG"):
            node.query(
                "SYSTEM RELOAD CONFIG",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM RELOAD CONFIG with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user is bale to execute SYSTEM RELOAD CONFIG"):
            node.query("SYSTEM RELOAD CONFIG", settings=[("user", f"{user_name}")])

    with Scenario("SYSTEM RELOAD CONFIG with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM RELOAD CONFIG"):
            node.query(
                "SYSTEM RELOAD CONFIG",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def dictionary_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD DICTIONARY` if and only if
    they have `SYSTEM RELOAD DICTIONARY` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=dictionary,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in dictionary.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def dictionary_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD DICTIONARY` if and only if
    they have `SYSTEM RELOAD DICTIONARY` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=dictionary,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in dictionary.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Reload_Dictionary("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM RELOAD",),
        ("SYSTEM RELOAD DICTIONARIES",),
        ("RELOAD DICTIONARIES",),
        ("RELOAD DICTIONARY",),
    ],
)
def dictionary(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM RELOAD DICTIONARY` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM RELOAD DICTIONARY without privilege"):

        dict_name = f"dict_{getuid()}"
        table_name = f"table_{getuid()}"

        with dict_setup(node, table_name, dict_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user is unable to execute SYSTEM RELOAD DICTIONARY"):
                node.query(
                    f"SYSTEM RELOAD DICTIONARY default.{dict_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario("SYSTEM RELOAD DICTIONARY with privilege"):

        dict_name = f"dict_{getuid()}"
        table_name = f"table_{getuid()}"

        with dict_setup(node, table_name, dict_name):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user is bale to execute SYSTEM RELOAD DICTIONARY"):
                node.query(
                    f"SYSTEM RELOAD DICTIONARY default.{dict_name}",
                    settings=[("user", f"{user_name}")],
                )

    with Scenario("SYSTEM RELOAD DICTIONARY with revoked privilege"):

        dict_name = f"dict_{getuid()}"
        table_name = f"table_{getuid()}"

        with dict_setup(node, table_name, dict_name):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user is unable to execute SYSTEM RELOAD DICTIONARY"):
                node.query(
                    f"SYSTEM RELOAD DICTIONARY default.{dict_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )


@TestSuite
def dictionaries_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD DICTIONARIES` if and only if
    they have `SYSTEM RELOAD DICTIONARIES` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=dictionaries,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in dictionaries.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def dictionaries_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD DICTIONARIES` if and only if
    they have `SYSTEM RELOAD DICTIONARIES` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=dictionaries,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in dictionaries.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Reload_Dictionaries("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM RELOAD",),
        ("SYSTEM RELOAD DICTIONARIES",),
        ("RELOAD DICTIONARIES",),
        ("RELOAD DICTIONARY",),
    ],
)
def dictionaries(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM RELOAD DICTIONARIES` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM RELOAD DICTIONARIES without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM RELOAD DICTIONARIES"):
            node.query(
                "SYSTEM RELOAD DICTIONARIES",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM RELOAD DICTIONARIES with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user is bale to execute SYSTEM RELOAD DICTIONARIES"):
            node.query(
                "SYSTEM RELOAD DICTIONARIES", settings=[("user", f"{user_name}")]
            )

    with Scenario("SYSTEM RELOAD DICTIONARIES with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user is unable to execute SYSTEM RELOAD DICTIONARIES"):
            node.query(
                "SYSTEM RELOAD DICTIONARIES",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def embedded_dictionaries_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD EMBEDDED DICTIONARIES` if and only if
    they have `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilege granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=embedded_dictionaries,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in embedded_dictionaries.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def embedded_dictionaries_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM RELOAD EMBEDDED DICTIONARIES` if and only if
    they have `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilege granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=embedded_dictionaries,
            examples=Examples(
                "privilege grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in embedded_dictionaries.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Reload_EmbeddedDictionaries("1.0"),
)
@Examples(
    "privilege",
    [
        ("ALL",),
        ("SYSTEM",),
        ("SYSTEM RELOAD",),
        ("SYSTEM RELOAD EMBEDDED DICTIONARIES",),
        ("SYSTEM RELOAD DICTIONARY",),
    ],
)
def embedded_dictionaries(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM RELOAD EMBEDDED DICTIONARIES without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then(
            "I check the user is unable to execute SYSTEM RELOAD EMBEDDED DICTIONARIES"
        ):
            node.query(
                "SYSTEM RELOAD EMBEDDED DICTIONARIES",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM RELOAD EMBEDDED DICTIONARIES with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then(
            "I check the user is bale to execute SYSTEM RELOAD EMBEDDED DICTIONARIES"
        ):
            node.query(
                "SYSTEM RELOAD EMBEDDED DICTIONARIES",
                settings=[("user", f"{user_name}")],
            )

    with Scenario("SYSTEM RELOAD EMBEDDED DICTIONARIES with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then(
            "I check the user is unable to execute SYSTEM RELOAD EMBEDDED DICTIONARIES"
        ):
            node.query(
                "SYSTEM RELOAD EMBEDDED DICTIONARIES",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestFeature
@Name("system reload")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Reload("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM RELOAD."""
    self.context.node = self.context.cluster.node(node)

    Suite(
        run=config_privileges_granted_directly, setup=instrument_clickhouse_server_log
    )
    Suite(
        run=config_privileges_granted_via_role, setup=instrument_clickhouse_server_log
    )
    Suite(
        run=dictionary_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=dictionary_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=dictionaries_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=dictionaries_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=embedded_dictionaries_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=embedded_dictionaries_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
