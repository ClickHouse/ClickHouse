from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def dict_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `USE` and `SHOW CREATE`
    commands on a database and see the database when they execute `SHOW DATABASES` command
    if and only if they have any privilege on that database granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        db_name = f"db_name_{getuid()}"

        Suite(
            run=check_privilege,
            examples=Examples(
                "privilege on grant_target_name user_name db_name",
                [
                    tuple(list(row) + [user_name, user_name, db_name])
                    for row in check_privilege.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def dict_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `USE` and `SHOW CREATE`
    commands on a database and see the database when they execute `SHOW DATABASES` command
    if and only if they have any privilege on that database granted via role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        db_name = f"db_name_{getuid()}"

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=check_privilege,
            examples=Examples(
                "privilege on grant_target_name user_name db_name",
                [
                    tuple(list(row) + [role_name, user_name, db_name])
                    for row in check_privilege.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege on",
    [
        ("ALL", "*.*"),
        ("SHOW", "*.*"),
        ("SHOW DATABASES", "db"),
        ("CREATE DATABASE", "db"),
        ("DROP DATABASE", "db"),
    ],
)
def check_privilege(
    self, privilege, on, grant_target_name, user_name, db_name, node=None
):
    """Run checks for commands that require SHOW DATABASE privilege."""

    if node is None:
        node = self.context.node

    on = on.replace("db", f"{db_name}")

    Suite(test=show_db)(
        privilege=privilege,
        on=on,
        grant_target_name=grant_target_name,
        user_name=user_name,
        db_name=db_name,
    )
    Suite(test=use)(
        privilege=privilege,
        on=on,
        grant_target_name=grant_target_name,
        user_name=user_name,
        db_name=db_name,
    )
    Suite(test=show_create)(
        privilege=privilege,
        on=on,
        grant_target_name=grant_target_name,
        user_name=user_name,
        db_name=db_name,
    )


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowDatabases_RequiredPrivilege("1.0"),
)
def show_db(self, privilege, on, grant_target_name, user_name, db_name, node=None):
    """Check that user is only able to see a database in SHOW DATABASES when they have a privilege on that database."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    try:
        with Given("I have a database"):

            node.query(f"CREATE DATABASE {db_name}")

        with Scenario("SHOW DATABASES without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user doesn't see the database"):
                output = node.query(
                    "SHOW DATABASES", settings=[("user", f"{user_name}")]
                ).output
                assert output == "", error()

        with Scenario("SHOW DATABASES with privilege"):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON {db_name}.* TO {grant_target_name}")

            with Then("I check the user does see a database"):
                output = node.query(
                    "SHOW DATABASES",
                    settings=[("user", f"{user_name}")],
                    message=f"{db_name}",
                )

        with Scenario("SHOW DATABASES with revoked privilege"):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON {db_name}.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the database"):
                node.query(
                    f"REVOKE {privilege} ON {db_name}.* FROM {grant_target_name}"
                )

            with Then("I check the user does not see a database"):
                output = node.query(
                    "SHOW DATABASES", settings=[("user", f"{user_name}")]
                ).output
                assert output == f"", error()

    finally:
        with Finally("I drop the database"):
            node.query(f"DROP DATABASE IF EXISTS {db_name}")


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_UseDatabase_RequiredPrivilege("1.0"),
)
def use(self, privilege, on, grant_target_name, user_name, db_name, node=None):
    """Check that user is able to execute EXISTS on a database if and only if the user has SHOW DATABASE privilege
    on that database.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    try:
        with Given("I have a database"):
            node.query(f"CREATE DATABASE {db_name}")

        with Scenario("USE without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then(f"I attempt to USE {db_name}"):
                node.query(
                    f"USE {db_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

        with Scenario("USE with privilege"):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON {db_name}.* TO {grant_target_name}")

            with Then(f"I attempt to USE {db_name}"):
                node.query(f"USE {db_name}", settings=[("user", user_name)])

        with Scenario("USE with revoked privilege"):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON {db_name}.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the database"):
                node.query(
                    f"REVOKE {privilege} ON {db_name}.* FROM {grant_target_name}"
                )

            with Then(f"I attempt to USE {db_name}"):
                node.query(
                    f"USE {db_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    finally:
        with Finally("I drop the database"):
            node.query(f"DROP DATABASE IF EXISTS {db_name}")


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowCreateDatabase_RequiredPrivilege("1.0"),
)
def show_create(self, privilege, on, grant_target_name, user_name, db_name, node=None):
    """Check that user is able to execute EXISTS on a database if and only if the user has SHOW DATABASE privilege
    on that database.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    try:
        with Given("I have a database"):
            node.query(f"CREATE DATABASE {db_name}")

        with Scenario("SHOW CREATE without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then(f"I attempt to SHOW CREATE {db_name}"):
                node.query(
                    f"SHOW CREATE DATABASE {db_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

        with Scenario("SHOW CREATE with privilege"):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON {db_name}.* TO {grant_target_name}")

            with Then(f"I attempt to SHOW CREATE {db_name}"):
                node.query(
                    f"SHOW CREATE DATABASE {db_name}", settings=[("user", user_name)]
                )

        with Scenario("SHOW CREATE with revoked privilege"):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON {db_name}.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the database"):
                node.query(
                    f"REVOKE {privilege} ON {db_name}.* FROM {grant_target_name}"
                )

            with Then(f"I attempt to SHOW CREATE {db_name}"):
                node.query(
                    f"SHOW CREATE DATABASE {db_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    finally:
        with Finally("I drop the database"):
            node.query(f"DROP DATABASE IF EXISTS {db_name}")


@TestFeature
@Name("show databases")
@Requirements(
    RQ_SRS_006_RBAC_ShowDatabases_Privilege("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW DATABASES."""
    self.context.node = self.context.cluster.node(node)

    Suite(run=dict_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=dict_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
