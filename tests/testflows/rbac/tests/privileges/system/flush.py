from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM FLUSH LOGS` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(
            run=flush_logs,
            examples=Examples(
                "privilege on grant_target_name user_name",
                [
                    tuple(list(row) + [user_name, user_name])
                    for row in flush_logs.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM FLUSH LOGS` commands if and only if
    the privilege has been granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=flush_logs,
            examples=Examples(
                "privilege on grant_target_name user_name",
                [
                    tuple(list(row) + [role_name, user_name])
                    for row in flush_logs.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege on",
    [
        ("ALL", "*.*"),
        ("SYSTEM", "*.*"),
        ("SYSTEM FLUSH", "*.*"),
        ("SYSTEM FLUSH LOGS", "*.*"),
        ("FLUSH LOGS", "*.*"),
    ],
)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Flush_Logs("1.0"),
)
def flush_logs(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM START REPLICATED FLUSH` when they have privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM FLUSH LOGS without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't flush logs"):
            node.query(
                f"SYSTEM FLUSH LOGS",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    with Scenario("SYSTEM FLUSH LOGS with privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

        with Then("I check the user can flush logs"):
            node.query(f"SYSTEM FLUSH LOGS", settings=[("user", f"{user_name}")])

    with Scenario("SYSTEM FLUSH LOGS with revoked privilege"):

        with When(f"I grant {privilege} on the table"):
            node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

        with And(f"I revoke {privilege} on the table"):
            node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

        with Then("I check the user can't flush logs"):
            node.query(
                f"SYSTEM FLUSH LOGS",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestSuite
def distributed_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM FLUSH DISTRIBUTED` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        table_name = f"table_name_{getuid()}"

        Suite(
            run=flush_distributed,
            examples=Examples(
                "privilege on grant_target_name user_name table_name",
                [
                    tuple(list(row) + [user_name, user_name, table_name])
                    for row in flush_distributed.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestSuite
def distributed_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM FLUSH DISTRIBUTED` commands if and only if
    the privilege has been granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        table_name = f"table_name_{getuid()}"

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(
            run=flush_distributed,
            examples=Examples(
                "privilege on grant_target_name user_name table_name",
                [
                    tuple(list(row) + [role_name, user_name, table_name])
                    for row in flush_distributed.examples
                ],
                args=Args(name="check privilege={privilege}", format_name=True),
            ),
        )


@TestOutline(Suite)
@Examples(
    "privilege on",
    [
        ("ALL", "*.*"),
        ("SYSTEM", "*.*"),
        ("SYSTEM FLUSH", "*.*"),
        ("SYSTEM FLUSH DISTRIBUTED", "table"),
        ("FLUSH DISTRIBUTED", "table"),
    ],
)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Flush_Distributed("1.0"),
)
def flush_distributed(
    self, privilege, on, grant_target_name, user_name, table_name, node=None
):
    """Check that user is only able to execute `SYSTEM FLUSH DISTRIBUTED` when they have privilege."""
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table0_name = f"table0_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table0_name):
        try:
            with Given("I have a distributed table"):
                node.query(
                    f"CREATE TABLE {table_name} (a UInt64) ENGINE = Distributed(sharded_cluster, default, {table0_name}, rand())"
                )

            with Scenario("SYSTEM FLUSH DISTRIBUTED without privilege"):

                with When("I grant the user NONE privilege"):
                    node.query(f"GRANT NONE TO {grant_target_name}")

                with And("I grant the user USAGE privilege"):
                    node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

                with Then("I check the user can't flush distributed"):
                    node.query(
                        f"SYSTEM FLUSH DISTRIBUTED {table_name}",
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )

            with Scenario("SYSTEM FLUSH DISTRIBUTED with privilege"):

                with When(f"I grant {privilege} on the table"):
                    node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

                with Then("I check the user can flush distributed"):
                    node.query(
                        f"SYSTEM FLUSH DISTRIBUTED {table_name}",
                        settings=[("user", f"{user_name}")],
                    )

            with Scenario("SYSTEM FLUSH DISTRIBUTED with revoked privilege"):

                with When(f"I grant {privilege} on the table"):
                    node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

                with And(f"I revoke {privilege} on the table"):
                    node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

                with Then("I check the user can't flush distributed"):
                    node.query(
                        f"SYSTEM FLUSH DISTRIBUTED {table_name}",
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )

        finally:
            with Finally("I drop the distributed table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestFeature
@Name("system flush")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Flush("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM FLUSH."""
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(
        run=distributed_privileges_granted_directly,
        setup=instrument_clickhouse_server_log,
    )
    Suite(
        run=distributed_privileges_granted_via_role,
        setup=instrument_clickhouse_server_log,
    )
