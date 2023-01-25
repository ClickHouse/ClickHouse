from contextlib import contextmanager
import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestScenario
@Requirements(RQ_SRS_006_RBAC_Privileges_None("1.0"))
def without_privilege(self, table_type, node=None):
    """Check that user without select privilege on a table is not able to select on that table."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {user_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {user_name}")

            with Then("I run SELECT without privilege"):
                exitcode, message = errors.not_enough_privileges(name=user_name)

                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
)
def user_with_privilege(self, table_type, node=None):
    """Check that user can select from a table on which they have select privilege."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")

        with user(node, user_name):

            with When("I grant privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {user_name}")

            with Then("I verify SELECT command"):
                user_select = node.query(
                    f"SELECT d FROM {table_name}", settings=[("user", user_name)]
                )

                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()


@TestScenario
@Requirements(RQ_SRS_006_RBAC_Privileges_All("1.0"))
def user_with_all_privilege(self, table_type, node=None):
    """Check that user can select from a table if have ALL privilege."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")

        with user(node, user_name):

            with When("I grant privilege"):
                node.query(f"GRANT ALL ON *.* TO {user_name}")

            with Then("I verify SELECT command"):
                user_select = node.query(
                    f"SELECT d FROM {table_name}", settings=[("user", user_name)]
                )

                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"),
)
def user_with_revoked_privilege(self, table_type, node=None):
    """Check that user is unable to select from a table after select privilege
    on that table has been revoked from the user.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name):

            with When("I grant privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {user_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE SELECT ON {table_name} FROM {user_name}")

            with Then("I use SELECT, throws exception"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def user_with_revoked_all_privilege(self, table_type, node=None):
    """Check that user is unable to select from a table after ALL privilege
    on that table has been revoked from the user.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name):

            with When("I grant privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {user_name}")

            with And("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {user_name}")

            with Then("I use SELECT, throws exception"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def user_with_privilege_on_columns(self, table_type):
    Scenario(
        run=user_column_privileges,
        examples=Examples(
            "grant_columns revoke_columns select_columns_fail select_columns_pass data_pass table_type",
            [
                tuple(list(row) + [table_type])
                for row in user_column_privileges.examples
            ],
        ),
    )


@TestOutline
@Requirements(
    RQ_SRS_006_RBAC_Select_Column("1.0"),
)
@Examples(
    "grant_columns revoke_columns select_columns_fail select_columns_pass data_pass",
    [
        ("d", "d", "x", "d", "'2020-01-01'"),
        ("d,a", "d", "x", "d", "'2020-01-01'"),
        ("d,a,b", "d,a,b", "x", "d,b", "'2020-01-01',9"),
        ("d,a,b", "b", "y", "d,a,b", "'2020-01-01','woo',9"),
    ],
)
def user_column_privileges(
    self,
    grant_columns,
    select_columns_pass,
    data_pass,
    table_type,
    revoke_columns=None,
    select_columns_fail=None,
    node=None,
):
    """Check that user is able to select on granted columns
    and unable to select on not granted or revoked columns.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type), user(node, user_name):

        with Given("The table has some data on some columns"):
            node.query(
                f"INSERT INTO {table_name} ({select_columns_pass}) VALUES ({data_pass})"
            )

        with When("I grant select privilege"):
            node.query(f"GRANT SELECT({grant_columns}) ON {table_name} TO {user_name}")

        if select_columns_fail is not None:

            with And("I select from not granted column"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"SELECT ({select_columns_fail}) FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

        with Then("I select from granted column, verify correct result"):
            user_select = node.query(
                f"SELECT ({select_columns_pass}) FROM {table_name}",
                settings=[("user", user_name)],
            )
            default = node.query(f"SELECT ({select_columns_pass}) FROM {table_name}")
            assert user_select.output == default.output

        if revoke_columns is not None:

            with When("I revoke select privilege for columns from user"):
                node.query(
                    f"REVOKE SELECT({revoke_columns}) ON {table_name} FROM {user_name}"
                )

            with And("I select from revoked columns"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"SELECT ({select_columns_pass}) FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
)
def role_with_privilege(self, table_type, node=None):
    """Check that user can select from a table after it is granted a role that
    has the select privilege for that table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")

        with user(node, user_name):

            with role(node, role_name):

                with When("I grant select privilege to a role"):
                    node.query(f"GRANT SELECT ON {table_name} TO {role_name}")

                with And("I grant role to the user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with Then("I verify SELECT command"):
                    user_select = node.query(
                        f"SELECT d FROM {table_name}", settings=[("user", user_name)]
                    )
                    default = node.query(f"SELECT d FROM {table_name}")
                    assert user_select.output == default.output, error()


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"),
)
def role_with_revoked_privilege(self, table_type, node=None):
    """Check that user with a role that has select privilege on a table is unable
    to select from that table after select privilege has been revoked from the role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name), role(node, role_name):

            with When("I grant privilege to a role"):
                node.query(f"GRANT SELECT ON {table_name} TO {role_name}")

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And("I revoke privilege from the role"):
                node.query(f"REVOKE SELECT ON {table_name} FROM {role_name}")

            with And("I select from the table"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has select privilege on a table is unable to
    select from that table after the role with select privilege has been revoked from the user.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name), role(node, role_name):

            with When("I grant privilege to a role"):
                node.query(f"GRANT SELECT ON {table_name} TO {role_name}")

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And("I revoke the role from the user"):
                node.query(f"REVOKE {role_name} FROM {user_name}")

            with And("I select from the table"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def role_with_privilege_on_columns(self, table_type):
    Scenario(
        run=role_column_privileges,
        examples=Examples(
            "grant_columns revoke_columns select_columns_fail select_columns_pass data_pass table_type",
            [
                tuple(list(row) + [table_type])
                for row in role_column_privileges.examples
            ],
        ),
    )


@TestOutline
@Requirements(
    RQ_SRS_006_RBAC_Select_Column("1.0"),
)
@Examples(
    "grant_columns revoke_columns select_columns_fail select_columns_pass data_pass",
    [
        ("d", "d", "x", "d", "'2020-01-01'"),
        ("d,a", "d", "x", "d", "'2020-01-01'"),
        ("d,a,b", "d,a,b", "x", "d,b", "'2020-01-01',9"),
        ("d,a,b", "b", "y", "d,a,b", "'2020-01-01','woo',9"),
    ],
)
def role_column_privileges(
    self,
    grant_columns,
    select_columns_pass,
    data_pass,
    table_type,
    revoke_columns=None,
    select_columns_fail=None,
    node=None,
):
    """Check that user is able to select from granted columns and unable
    to select from not granted or revoked columns.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with Given("The table has some data on some columns"):
            node.query(
                f"INSERT INTO {table_name} ({select_columns_pass}) VALUES ({data_pass})"
            )

        with user(node, user_name), role(node, role_name):

            with When("I grant select privilege"):
                node.query(
                    f"GRANT SELECT({grant_columns}) ON {table_name} TO {role_name}"
                )

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            if select_columns_fail is not None:
                with And("I select from not granted column"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)
                    node.query(
                        f"SELECT ({select_columns_fail}) FROM {table_name}",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )

            with Then("I verify SELECT command"):
                user_select = node.query(
                    f"SELECT d FROM {table_name}", settings=[("user", user_name)]
                )
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

            if revoke_columns is not None:

                with When("I revoke select privilege for columns from role"):
                    node.query(
                        f"REVOKE SELECT({revoke_columns}) ON {table_name} FROM {role_name}"
                    )

                with And("I select from revoked columns"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)
                    node.query(
                        f"SELECT ({select_columns_pass}) FROM {table_name}",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Select_Cluster("1.0"),
)
def user_with_privilege_on_cluster(self, table_type, node=None):
    """Check that user is able to select from a table with
    privilege granted on a cluster.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):
        try:
            with Given("I have some data inserted into table"):
                node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")

            with Given("I have a user on a cluster"):
                node.query(
                    f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster"
                )

            with When("I grant select privilege on a cluster"):
                node.query(
                    f"GRANT ON CLUSTER sharded_cluster SELECT ON {table_name} TO {user_name}"
                )

            with Then("I verify SELECT command"):
                user_select = node.query(
                    f"SELECT d FROM {table_name}", settings=[("user", user_name)]
                )
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")


@TestOutline(Feature)
@Requirements(RQ_SRS_006_RBAC_Select("1.0"), RQ_SRS_006_RBAC_Select_TableEngines("1.0"))
@Examples("table_type", [(key,) for key in table_types.keys()])
@Name("select")
def feature(self, table_type, stress=None, node="clickhouse1"):
    """Check the RBAC functionality of SELECT."""
    self.context.node = self.context.cluster.node(node)

    if stress is not None:
        self.context.stress = stress

    args = {"table_type": table_type}

    with Pool(10) as pool:
        try:
            for scenario in loads(current_module(), Scenario):
                Scenario(
                    test=scenario,
                    setup=instrument_clickhouse_server_log,
                    parallel=True,
                    executor=pool,
                )(**args)
        finally:
            join()
