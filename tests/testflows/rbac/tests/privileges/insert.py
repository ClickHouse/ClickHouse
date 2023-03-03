from contextlib import contextmanager
import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


def input_output_equality_check(node, input_columns, input_data, table_name):
    data_list = [x.strip("'") for x in input_data.split(",")]
    input_dict = dict(zip(input_columns.split(","), data_list))
    output_dict = json.loads(
        node.query(
            f"select {input_columns} from {table_name} format JSONEachRow"
        ).output
    )
    output_dict = {k: str(v) for (k, v) in output_dict.items()}
    return input_dict == output_dict


@TestScenario
@Requirements(RQ_SRS_006_RBAC_Privileges_None("1.0"))
def without_privilege(self, table_type, node=None):
    """Check that user without insert privilege on a table is not able to insert on that table."""
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

            with Then("I run INSERT without privilege"):
                exitcode, message = errors.not_enough_privileges(name=user_name)

                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Grant_Privilege_Insert("1.0"),
)
def user_with_privilege(self, table_type, node=None):
    """Check that user can insert into a table on which they have insert privilege."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):
        with user(node, user_name):

            with When("I grant insert privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {user_name}")

            with And("I use INSERT"):
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                )

            with Then("I check the insert functioned"):
                output = node.query(
                    f"SELECT d FROM {table_name} FORMAT JSONEachRow"
                ).output
                assert output == '{"d":"2020-01-01"}', error()


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Insert("1.0"),
)
def all_privilege(self, table_type, node=None):
    """Check that user can insert into a table on which they have insert privilege."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name):

            with When("I grant insert privilege"):
                node.query(f"GRANT ALL ON *.* TO {user_name}")

            with And("I use INSERT"):
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                )

            with Then("I check the insert functioned"):
                output = node.query(
                    f"SELECT d FROM {table_name} FORMAT JSONEachRow"
                ).output
                assert output == '{"d":"2020-01-01"}', error()


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Revoke_Privilege_Insert("1.0"),
)
def user_with_revoked_privilege(self, table_type, node=None):
    """Check that user is unable to insert into a table after insert privilege on that table has been revoked from user."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):
        with user(node, user_name):

            with When("I grant insert privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {user_name}")

            with And("I revoke insert privilege"):
                node.query(f"REVOKE INSERT ON {table_name} FROM {user_name}")

            with Then("I use INSERT"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def user_with_all_revoked_privilege(self, table_type, node=None):
    """Check that user is unable to insert into a table after ALL privilege has been revoked from user."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):
        with user(node, user_name):

            with When("I grant insert privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {user_name}")

            with And("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {user_name}")

            with Then("I use INSERT"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def user_with_privilege_on_columns(self, table_type):
    Scenario(
        run=user_column_privileges,
        examples=Examples(
            "grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass table_type",
            [
                tuple(list(row) + [table_type])
                for row in user_column_privileges.examples
            ],
        ),
    )


@TestOutline
@Requirements(
    RQ_SRS_006_RBAC_Insert_Column("1.0"),
)
@Examples(
    "grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass",
    [
        ("d", "d", "x", "d", "'woo'", "'2020-01-01'"),
        ("d,a", "d", "x", "d", "'woo'", "'2020-01-01'"),
        ("d,a,b", "d,a,b", "x", "d,b", "'woo'", "'2020-01-01',9"),
        ("d,a,b", "b", "y", "d,a,b", "9", "'2020-01-01','woo',9"),
    ],
)
def user_column_privileges(
    self,
    grant_columns,
    insert_columns_pass,
    data_fail,
    data_pass,
    table_type,
    revoke_columns=None,
    insert_columns_fail=None,
    node=None,
):
    """Check that user is able to insert on columns where insert privilege is granted
    and unable to insert on columns where insert privilege has not been granted or has been revoked.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name):

            with When("I grant insert privilege"):
                node.query(
                    f"GRANT INSERT({grant_columns}) ON {table_name} TO {user_name}"
                )

            if insert_columns_fail is not None:
                with And("I insert into a column without insert privilege"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)
                    node.query(
                        f"INSERT INTO {table_name} ({insert_columns_fail}) VALUES ({data_fail})",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )

            with And("I insert into granted column"):
                node.query(
                    f"INSERT INTO {table_name} ({insert_columns_pass}) VALUES ({data_pass})",
                    settings=[("user", user_name)],
                )

            with Then("I check the insert functioned"):
                input_equals_output = input_output_equality_check(
                    node, insert_columns_pass, data_pass, table_name
                )
                assert input_equals_output, error()

            if revoke_columns is not None:

                with When("I revoke insert privilege from columns"):
                    node.query(
                        f"REVOKE INSERT({revoke_columns}) ON {table_name} FROM {user_name}"
                    )

                with And("I insert into revoked columns"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)
                    node.query(
                        f"INSERT INTO {table_name} ({insert_columns_pass}) VALUES ({data_pass})",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Grant_Privilege_Insert("1.0"),
)
def role_with_privilege(self, table_type, node=None):
    """Check that user can insert into a table after being granted a role that
    has the insert privilege for that table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name), role(node, role_name):

            with When("I grant insert privilege to a role"):
                node.query(f"GRANT INSERT ON {table_name} TO {role_name}")

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And("I insert into the table"):
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                )

            with Then("I check the data matches the input"):
                output = node.query(
                    f"SELECT d FROM {table_name} FORMAT JSONEachRow"
                ).output
                assert output == '{"d":"2020-01-01"}', error()


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Revoke_Privilege_Insert("1.0"),
)
def role_with_revoked_privilege(self, table_type, node=None):
    """Check that user with a role that has insert privilege on a table
    is unable to insert into that table after insert privilege
    has been revoked from the role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name), role(node, role_name):

            with When("I grant privilege to a role"):
                node.query(f"GRANT INSERT ON {table_name} TO {role_name}")

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And("I revoke privilege from the role"):
                node.query(f"REVOKE INSERT ON {table_name} FROM {role_name}")

            with And("I insert into the table"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has insert privilege on a table
    is unable to insert into that table after the role has been revoked from the user.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        with user(node, user_name), role(node, role_name):

            with When("I grant privilege to a role"):
                node.query(f"GRANT INSERT ON {table_name} TO {role_name}")

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And("I revoke the role from the user"):
                node.query(f"REVOKE {role_name} FROM {user_name}")

            with And("I insert into the table"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def role_with_privilege_on_columns(self, table_type):
    Scenario(
        run=role_column_privileges,
        examples=Examples(
            "grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass table_type",
            [
                tuple(list(row) + [table_type])
                for row in role_column_privileges.examples
            ],
        ),
    )


@TestOutline
@Requirements(
    RQ_SRS_006_RBAC_Insert_Column("1.0"),
)
@Examples(
    "grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass",
    [
        ("d", "d", "x", "d", "'woo'", "'2020-01-01'"),
        ("d,a", "d", "x", "d", "'woo'", "'2020-01-01'"),
        ("d,a,b", "d,a,b", "x", "d,b", "'woo'", "'2020-01-01',9"),
        ("d,a,b", "b", "y", "d,a,b", "9", "'2020-01-01','woo',9"),
    ],
)
def role_column_privileges(
    self,
    grant_columns,
    insert_columns_pass,
    data_fail,
    data_pass,
    table_type,
    revoke_columns=None,
    insert_columns_fail=None,
    node=None,
):
    """Check that user with a role is able to insert on columns where insert privilege is granted to the role
    and unable to insert on columns where insert privilege has not been granted or has been revoked from the role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):
        with user(node, user_name), role(node, role_name):

            with When("I grant insert privilege"):
                node.query(
                    f"GRANT INSERT({grant_columns}) ON {table_name} TO {role_name}"
                )

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            if insert_columns_fail is not None:
                with And("I insert into columns without insert privilege"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)

                    node.query(
                        f"INSERT INTO {table_name} ({insert_columns_fail}) VALUES ({data_fail})",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )

            with And("I insert into granted column"):
                node.query(
                    f"INSERT INTO {table_name} ({insert_columns_pass}) VALUES ({data_pass})",
                    settings=[("user", user_name)],
                )

            with Then("I check the insert functioned"):
                input_equals_output = input_output_equality_check(
                    node, insert_columns_pass, data_pass, table_name
                )
                assert input_equals_output, error()

            if revoke_columns is not None:
                with When("I revoke insert privilege from columns"):
                    node.query(
                        f"REVOKE INSERT({revoke_columns}) ON {table_name} FROM {role_name}"
                    )

                with And("I insert into revoked columns"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)

                    node.query(
                        f"INSERT INTO {table_name} ({insert_columns_pass}) VALUES ({data_pass})",
                        settings=[("user", user_name)],
                        exitcode=exitcode,
                        message=message,
                    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Insert_Cluster("1.0"),
)
def user_with_privilege_on_cluster(self, table_type, node=None):
    """Check that user is able or unable to insert into a table
    depending whether insert privilege is granted or revoked on a cluster.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        try:
            with Given("I have a user on a cluster"):
                node.query(
                    f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster"
                )

            with When(
                "I grant insert privilege on a cluster without the node with the table"
            ):
                node.query(
                    f"GRANT ON CLUSTER sharded_cluster23 INSERT ON {table_name} TO {user_name}"
                )

            with And("I insert into the table expecting a fail"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

            with And("I grant insert privilege on cluster including all nodes"):
                node.query(
                    f"GRANT ON CLUSTER sharded_cluster INSERT ON {table_name} TO {user_name}"
                )

            with And(
                "I revoke insert privilege on cluster without the node with the table"
            ):
                node.query(
                    f"REVOKE ON CLUSTER sharded_cluster23 INSERT ON {table_name} FROM {user_name}"
                )

            with And("I insert into the table"):
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                )

            with And("I check that I can read inserted data"):
                output = node.query(
                    f"SELECT d FROM {table_name} FORMAT JSONEachRow"
                ).output
                assert output == '{"d":"2020-01-01"}', error()

            with And("I revoke insert privilege on cluster with all nodes"):
                node.query(
                    f"REVOKE ON CLUSTER sharded_cluster INSERT ON {table_name} FROM {user_name}"
                )

            with Then("I insert into table expecting fail"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )
        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Insert_Cluster("1.0"),
)
def role_with_privilege_on_cluster(self, table_type, node=None):
    """Check that user with role is able to insert into a table
    depending whether insert privilege granted or revoked from the role on the cluster.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name, table_type):

        try:
            with Given("I have a user on a cluster"):
                node.query(
                    f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster"
                )

            with And("I have a role on a cluster"):
                node.query(
                    f"CREATE ROLE OR REPLACE {role_name} ON CLUSTER sharded_cluster"
                )

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(
                "I grant insert privilege on a cluster without the node with the table"
            ):
                node.query(
                    f"GRANT ON CLUSTER sharded_cluster23 INSERT ON {table_name} TO {role_name}"
                )

            with And("I insert into the table expecting a fail"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

            with And("I grant insert privilege on cluster including all nodes"):
                node.query(
                    f"GRANT ON CLUSTER sharded_cluster INSERT ON {table_name} TO {role_name}"
                )

            with And("I revoke insert privilege on cluster without the table node"):
                node.query(
                    f"REVOKE ON CLUSTER sharded_cluster23 INSERT ON {table_name} FROM {role_name}"
                )

            with And("I insert into the table"):
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                )

            with And("I check that I can read inserted data"):
                output = node.query(
                    f"SELECT d FROM {table_name} FORMAT JSONEachRow"
                ).output
                assert output == '{"d":"2020-01-01"}', error()

            with And("I revoke insert privilege on cluster with all nodes"):
                node.query(
                    f"REVOKE ON CLUSTER sharded_cluster INSERT ON {table_name} FROM {role_name}"
                )

            with Then("I insert into table expecting fail"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(
                    f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )
        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")


@TestOutline(Feature)
@Requirements(RQ_SRS_006_RBAC_Insert("1.0"), RQ_SRS_006_RBAC_Insert_TableEngines("1.0"))
@Examples("table_type", [(key,) for key in table_types.keys()])
@Name("insert")
def feature(self, table_type, stress=None, node="clickhouse1"):
    """Check the RBAC functionality of INSERT."""
    args = {"table_type": table_type}

    self.context.node = self.context.cluster.node(node)

    self.context.node1 = self.context.cluster.node("clickhouse1")
    self.context.node2 = self.context.cluster.node("clickhouse2")
    self.context.node3 = self.context.cluster.node("clickhouse3")

    if stress is not None:
        self.context.stress = stress

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
