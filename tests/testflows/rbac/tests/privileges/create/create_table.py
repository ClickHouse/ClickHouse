from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestScenario
@Requirements(RQ_SRS_006_RBAC_Privileges_None("1.0"))
def create_without_create_table_privilege(self, node=None):
    """Check that user is unable to create a table without CREATE TABLE privilege."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        try:
            with Given("I don't have a table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {user_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {user_name}")

            with Then(
                "I try to create a table without CREATE TABLE privilege as the user"
            ):
                node.query(
                    f"CREATE TABLE {table_name} (x Int8) ENGINE = Memory",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

        finally:
            with Finally("I drop the table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_create_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a table with CREATE TABLE privilege, either granted directly or through a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_create_table_privilege,
            name="create with create table privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_create_table_privilege,
            name="create with create table privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_create_table_privilege(self, grant_target_name, user_name, node=None):
    """Check that user is able to create a table with the granted privileges."""
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node
    try:
        with Given("I don't have a table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

        with When("I grant the CREATE TABLE privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with Then("I try to create a table without privilege as the user"):
            node.query(
                f"CREATE TABLE {table_name} (x Int8) ENGINE = Memory",
                settings=[("user", f"{user_name}")],
            )

    finally:
        with Then("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
@Requirements(RQ_SRS_006_RBAC_Privileges_All("1.0"))
def create_with_all_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a table with ALL privilege, either granted directly or through a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_all_privilege,
            name="create with ALL privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_all_privilege,
            name="create with ALL privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_all_privilege(self, grant_target_name, user_name, node=None):
    """Check that user is able to create a table with the granted privileges."""
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node
    try:
        with Given("I don't have a table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

        with Then("I try to create a table without privilege as the user"):
            node.query(
                f"CREATE TABLE {table_name} (x Int8) ENGINE = Memory",
                settings=[("user", f"{user_name}")],
            )

    finally:
        with Then("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_revoked_create_table_privilege_revoked_directly_or_from_role(
    self, node=None
):
    """Check that user is unable to create table after the CREATE TABLE privilege is revoked, either directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_revoked_create_table_privilege,
            name="create with create table privilege revoked directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_revoked_create_table_privilege,
            name="create with create table privilege revoked from a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_revoked_create_table_privilege(
    self, grant_target_name, user_name, node=None
):
    """Revoke CREATE TABLE privilege and check the user is unable to create a table."""
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    try:
        with Given("I don't have a table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

        with When("I grant CREATE TABLE privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with And("I revoke CREATE TABLE privilege"):
            node.query(f"REVOKE CREATE TABLE ON {table_name} FROM {grant_target_name}")

        with Then("I try to create a table on the table as the user"):
            node.query(
                f"CREATE TABLE {table_name} (x Int8) ENGINE = Memory",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    finally:
        with Finally("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_all_privileges_revoked_directly_or_from_role(self, node=None):
    """Check that user is unable to create table after ALL privileges are revoked, either directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_revoked_all_privilege,
            name="create with all privilege revoked directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_revoked_all_privilege,
            name="create with all privilege revoked from a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_revoked_all_privilege(self, grant_target_name, user_name, node=None):
    """Revoke ALL privilege and check the user is unable to create a table."""
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    try:
        with Given("I don't have a table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

        with When("I grant CREATE TABLE privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with And("I revoke ALL privilege"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I try to create a table on the table as the user"):
            node.query(
                f"CREATE TABLE {table_name} (x Int8) ENGINE = Memory",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    finally:
        with Finally("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_without_source_table_privilege(self, node=None):
    """Check that user is unable to create a table without select
    privilege on the source table.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    source_table_name = f"source_table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    with table(node, f"{source_table_name}"):
        with user(node, f"{user_name}"):
            try:

                with When("I grant CREATE TABLE privilege to a user"):
                    node.query(f"GRANT CREATE TABLE ON {table_name} TO {user_name}")

                with And("I grant INSERT privilege"):
                    node.query(f"GRANT INSERT ON {table_name} TO {user_name}")

                with Then(
                    "I try to create a table without select privilege on the table"
                ):
                    node.query(
                        f"CREATE TABLE {table_name} ENGINE = Memory AS SELECT * FROM {source_table_name}",
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )

            finally:
                with Finally("I drop the table"):
                    node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_without_insert_privilege(self, node=None):
    """Check that user is unable to create a table without insert
    privilege on that table.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    source_table_name = f"source_table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    with table(node, f"{source_table_name}"):
        with user(node, f"{user_name}"):

            try:
                with When("I grant CREATE TABLE privilege to a user"):
                    node.query(f"GRANT CREATE TABLE ON {table_name} TO {user_name}")

                with And("I grant SELECT privilege"):
                    node.query(f"GRANT SELECT ON {source_table_name} TO {user_name}")

                with Then(
                    "I try to create a table without select privilege on the table"
                ):
                    node.query(
                        f"CREATE TABLE {table_name} ENGINE = Memory AS SELECT * FROM {source_table_name}",
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )
            finally:
                with Finally("I drop the table"):
                    node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_source_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that a user is able to create a table if and only if the user has create table privilege and
    select privilege on the source table, either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_source_table_privilege,
            name="create with create table and select privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_source_table_privilege,
            name="create with create table and select privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_source_table_privilege(self, user_name, grant_target_name, node=None):
    """Check that user is unable to create a table without SELECT privilege on the source table."""
    table_name = f"table_{getuid()}"
    source_table_name = f"source_table_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, f"{source_table_name}"):

        try:
            with When("I grant CREATE table privilege"):
                node.query(f"GRANT CREATE table ON {table_name} TO {grant_target_name}")

            with And("I grant INSERT privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")

            with And("I grant SELECT privilege"):
                node.query(
                    f"GRANT SELECT ON {source_table_name} TO {grant_target_name}"
                )

            with And("I try to create a table on the table as the user"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")
                node.query(
                    f"CREATE TABLE {table_name} ENGINE = Memory AS SELECT * FROM {source_table_name}",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_subquery_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a table where the stored query has two subqueries
    if and only if the user has SELECT privilege on all of the tables,
    either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_subquery,
            name="create with subquery, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_subquery,
            name="create with subquery, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_subquery(self, user_name, grant_target_name, node=None):
    """Grant select and create table privileges and check that user is able to create a table
    if and only if they have all necessary privileges.
    """
    table_name = f"table_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_table_query = "CREATE TABLE {table_name} ENGINE = Memory AS SELECT * FROM {table0_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table2_name} WHERE y<2))"

    if node is None:
        node = self.context.node

    with table(node, f"{table0_name},{table1_name},{table2_name}"):

        try:
            with When("I grant CREATE TABLE privilege"):
                node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

            with And("I grant INSERT privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")

            with Then("I attempt to CREATE TABLE as the user with create privilege"):
                node.query(
                    create_table_query.format(
                        table_name=table_name,
                        table0_name=table0_name,
                        table1_name=table1_name,
                        table2_name=table2_name,
                    ),
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            for permutation in permutations(table_count=3):
                with grant_select_on_table(
                    node,
                    permutation,
                    grant_target_name,
                    table0_name,
                    table1_name,
                    table2_name,
                ) as tables_granted:

                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):

                        with When("I attempt to create a table as the user"):
                            node.query(
                                create_table_query.format(
                                    table_name=table_name,
                                    table0_name=table0_name,
                                    table1_name=table1_name,
                                    table2_name=table2_name,
                                ),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all tables"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=3)) + 1,
                    grant_target_name,
                    table0_name,
                    table1_name,
                    table2_name,
                ):

                    with When("I attempt to create a table as the user"):
                        node.query(
                            create_table_query.format(
                                table_name=table_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                                table2_name=table2_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_join_query_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a table where the stored query includes a `JOIN` statement
    if and only if the user has SELECT privilege on all of the tables,
    either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_join_query,
            name="create with join query, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_join_query,
            name="create with join query, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_join_query(self, grant_target_name, user_name, node=None):
    """Grant select and create table privileges and check that user is able to create a table
    if and only if they have all necessary privileges.
    """
    table_name = f"table_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_table_query = "CREATE TABLE {table_name} ENGINE = Memory AS SELECT * FROM {table0_name} JOIN {table1_name} USING d"

    if node is None:
        node = self.context.node

    with table(node, f"{table0_name},{table1_name}"):

        try:
            with When("I grant CREATE TABLE privilege"):
                node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

            with And("I grant INSERT privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")

            with Then("I attempt to create table as the user"):
                node.query(
                    create_table_query.format(
                        table_name=table_name,
                        table0_name=table0_name,
                        table1_name=table1_name,
                    ),
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            for permutation in permutations(table_count=2):
                with grant_select_on_table(
                    node, permutation, grant_target_name, table0_name, table1_name
                ) as tables_granted:

                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):

                        with When("I attempt to create a table as the user"):
                            node.query(
                                create_table_query.format(
                                    table_name=table_name,
                                    table0_name=table0_name,
                                    table1_name=table1_name,
                                ),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all tables"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=2)) + 1,
                    grant_target_name,
                    table0_name,
                    table1_name,
                ):

                    with When("I attempt to create a table as the user"):
                        node.query(
                            create_table_query.format(
                                table_name=table_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Then("I drop the table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_union_query_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a table where the stored query includes a `UNION ALL` statement
    if and only if the user has SELECT privilege on all of the tables,
    either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_union_query,
            name="create with union query, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_union_query,
            name="create with union query, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_union_query(self, grant_target_name, user_name, node=None):
    """Grant select and create table privileges and check that user is able to create a table
    if and only if they have all necessary privileges.
    """
    table_name = f"table_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_table_query = "CREATE TABLE {table_name} ENGINE = Memory AS SELECT * FROM {table0_name} UNION ALL SELECT * FROM {table1_name}"

    if node is None:
        node = self.context.node

    with table(node, f"{table0_name},{table1_name}"):

        try:
            with When("I grant CREATE TABLE privilege"):
                node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

            with And("I grant INSERT privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")

            with Then("I attempt to create table as the user"):
                node.query(
                    create_table_query.format(
                        table_name=table_name,
                        table0_name=table0_name,
                        table1_name=table1_name,
                    ),
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            for permutation in permutations(table_count=2):
                with grant_select_on_table(
                    node, permutation, grant_target_name, table0_name, table1_name
                ) as tables_granted:

                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):

                        with When("I attempt to create a table as the user"):
                            node.query(
                                create_table_query.format(
                                    table_name=table_name,
                                    table0_name=table0_name,
                                    table1_name=table1_name,
                                ),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all tables"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=2)) + 1,
                    grant_target_name,
                    table0_name,
                    table1_name,
                ):

                    with When("I attempt to create a table as the user"):
                        node.query(
                            create_table_query.format(
                                table_name=table_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_join_union_subquery_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to create a table with a stored query that includes `UNION ALL`, `JOIN` and two subqueries
    if and only if the user has SELECT privilege on all of the tables, either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_join_union_subquery,
            name="create with join union subquery, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_join_union_subquery,
            name="create with join union subquery, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_join_union_subquery(self, grant_target_name, user_name, node=None):
    """Grant select and create table privileges and check that user is able to create a table
    if and only if they have all necessary privileges.
    """
    table_name = f"table_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    table4_name = f"table4_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_table_query = "CREATE TABLE {table_name} ENGINE = Memory AS SELECT y FROM {table0_name} JOIN {table1_name} USING y UNION ALL SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table3_name} WHERE y IN (SELECT y FROM {table4_name} WHERE y<2))"

    if node is None:
        node = self.context.node

    with table(
        node, f"{table0_name},{table1_name},{table2_name},{table3_name},{table4_name}"
    ):
        with user(node, f"{user_name}"):

            try:
                with When("I grant CREATE TABLE privilege"):
                    node.query(
                        f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}"
                    )

                with And("I grant INSERT privilege"):
                    node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")

                with Then(
                    "I attempt to create table as the user with CREATE TABLE privilege"
                ):
                    node.query(
                        create_table_query.format(
                            table_name=table_name,
                            table0_name=table0_name,
                            table1_name=table1_name,
                            table2_name=table2_name,
                            table3_name=table3_name,
                            table4_name=table4_name,
                        ),
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )

                for permutation in permutations(table_count=5):
                    with grant_select_on_table(
                        node,
                        permutation,
                        grant_target_name,
                        table0_name,
                        table1_name,
                        table3_name,
                        table4_name,
                    ) as tables_granted:

                        with When(
                            f"permutation={permutation}, tables granted = {tables_granted}"
                        ):

                            with Given("I don't have a table"):
                                node.query(f"DROP TABLE IF EXISTS {table_name}")

                            with Then("I attempt to create a table as the user"):
                                node.query(
                                    create_table_query.format(
                                        table_name=table_name,
                                        table0_name=table0_name,
                                        table1_name=table1_name,
                                        table2_name=table2_name,
                                        table3_name=table3_name,
                                        table4_name=table4_name,
                                    ),
                                    settings=[("user", f"{user_name}")],
                                    exitcode=exitcode,
                                    message=message,
                                )

                with When("I grant select on all tables"):
                    with grant_select_on_table(
                        node,
                        max(permutations(table_count=5)) + 1,
                        grant_target_name,
                        table0_name,
                        table1_name,
                        table2_name,
                        table3_name,
                        table4_name,
                    ):

                        with Given("I don't have a table"):
                            node.query(f"DROP TABLE IF EXISTS {table_name}")

                        with Then("I attempt to create a table as the user"):
                            node.query(
                                create_table_query.format(
                                    table_name=table_name,
                                    table0_name=table0_name,
                                    table1_name=table1_name,
                                    table2_name=table2_name,
                                    table3_name=table3_name,
                                    table4_name=table4_name,
                                ),
                                settings=[("user", f"{user_name}")],
                            )

            finally:
                with Finally("I drop the table"):
                    node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_with_nested_tables_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a table with a stored query that includes other tables if and only if
    they have SELECT privilege on all the tables and the source tables for those tables.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_nested_tables,
            name="create with nested tables, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_nested_tables,
            name="create with nested tables, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_nested_tables(self, grant_target_name, user_name, node=None):
    """Grant select and create table privileges and check that user is able to create a table
    if and only if they have all necessary privileges.
    """
    table_name = f"table_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    table4_name = f"table4_{getuid()}"
    table5_name = f"table5_{getuid()}"
    table6_name = f"table6_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_table_query = "CREATE TABLE {table_name} ENGINE = Memory AS SELECT y FROM {table6_name} UNION ALL SELECT y FROM {table5_name}"

    if node is None:
        node = self.context.node

    with table(node, f"{table0_name},{table2_name},{table4_name},{table6_name}"):

        try:
            with Given("I have some tables"):
                node.query(
                    f"CREATE TABLE {table1_name} ENGINE = Memory AS SELECT y FROM {table0_name}"
                )
                node.query(
                    f"CREATE TABLE {table3_name} ENGINE = Memory AS SELECT y FROM {table2_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y<2)"
                )
                node.query(
                    f"CREATE TABLE {table5_name} ENGINE = Memory AS SELECT y FROM {table4_name} JOIN {table3_name} USING y"
                )

            with When("I grant CREATE TABLE privilege"):
                node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

            with And("I grant INSERT privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")

            with Then(
                "I attempt to create table as the user with CREATE TABLE privilege"
            ):
                node.query(
                    create_table_query.format(
                        table_name=table_name,
                        table5_name=table5_name,
                        table6_name=table6_name,
                    ),
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            for permutation in (
                [0, 1, 2, 3, 7, 11, 15, 31, 39, 79, 95],
                permutations(table_count=7),
            )[self.context.stress]:
                with grant_select_on_table(
                    node,
                    permutation,
                    grant_target_name,
                    table5_name,
                    table6_name,
                    table3_name,
                    table4_name,
                    table1_name,
                    table2_name,
                    table0_name,
                ) as tables_granted:

                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):

                        with Given("I don't have a table"):
                            node.query(f"DROP TABLE IF EXISTS {table3_name}")

                        with Then("I attempt to create a table as the user"):
                            node.query(
                                create_table_query.format(
                                    table_name=table_name,
                                    table5_name=table5_name,
                                    table6_name=table6_name,
                                ),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all tables"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=7)) + 1,
                    grant_target_name,
                    table0_name,
                    table1_name,
                    table2_name,
                    table3_name,
                    table4_name,
                    table5_name,
                    table6_name,
                ):

                    with Given("I don't have a table"):
                        node.query(f"DROP TABLE IF EXISTS {table_name}")

                    with Then("I attempt to create a table as the user"):
                        node.query(
                            create_table_query.format(
                                table_name=table_name,
                                table5_name=table5_name,
                                table6_name=table6_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally(f"I drop {table_name}"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

            with And(f"I drop {table1_name}"):
                node.query(f"DROP TABLE IF EXISTS {table1_name}")

            with And(f"I drop {table3_name}"):
                node.query(f"DROP TABLE IF EXISTS {table3_name}")

            with And(f"I drop {table5_name}"):
                node.query(f"DROP TABLE IF EXISTS {table5_name}")


@TestScenario
def create_as_another_table(self, node=None):
    """Check that user is able to create a table as another table with only CREATE TABLE privilege."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    source_table_name = f"source_table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    with table(node, f"{source_table_name}"):
        with user(node, f"{user_name}"):

            try:
                with When("I grant CREATE TABLE privilege to a user"):
                    node.query(f"GRANT CREATE TABLE ON {table_name} TO {user_name}")

                with Then("I try to create a table as another table"):
                    node.query(
                        f"CREATE TABLE {table_name} AS {source_table_name}",
                        settings=[("user", f"{user_name}")],
                    )

            finally:
                with Finally("I drop the tables"):
                    node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_as_numbers(self, node=None):
    """Check that user is able to create a table as numbers table function."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        try:
            with When("I grant CREATE TABLE privilege to a user"):
                node.query(f"GRANT CREATE TABLE ON {table_name} TO {user_name}")

            with And("I grant INSERT privilege"):
                node.query(f"GRANT INSERT ON {table_name} TO {user_name}")

            with Then("I try to create a table without select privilege on the table"):
                node.query(
                    f"CREATE TABLE {table_name} AS numbers(5)",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the tables"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestScenario
def create_as_merge(self, node=None):
    """Check that user is able to create a table as merge table function."""
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    source_table_name = f"source_table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    with table(node, f"{source_table_name}"):
        with user(node, f"{user_name}"):

            try:
                with When("I grant CREATE TABLE privilege to a user"):
                    node.query(f"GRANT CREATE TABLE ON {table_name} TO {user_name}")

                with And("I grant SELECT privilege on the source table"):
                    node.query(f"GRANT SELECT ON {source_table_name} TO {user_name}")

                with Then("I try to create a table as another table"):
                    node.query(
                        f"CREATE TABLE {table_name} AS merge(default,'{source_table_name}')",
                        settings=[("user", f"{user_name}")],
                    )

            finally:
                with Finally("I drop the tables"):
                    node.query(f"DROP TABLE IF EXISTS {table_name}")


@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_CreateTable("1.0"),
)
@Name("create table")
def feature(self, stress=None, node="clickhouse1"):
    """Check the RBAC functionality of CREATE TABLE."""
    self.context.node = self.context.cluster.node(node)

    if stress is not None:
        self.context.stress = stress

    with Pool(10) as pool:
        try:
            for scenario in loads(current_module(), Scenario):
                Scenario(run=scenario, parallel=True, executor=pool)
        finally:
            join()
