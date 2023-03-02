from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@contextmanager
def allow_experimental_alter_materialized_view_structure(node):
    setting = ("allow_experimental_alter_materialized_view_structure", 1)
    default_query_settings = None

    try:
        with Given(
            "I add allow_experimental_alter_materialized_view_structure to the default query settings"
        ):
            default_query_settings = getsattr(
                current().context, "default_query_settings", []
            )
            default_query_settings.append(setting)
        yield
    finally:
        with Finally(
            "I remove allow_experimental_alter_materialized_view_structure from the default query settings"
        ):
            if default_query_settings:
                try:
                    default_query_settings.pop(default_query_settings.index(setting))
                except ValueError:
                    pass


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Create("1.0"),
)
def create(self, node=None):
    """Test the RBAC functionality of the `CREATE MATERIALIZED VIEW` command."""
    Scenario(
        run=create_without_create_view_privilege, setup=instrument_clickhouse_server_log
    )
    Scenario(
        run=create_with_create_view_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_revoked_create_view_privilege_revoked_directly_or_from_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_without_source_table_privilege,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_source_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_subquery_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_join_query_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_union_query_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_join_union_subquery_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_nested_views_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_target_table_privilege_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_populate_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=create_with_populate_source_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )


@TestScenario
def create_without_create_view_privilege(self, node=None):
    """Check that user is unable to create a view without CREATE VIEW privilege."""
    user_name = f"user_{getuid()}"
    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        with When("I try to create a view without CREATE VIEW privilege as the user"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )


@TestScenario
def create_with_create_view_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a view with CREATE VIEW privilege, either granted directly or through a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=create_with_create_view_privilege,
            name="create with create view privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=create_with_create_view_privilege,
            name="create with create view privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_create_view_privilege(self, grant_target_name, user_name, node=None):
    """Check that user is able to create a view with the granted privileges."""
    view_name = f"view_{getuid()}"

    if node is None:
        node = self.context.node
    try:
        with When("I grant the CREATE VIEW privilege"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")

        with Then("I try to create a view without privilege as the user"):
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1",
                settings=[("user", f"{user_name}")],
            )

    finally:
        with Then("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_revoked_create_view_privilege_revoked_directly_or_from_role(
    self, node=None
):
    """Check that user is unable to create view after the CREATE VIEW privilege is revoked, either directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=create_with_revoked_create_view_privilege,
            name="create with create view privilege revoked directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=create_with_revoked_create_view_privilege,
            name="create with create view privilege revoked from a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_revoked_create_view_privilege(
    self, grant_target_name, user_name, node=None
):
    """Revoke CREATE VIEW privilege and check the user is unable to create a view."""
    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with When("I grant CREATE VIEW privilege"):
        node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")

    with And("I revoke CREATE VIEW privilege"):
        node.query(f"REVOKE CREATE VIEW ON {view_name} FROM {grant_target_name}")

    with Then("I try to create a view on the table as the user"):
        node.query(
            f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1",
            settings=[("user", f"{user_name}")],
            exitcode=exitcode,
            message=message,
        )


@TestScenario
def create_without_source_table_privilege(self, node=None):
    """Check that user is unable to create a view without select
    privilege on the source table.
    """
    user_name = f"user_{getuid()}"
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        with user(node, f"{user_name}"):
            with When("I grant CREATE VIEW privilege to a user"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {user_name}")

            with Then("I try to create a view without select privilege on the table"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )


@TestScenario
def create_with_source_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that a user is able to create a view if and only if the user has create view privilege and
    select privilege on the source table, either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=create_with_source_table_privilege,
            name="create with create view and select privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=create_with_source_table_privilege,
            name="create with create view and select privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_source_table_privilege(self, user_name, grant_target_name, node=None):
    """Check that user is unable to create a view without SELECT privilege on the source table."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")

            with And("I grant SELECT privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")

            with And("I try to create a view on the table as the user"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                )

            with Then("I check the view"):
                output = node.query(f"SELECT count(*) FROM {view_name}").output
                assert output == "0", error()

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_subquery_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a view where the stored query has two subqueries
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
    """Grant select and create view privileges and check that user is able to create a view
    if and only if they have all necessary privileges.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_view_query = "CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table0_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table2_name} WHERE y<2))"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name},{table2_name}"):
        try:
            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")
            with Then("I attempt to CREATE VIEW as the user with create privilege"):
                node.query(
                    create_view_query.format(
                        view_name=view_name,
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
                        with Given("I don't have a view"):
                            node.query(f"DROP VIEW IF EXISTS {view_name}")
                        with Then("I attempt to create a view as the user"):
                            node.query(
                                create_view_query.format(
                                    view_name=view_name,
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
                    with Given("I don't have a view"):
                        node.query(f"DROP VIEW IF EXISTS {view_name}")
                    with Then("I attempt to create a view as the user"):
                        node.query(
                            create_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                                table2_name=table2_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_join_query_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a view where the stored query includes a `JOIN` statement
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
    """Grant select and create view privileges and check that user is able to create a view
    if and only if they have all necessary privileges.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_view_query = "CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table0_name} JOIN {table1_name} USING d"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")
            with Then("I attempt to create view as the user"):
                node.query(
                    create_view_query.format(
                        view_name=view_name,
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
                        with Given("I don't have a view"):
                            node.query(f"DROP VIEW IF EXISTS {view_name}")
                        with Then("I attempt to create a view as the user"):
                            node.query(
                                create_view_query.format(
                                    view_name=view_name,
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
                    with Given("I don't have a view"):
                        node.query(f"DROP VIEW IF EXISTS {view_name}")
                    with Then("I attempt to create a view as the user"):
                        node.query(
                            create_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Then("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_union_query_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a view where the stored query includes a `UNION ALL` statement
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
    """Grant select and create view privileges and check that user is able to create a view
    if and only if they have all necessary privileges.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_view_query = "CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table0_name} UNION ALL SELECT * FROM {table1_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")
            with Then("I attempt to create view as the user"):
                node.query(
                    create_view_query.format(
                        view_name=view_name,
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
                        with Given("I don't have a view"):
                            node.query(f"DROP VIEW IF EXISTS {view_name}")
                        with Then("I attempt to create a view as the user"):
                            node.query(
                                create_view_query.format(
                                    view_name=view_name,
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
                    with Given("I don't have a view"):
                        node.query(f"DROP VIEW IF EXISTS {view_name}")
                    with Then("I attempt to create a view as the user"):
                        node.query(
                            create_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_join_union_subquery_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to create a view with a stored query that includes `UNION ALL`, `JOIN` and two subqueries
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
    """Grant select and create view privileges and check that user is able to create a view
    if and only if they have all necessary privileges.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    table4_name = f"table4_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_view_query = "CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT y FROM {table0_name} JOIN {table1_name} USING y UNION ALL SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table3_name} WHERE y IN (SELECT y FROM {table4_name} WHERE y<2))"

    if node is None:
        node = self.context.node
    with table(
        node, f"{table0_name},{table1_name},{table2_name},{table3_name},{table4_name}"
    ):
        with user(node, f"{user_name}"):
            try:
                with When("I grant CREATE VIEW privilege"):
                    node.query(
                        f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}"
                    )
                with Then(
                    "I attempt to create view as the user with CREATE VIEW privilege"
                ):
                    node.query(
                        create_view_query.format(
                            view_name=view_name,
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
                            with Given("I don't have a view"):
                                node.query(f"DROP VIEW IF EXISTS {view_name}")
                            with Then("I attempt to create a view as the user"):
                                node.query(
                                    create_view_query.format(
                                        view_name=view_name,
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
                        with Given("I don't have a view"):
                            node.query(f"DROP VIEW IF EXISTS {view_name}")
                        with Then("I attempt to create a view as the user"):
                            node.query(
                                create_view_query.format(
                                    view_name=view_name,
                                    table0_name=table0_name,
                                    table1_name=table1_name,
                                    table2_name=table2_name,
                                    table3_name=table3_name,
                                    table4_name=table4_name,
                                ),
                                settings=[("user", f"{user_name}")],
                            )

            finally:
                with Finally("I drop the view"):
                    node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_nested_views_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a view with a stored query that includes other views if and only if
    they have SELECT privilege on all the views and the source tables for those views.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=create_with_nested_views,
            name="create with nested views, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=create_with_nested_views,
            name="create with nested views, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_nested_views(self, grant_target_name, user_name, node=None):
    """Grant SELECT and CREATE VIEW privileges and check that user is able to create a view
    if and only if they have all necessary privileges.
    """
    view0_name = f"view0_{getuid()}"
    view1_name = f"view1_{getuid()}"
    view2_name = f"view2_{getuid()}"
    view3_name = f"view3_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    create_view_query = "CREATE MATERIALIZED VIEW {view3_name} ENGINE = Memory AS SELECT y FROM {table3_name} UNION ALL SELECT y FROM {view2_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name},{table2_name},{table3_name}"):
        try:
            with Given("I have some views"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view0_name} ENGINE = Memory AS SELECT y FROM {table0_name}"
                )
                node.query(
                    f"CREATE MATERIALIZED VIEW {view1_name} ENGINE = Memory AS SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {view0_name} WHERE y<2)"
                )
                node.query(
                    f"CREATE MATERIALIZED VIEW {view2_name} ENGINE = Memory AS SELECT y FROM {table2_name} JOIN {view1_name} USING y"
                )

            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view3_name} TO {grant_target_name}")
            with Then(
                "I attempt to create view as the user with CREATE VIEW privilege"
            ):
                node.query(
                    create_view_query.format(
                        view3_name=view3_name,
                        view2_name=view2_name,
                        table3_name=table3_name,
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
                    view2_name,
                    table3_name,
                    view1_name,
                    table2_name,
                    view0_name,
                    table1_name,
                    table0_name,
                ) as tables_granted:
                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):
                        with Given("I don't have a view"):
                            node.query(f"DROP VIEW IF EXISTS {view3_name}")
                        with Then("I attempt to create a view as the user"):
                            node.query(
                                create_view_query.format(
                                    view3_name=view3_name,
                                    view2_name=view2_name,
                                    table3_name=table3_name,
                                ),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all views"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=7)) + 1,
                    grant_target_name,
                    view0_name,
                    view1_name,
                    view2_name,
                    table0_name,
                    table1_name,
                    table2_name,
                    table3_name,
                ):
                    with Given("I don't have a view"):
                        node.query(f"DROP VIEW IF EXISTS {view3_name}")
                    with Then("I attempt to create a view as the user"):
                        node.query(
                            create_view_query.format(
                                view3_name=view3_name,
                                view2_name=view2_name,
                                table3_name=table3_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the views"):
                with When("I drop view0", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view3_name}")
                with And("I drop view1", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view2_name}")
                with And("I drop view2", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view1_name}")
                with And("I drop view3", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view0_name}")


@TestScenario
def create_with_target_table_privilege_directly_or_via_role(self, node=None):
    """Check that user is able to create a materialized view with a target table if and only if
    the user has CREATE VIEW privilege and SELECT and INSERT privileges on the target table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=create_with_target_table,
            name="create with target table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=create_with_target_table,
            name="create with target table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_target_table(self, grant_target_name, user_name, node=None):
    """Check that user is unable to create a view without INSERT and SELECT privileges and is able to once both are granted."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")
            with Then("I attempt to create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table_name} AS SELECT 1",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT on the target table"):
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")
            with Then("I attempt to create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table_name} AS SELECT 1",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I revoke SELECT on the target table"):
                node.query(f"REVOKE SELECT ON {table_name} FROM {grant_target_name}")
            with And("I grant INSERT privilege on the target table"):
                node.query(f"GRANT INSERT ON {table_name} TO {grant_target_name}")
            with Then("I attempt to create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table_name} AS SELECT 1",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT on the target table"):
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")
            with Then("I successfully create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table_name} AS SELECT 1",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_populate_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a view with POPULATE specified if and only if
    they have CREATE VIEW and INSERT privileges for the view, either directly or from a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Scenario(
            test=create_with_populate,
            name="create with populate privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Scenario(
            test=create_with_populate,
            name="create with populate privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_populate(self, user_name, grant_target_name, node=None):
    """Check that user is only able to create the view after INSERT privilege is granted."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    try:

        with When("I grant CREATE VIEW privilege"):
            node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")

        with Then("I attempt to create a view as the user"):
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT 1",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

        with When("I grant INSERT privilege on the view"):
            node.query(f"GRANT INSERT ON {view_name} TO {grant_target_name}")

        with Given("I don't have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")

        with Then("I attempt to create a view as the user"):
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT 1",
                settings=[("user", f"{user_name}")],
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def create_with_populate_source_table_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to create a view with POPULATE and a source table specified if and only if
    they have CREATE VIEW and INSERT privileges for the view, either directly or from a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=create_with_populate_source_table,
            name="create with populate and source table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=create_with_populate_source_table,
            name="create with populate and source table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def create_with_populate_source_table(self, user_name, grant_target_name, node=None):
    """Check that user is only able to create the view after INSERT privilege is granted."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with When("I grant CREATE VIEW privilege"):
                node.query(f"GRANT CREATE VIEW ON {view_name} TO {grant_target_name}")
            with Then("I attempt to create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT privilege on the source table"):
                node.query(f"GRANT SELECT ON {table_name} TO {user_name}")
            with Then("I attempt to create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant INSERT privilege on the view"):
                node.query(f"GRANT INSERT ON {view_name} TO {grant_target_name}")
            with Given("I don't have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
            with Then("I attempt to create a view as the user"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Select("1.0"),
)
def select(self, node=None):
    """Test the RBAC functionality of the `SELECT FROM materialized view` command"""
    Scenario(
        run=select_without_select_privilege, setup=instrument_clickhouse_server_log
    )
    Scenario(
        run=select_with_select_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_select_privilege_revoked_directly_or_from_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_without_source_table_privilege,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_source_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_subquery_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_join_query_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_union_query_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_join_union_subquery_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_nested_views_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_with_privilege_granted_directly_or_via_role_without_target_table_privilege,
        setup=instrument_clickhouse_server_log,
    )


@TestScenario
def select_without_select_privilege(self, node=None):
    """Check that user is unable to select on a view without view SELECT privilege."""
    user_name = f"user_{getuid()}"
    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        try:
            with When("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                )

            with Then("I try to select from view without privilege as the user"):
                node.query(
                    f"SELECT * FROM {view_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_select_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a view if and only if they have select privilege on that view, either directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_select_privilege,
            name="select with select privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_select_privilege,
            name="select with select privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_select_privilege(self, user_name, grant_target_name, node=None):
    """Grant SELECT privilege on a view and check the user is able to SELECT from it."""
    view_name = f"view_{getuid()}"

    if node is None:
        node = self.context.node
    try:
        with When("I have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
            )

        with And("I grant SELECT privilege for the view"):
            node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")

        with Then("I attempt to select from view with privilege as the user"):
            output = node.query(
                f"SELECT count(*) FROM {view_name}", settings=[("user", f"{user_name}")]
            ).output
            assert output == "1", error()

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_select_privilege_revoked_directly_or_from_role(self, node=None):
    """Check that user is unable to select from a view if their SELECT privilege is revoked, either directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_select_privilege,
            name="select with select privilege revoked directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_select_privilege,
            name="select with select privilege revoked from a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_revoked_select_privilege(self, user_name, grant_target_name, node=None):
    """Grant and revoke SELECT privilege on a view and check the user is unable to SELECT from it."""
    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    try:
        with When("I have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(f"CREATE VIEW {view_name} AS SELECT 1")

        with And("I grant SELECT privilege for the view"):
            node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
        with And("I revoke SELECT privilege for the view"):
            node.query(f"REVOKE SELECT ON {view_name} FROM {grant_target_name}")

        with Then("I attempt to select from view with privilege as the user"):
            node.query(
                f"SELECT count(*) FROM {view_name}",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_without_source_table_privilege(self, node=None):
    """Check that user is unable to select from a view without SELECT privilege for the source table."""
    user_name = f"user_{getuid()}"
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        with user(node, f"{user_name}"):
            try:
                with When("I create a view from the source table"):
                    node.query(f"DROP VIEW IF EXISTS {view_name}")
                    node.query(
                        f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}"
                    )

                with And("I grant view select privilege to the user"):
                    node.query(f"GRANT SELECT ON {view_name} TO {user_name}")
                with Then(
                    "I attempt to select from view without privilege on the source table"
                ):
                    node.query(
                        f"SELECT count(*) FROM {view_name}",
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )

            finally:
                with Finally("I drop the view"):
                    node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_source_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a view, with source table in the stored query, if and only if
    the user has SELECT privilege for the view and the source table, either directly or from a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_source_table_privilege,
            name="select with source table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_source_table_privilege,
            name="select with source table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_source_table_privilege(self, user_name, grant_target_name, node=None):
    """Grant SELECT privilege on view and the source table for that view and check the user is able to SELECT from the view."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with Given("I have a view with a source table"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}"
                )

            with And("I grant select privileges"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")

            with Then("I check the user is able to select from the view"):
                output = node.query(
                    f"SELECT count(*) FROM {view_name}",
                    settings=[("user", f"{user_name}")],
                ).output
                assert output == "0", error()

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_subquery_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a view where the stored query has two subqueries if and only if
    the user has SELECT privilege for that view and all tables, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_subquery,
            name="select with subquery, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_subquery,
            name="select with subquery, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_subquery(self, user_name, grant_target_name, node=None):
    """Grant SELECT on the view and tables in the stored query and check the user is able to SELECT if and only if they have SELECT privilege on all of them."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    select_view_query = "SELECT count(*) FROM {view_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name},{table2_name}"):
        try:
            with Given("I have a view with a subquery"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table0_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table2_name} WHERE y<2))"
                )

            with When("I grant SELECT privilege on view"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to select from the view as the user"):
                node.query(
                    select_view_query.format(view_name=view_name),
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
                        with Then("I attempt to select from a view as the user"):
                            node.query(
                                select_view_query.format(view_name=view_name),
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
                    with Then("I attempt to select from a view as the user"):
                        output = node.query(
                            select_view_query.format(view_name=view_name),
                            settings=[("user", f"{user_name}")],
                        ).output
                        assert output == "0", error()

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_join_query_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a view where the stored query includes a `JOIN` statement if and only if
    the user has SELECT privilege on all the tables and the view, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_join_query,
            name="select with join, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_join_query,
            name="select with join, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_join_query(self, user_name, grant_target_name, node=None):
    """Grant SELECT on the view and tables in the stored query and check the user is able to SELECT if and only if they have SELECT privilege on all of them."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    select_view_query = "SELECT count(*) FROM {view_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view with a JOIN statement"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table0_name} JOIN {table1_name} USING d"
                )

            with When("I grant SELECT privilege on view"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to select from the view as the user"):
                node.query(
                    select_view_query.format(view_name=view_name),
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
                        with Then("I attempt to select from a view as the user"):
                            node.query(
                                select_view_query.format(view_name=view_name),
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
                    with Then("I attempt to select from a view as the user"):
                        node.query(
                            select_view_query.format(view_name=view_name),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_union_query_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a view where the stored query includes a `UNION ALL` statement if and only if
    the user has SELECT privilege on all the tables and the view, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_union_query,
            name="select with union, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_union_query,
            name="select with union, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_union_query(self, user_name, grant_target_name, node=None):
    """Grant SELECT on the view and tables in the stored query and check the user is able to SELECT if and only if they have SELECT privilege on all of them."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    select_view_query = "SELECT count(*) FROM {view_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view with a UNION statement"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table0_name} UNION ALL SELECT * FROM {table1_name}"
                )

            with When("I grant SELECT privilege on view"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to select from the view as the user"):
                node.query(
                    select_view_query.format(view_name=view_name),
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
                        with Then("I attempt to select from a view as the user"):
                            node.query(
                                select_view_query.format(view_name=view_name),
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
                    with Then("I attempt to select from a view as the user"):
                        node.query(
                            select_view_query.format(view_name=view_name),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_join_union_subquery_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to select from a view with a stored query that includes `UNION ALL`, `JOIN` and two subqueries
    if and only if the user has SELECT privilege on all the tables and the view, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_join_union_subquery,
            name="select with join union subquery, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_join_union_subquery,
            name="select with join union subquery, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_join_union_subquery(self, grant_target_name, user_name, node=None):
    """Grant SELECT on the view and tables in the stored query and check the user is able to SELECT if and only if they have SELECT privilege on all of them."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    table4_name = f"table4_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    select_view_query = "SELECT count(*) FROM {view_name}"

    if node is None:
        node = self.context.node
    with table(
        node, f"{table0_name},{table1_name},{table2_name},{table3_name},{table4_name}"
    ):
        try:
            with Given("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT y FROM {table0_name} JOIN {table1_name} USING y UNION ALL SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table3_name} WHERE y IN (SELECT y FROM {table4_name} WHERE y<2))"
                )

            with When("I grant SELECT privilege on view"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to select from the view as the user"):
                node.query(
                    select_view_query.format(view_name=view_name),
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
                    table2_name,
                    table3_name,
                    table4_name,
                ) as tables_granted:
                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):
                        with Then("I attempt to select from a view as the user"):
                            node.query(
                                select_view_query.format(view_name=view_name),
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
                    with Then("I attempt to select from a view as the user"):
                        node.query(
                            select_view_query.format(view_name=view_name),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def select_with_nested_views_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a view with a stored query that includes other views if and only if
    the user has SELECT privilege on all of the views and the source tables for those views, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_with_nested_views,
            name="select with nested views, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_with_nested_views,
            name="select with nested views, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_with_nested_views(self, grant_target_name, user_name, node=None):
    """Grant SELECT on views and tables in the stored query and check the user is able to SELECT if and only if they have SELECT privilege on all of them."""
    view0_name = f"view0_{getuid()}"
    view1_name = f"view1_{getuid()}"
    view2_name = f"view2_{getuid()}"
    view3_name = f"view3_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    select_view_query = "SELECT count(*) FROM {view3_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name},{table2_name},{table3_name}"):
        try:
            with Given("I have some views"):
                node.query(f"CREATE VIEW {view0_name} AS SELECT y FROM {table0_name}")
                node.query(
                    f"CREATE VIEW {view1_name} AS SELECT y FROM {view0_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y<2)"
                )
                node.query(
                    f"CREATE VIEW {view2_name} AS SELECT y FROM {view1_name} JOIN {table2_name} USING y"
                )
                node.query(
                    f"CREATE VIEW {view3_name} AS SELECT y FROM {view2_name} UNION ALL SELECT y FROM {table3_name}"
                )

            with Then("I attempt to select from a view as the user"):
                node.query(
                    select_view_query.format(view3_name=view3_name),
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            for permutation in (
                [0, 1, 3, 5, 7, 13, 15, 23, 31, 45, 63, 95, 127, 173, 237, 247, 253],
                permutations(table_count=8),
            )[self.context.stress]:
                with grant_select_on_table(
                    node,
                    permutation,
                    grant_target_name,
                    view3_name,
                    table3_name,
                    view2_name,
                    view1_name,
                    table2_name,
                    view0_name,
                    table1_name,
                    table0_name,
                ) as tables_granted:
                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):
                        with Then("I attempt to select from a view as the user"):
                            node.query(
                                select_view_query.format(view3_name=view3_name),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all views"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=8)) + 1,
                    grant_target_name,
                    view0_name,
                    view1_name,
                    view2_name,
                    view3_name,
                    table0_name,
                    table1_name,
                    table2_name,
                    table3_name,
                ):
                    with Then("I attempt to select from a view as the user"):
                        node.query(
                            select_view_query.format(view3_name=view3_name),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the views"):
                with When("I drop view0", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view3_name}")
                with And("I drop view1", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view2_name}")
                with And("I drop view2", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view1_name}")
                with And("I drop view3", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view0_name}")


@TestScenario
def select_with_privilege_granted_directly_or_via_role_without_target_table_privilege(
    self, node=None
):
    """Check that user is able to select from a materialized view without target table SELECT privilege."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_without_target_table_privilege,
            name="select without target table privilege, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_without_target_table_privilege,
            name="select without target table privilege, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_without_target_table_privilege(
    self, grant_target_name, user_name, node=None
):
    """GRANT the user SELECT privilege on the view and check the user is able to successfully SELECT from the view without target table privilege."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    try:
        with Given("I have a view"):
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} TO {table_name} AS SELECT 1"
            )
        with When("I grant SELECT privilege on the view"):
            node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
        with Then("I attempt to select from a view as the user"):
            node.query(
                f"SELECT * FROM {view_name}", settings=[("user", f"{user_name}")]
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestSuite
def select_from_tables(self, node=None):
    """Testing RBAC functionality of SELECT for tables related to materialized views - target tables, source tables."""
    Scenario(
        run=select_from_implicit_target_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_from_explicit_target_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=select_from_source_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )


@TestScenario
@Requirements(RQ_SRS_006_RBAC_MaterializedView_Select_TargetTable("1.0"))
def select_from_implicit_target_table_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to SELECT from the implicit target table created from a materialized view
    if they have SELECT privilege on that table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_from_implicit_target_table,
            name="select from implicit target table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_from_implicit_target_table,
            name="select from implicit target table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_from_implicit_target_table(self, grant_target_name, user_name, node=None):
    """Grant SELECT on the implicit target table and check the user is able to SELECT only if they have SELECT privilege on the table."""
    view_name = f"view_{getuid()}"
    implicit_table_name = f'\\".inner.{view_name}\\"'
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    if node is None:
        node = self.context.node
    try:
        with Given("I have a view"):
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
            )

        with Then("I attempt to SELECT from the implicit target table as the user"):
            node.query(
                f"SELECT * FROM {implicit_table_name}",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

        with When("I grant SELECT privilege on the view"):
            node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
        with Then("I attempt to SELECT from the implicit target table as the user"):
            node.query(
                f"SELECT * FROM {implicit_table_name}",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

        with When("I grant SELECT privilege on the target table"):
            node.query(f"GRANT SELECT ON {implicit_table_name} TO {grant_target_name}")
        with Then("I attempt to SELECT from the implicit target table as the user"):
            node.query(
                f"SELECT * FROM {implicit_table_name}",
                settings=[("user", f"{user_name}")],
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
@Requirements(RQ_SRS_006_RBAC_MaterializedView_Select_TargetTable("1.0"))
def select_from_explicit_target_table_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to SELECT from the explicit target table created from a materialized view
    if they have SELECT privilege on that table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_from_explicit_target_table,
            name="select from explicit target table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_from_explicit_target_table,
            name="select from explicit target table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_from_explicit_target_table(self, grant_target_name, user_name, node=None):
    """Grant SELECT on the explicit target table and check the user is able to SELECT only if they have SELECT privilege on the table."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with Given("I have a view"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table_name} AS SELECT 1"
                )

            with Then("I attempt to SELECT from the explicit target table as the user"):
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT privilege on the view"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to SELECT from the explicit target table as the user"):
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT privilege on the target table"):
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")
            with Then("I attempt to SELECT from the explicit target table as the user"):
                node.query(
                    f"SELECT * FROM {table_name}", settings=[("user", f"{user_name}")]
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
@Requirements(RQ_SRS_006_RBAC_MaterializedView_Select_SourceTable("1.0"))
def select_from_source_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to SELECT from the source table of a materialized view
    if they have SELECT privilege on that table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=select_from_source_table,
            name="select from source table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=select_from_source_table,
            name="select from source table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def select_from_source_table(self, grant_target_name, user_name, node=None):
    """Grant SELECT on the source table and check the user is able to SELECT only if they have SELECT privilege on the table."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with Given("I have a view"):
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}"
                )

            with Then("I attempt to SELECT from the source table as the user"):
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT privilege on the view"):
                node.query(f"GRANT SELECT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to SELECT from the implicit target table as the user"):
                node.query(
                    f"SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                    exitcode=exitcode,
                    message=message,
                )

            with When("I grant SELECT privilege on the target table"):
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")
            with Then("I attempt to SELECT from the implicit target table as the user"):
                node.query(
                    f"SELECT * FROM {table_name}", settings=[("user", f"{user_name}")]
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Drop("1.0"),
)
def drop(self, node=None):
    """Test the RBAC functionality of the `DROP VIEW` command."""
    Scenario(
        run=drop_with_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=drop_with_revoked_privilege_revoked_directly_or_from_role,
        setup=instrument_clickhouse_server_log,
    )


@TestScenario
def drop_with_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to drop view with DROP VIEW privilege if the user has privilege directly or through a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(test=drop_with_privilege, name="drop privilege granted directly")(
            grant_target_name=user_name, user_name=user_name
        )

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=drop_with_privilege, name="drop privilege granted through a role"
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def drop_with_privilege(self, grant_target_name, user_name, node=None):
    """Grant DROP VIEW privilege and check the user is able to successfully drop a view."""
    view_name = f"view_{getuid()}"
    exitcode, message = errors.table_does_not_exist(name=f"default.{view_name}")

    if node is None:
        node = self.context.node
    try:
        with Given("I have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
            )

        with When("I grant DROP VIEW privilege"):
            node.query(f"GRANT DROP VIEW ON {view_name} TO {grant_target_name}")

        with And("I drop the view as the user"):
            node.query(f"DROP VIEW {view_name}", settings=[("user", f"{user_name}")])

        with Then("I check the table does not exist"):
            node.query(f"SELECT * FROM {view_name}", exitcode=exitcode, message=message)

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def drop_with_revoked_privilege_revoked_directly_or_from_role(self, node=None):
    """Check that user is unable to drop view with DROP VIEW privilege revoked directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=drop_with_revoked_privilege, name="drop privilege revoked directly"
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=drop_with_revoked_privilege, name="drop privilege revoked from a role"
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def drop_with_revoked_privilege(self, grant_target_name, user_name, node=None):
    """Revoke DROP VIEW privilege and check the user is unable to DROP a view."""
    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    try:
        with Given("I have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
            )

        with When("I grant DROP VIEW privilege"):
            node.query(f"GRANT DROP VIEW ON {view_name} TO {grant_target_name}")

        with And("I revoke DROP VIEW privilege"):
            node.query(f"REVOKE DROP VIEW ON {view_name} FROM {grant_target_name}")

        with Then("I drop the view as the user"):
            node.query(
                f"DROP VIEW {view_name}",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_ModifyQuery("1.0"),
)
def modify_query(self, node=None):
    """Test the RBAC functionality of the `MODIFY QUERY` command."""
    if node is None:
        node = self.context.node

    with allow_experimental_alter_materialized_view_structure(node):
        Scenario(
            run=modify_query_with_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_privilege_revoked_directly_or_from_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_without_source_table_privilege,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_source_table_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_subquery_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_join_query_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_union_query_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_join_union_subquery_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )
        Scenario(
            run=modify_query_with_nested_views_privilege_granted_directly_or_via_role,
            setup=instrument_clickhouse_server_log,
        )


@TestScenario
def modify_query_with_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to modify view with MODIFY QUERY if the user has privilege directly or through a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_privilege,
            name="modify query privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_privilege,
            name="modify query privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestScenario
def modify_query_with_privilege(self, grant_target_name, user_name, node=None):
    """Grant MODIFY QUERY and check that user is able to execute it."""
    view_name = f"view_{getuid()}"

    if node is None:
        node = self.context.node
    try:
        with Given("I have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
            )

        with When("I grant MODIFY QUERY privilege"):
            node.query(
                f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
            )

        with Then("I modify the view query as the user"):
            node.query(
                f"ALTER TABLE {view_name} MODIFY QUERY SELECT 2",
                settings=[("user", f"{user_name}")],
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_privilege_revoked_directly_or_from_role(self, node=None):
    """Check that user is unable to modify the view query with MODIFY QUERY if the privilege has been revoked, directly or from a role."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_revoked_privilege,
            name="modify query privilege revoked directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_revoked_privilege,
            name="modify query privilege revoked from a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestScenario
def modify_query_with_revoked_privilege(self, grant_target_name, user_name, node=None):
    """Revoke MODIFY QUERY and check that user is unable to modify the view query."""
    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    try:
        with Given("I have a view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")
            node.query(
                f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
            )

        with When("I grant MODIFY QUERY privilege"):
            node.query(
                f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
            )
        with And("I revoke MODIFY QUERY privilege"):
            node.query(
                f"REVOKE ALTER VIEW MODIFY QUERY ON {view_name} FROM {grant_target_name}"
            )

        with Then("I modify the view query as the user"):
            node.query(
                f"ALTER TABLE {view_name} MODIFY QUERY SELECT 2",
                settings=[("user", f"{user_name}")],
                exitcode=exitcode,
                message=message,
            )

    finally:
        with Finally("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_without_source_table_privilege(self, node=None):
    """Check that user is unable to modify the view query to have a source table in the stored query
    without SELECT privilege for the new source table.
    """
    user_name = f"user_{getuid()}"
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        with user(node, f"{user_name}"):
            try:
                with When("I create a view"):
                    node.query(f"DROP VIEW IF EXISTS {view_name}")
                    node.query(
                        f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                    )

                with And("I grant view MODIFY QUERY privilege to the user"):
                    node.query(
                        f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {user_name}"
                    )
                with Then(
                    "I attempt to use MODIFY QUERY on the view without privilege on the source table"
                ):
                    node.query(
                        f"ALTER TABLE {view_name} MODIFY QUERY SELECT * FROM {table_name}",
                        settings=[("user", f"{user_name}")],
                        exitcode=exitcode,
                        message=message,
                    )

            finally:
                with Finally("I drop the view"):
                    node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_source_table_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to modify the view query to have a source table in the stored query, if and only if
    the user has SELECT privilege for the view and the source table, either directly or from a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_source_table_privilege,
            name="modify query with source table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_source_table_privilege,
            name="modify query with source table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def modify_query_with_source_table_privilege(
    self, user_name, grant_target_name, node=None
):
    """Grant MODIFY QUERY privilege on view and SELECT privilege on the new source table and check the user is able to modify the view query."""
    view_name = f"view_{getuid()}"
    table_name = f"table_{getuid()}"

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with Given("I have a view with a source table"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                )

            with And("I grant view MODIFY QUERY privilege"):
                node.query(
                    f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
                )
            with And("I grant table SELECT privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {grant_target_name}")

            with Then("I check the user is able to modify the view query"):
                node.query(
                    f"ALTER TABLE {view_name} MODIFY QUERY SELECT * FROM {table_name}",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_subquery_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to modify the view query to use a query with two subqueries if and only if
    the user has MODIFY QUERY privilege for that view and SELECT for all the tables, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_subquery,
            name="modify query with subquery, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_subquery,
            name="modify query with subquery, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def modify_query_with_subquery(self, user_name, grant_target_name, node=None):
    """Grant MODIFY QUERY on the view and SELECT on tables included in the MODIFY QUERY statement and check the user is able to modify the view query
    if and only if they have SELECT privilege on all of the tables.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    modify_query_view_query = "ALTER TABLE {view_name} MODIFY QUERY SELECT * FROM {table0_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table2_name} WHERE y<2))"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name},{table2_name}"):
        try:
            with Given("I have a view with a subquery"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                )

            with When("I grant MODIFY QUERY privilege on view"):
                node.query(
                    f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
                )
            with Then("I attempt to modify the view query as the user"):
                node.query(
                    modify_query_view_query.format(
                        view_name=view_name,
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
                        with Then("I attempt to modify the view query as the user"):
                            node.query(
                                modify_query_view_query.format(
                                    view_name=view_name,
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
                    with Then("I attempt to modify the view query as the user"):
                        output = node.query(
                            modify_query_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                                table2_name=table2_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        ).output
                        assert output == "0", error()

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_join_query_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to modify the view query to use a query that includes a `JOIN` statement if and only if
    the user has SELECT privilege on all the tables and MODIFY QUERY privilege on the view, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_join_query,
            name="modify query with join, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_join_query,
            name="modify query with join, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def modify_query_with_join_query(self, user_name, grant_target_name, node=None):
    """Grant MODIFY QUERY on the view and SELECT on the tables in the modified query and check the user is able to modify the view query to
    use a `JOIN` statement if and only if they have SELECT privilege on all of them.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    modify_query_view_query = "ALTER TABLE {view_name} MODIFY QUERY  SELECT * FROM {table0_name} JOIN {table1_name} USING d"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view with a JOIN statement"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                )

            with When("I grant MODIFY QUERY privilege on view"):
                node.query(
                    f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
                )
            with Then("I attempt to modify the view query as the user"):
                node.query(
                    modify_query_view_query.format(
                        view_name=view_name,
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
                        with Then("I attempt to modify the view query as the user"):
                            node.query(
                                modify_query_view_query.format(
                                    view_name=view_name,
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
                    with Then("I attempt to modify the view query as the user"):
                        node.query(
                            modify_query_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_union_query_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to modify the view query to include a `UNION ALL` statement if and only if
    the user has SELECT privilege on all the tables and MODIFY QUERY on the view, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_union_query,
            name="modify query with union, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_union_query,
            name="modify query with union, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def modify_query_with_union_query(self, user_name, grant_target_name, node=None):
    """Grant MODIFY QUERY on the view and SELECT on the tables in the stored query and check the user is able modify the view query if and only if they have SELECT privilege on all of them."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    modify_query_view_query = "ALTER TABLE {view_name} MODIFY QUERY SELECT * FROM {table0_name} UNION ALL SELECT * FROM {table1_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view with a UNION statement"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                )

            with When("I grant MODIFY QUERY privilege on view"):
                node.query(
                    f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
                )
            with Then("I attempt to modify the view query as the user"):
                node.query(
                    modify_query_view_query.format(
                        view_name=view_name,
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
                        with Then("I attempt to modify the view query as the user"):
                            node.query(
                                modify_query_view_query.format(
                                    view_name=view_name,
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
                    with Then("I attempt to modify the view query as the user"):
                        node.query(
                            modify_query_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_join_union_subquery_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to modify the view query to include `UNION ALL`, `JOIN` and two subqueries
    if and only if the user has SELECT privilege on all the tables and the view, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_join_union_subquery,
            name="modify query with join union subquery, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_join_union_subquery,
            name="modify query with join union subquery, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def modify_query_with_join_union_subquery(
    self, grant_target_name, user_name, node=None
):
    """Grant MODIFY QUERY on the view and SELECT on tables in the modify query and check the user is able modify the view query
    if and only if they have SELECT privilege on all of them.
    """
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    table4_name = f"table4_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    modify_query_view_query = "ALTER TABLE {view_name} MODIFY QUERY SELECT y FROM {table0_name} JOIN {table1_name} USING y UNION ALL SELECT y FROM {table1_name} WHERE y IN (SELECT y FROM {table3_name} WHERE y IN (SELECT y FROM {table4_name} WHERE y<2))"

    if node is None:
        node = self.context.node
    with table(
        node, f"{table0_name},{table1_name},{table2_name},{table3_name},{table4_name}"
    ):
        try:
            with Given("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT 1"
                )

            with When("I grant MODIFY QUERY privilege on view"):
                node.query(
                    f"GRANT ALTER VIEW MODIFY QUERY ON {view_name} TO {grant_target_name}"
                )
            with Then("I attempt to modify the view query as the user"):
                node.query(
                    modify_query_view_query.format(
                        view_name=view_name,
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
                    table2_name,
                    table3_name,
                    table4_name,
                ) as tables_granted:
                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):
                        with Then("I attempt to modify the view query as the user"):
                            node.query(
                                modify_query_view_query.format(
                                    view_name=view_name,
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
                    with Then("I attempt to modify the view query as the user"):
                        node.query(
                            modify_query_view_query.format(
                                view_name=view_name,
                                table0_name=table0_name,
                                table1_name=table1_name,
                                table2_name=table2_name,
                                table3_name=table3_name,
                                table4_name=table4_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
def modify_query_with_nested_views_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to modify the view query to include other views if and only if
    the user has MODIFY QUERY privilege on the view SELECT privilege on all of the views and the source tables for those views, either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=modify_query_with_nested_views,
            name="modify query with nested views, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=modify_query_with_nested_views,
            name="modify query with nested views, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def modify_query_with_nested_views(self, grant_target_name, user_name, node=None):
    """Grant MODIFY QUERY on view and SELECT on the tables and views in the modified query and check the user is able to modify the view query
    if and only if they have SELECT privilege on all of them.
    """
    view0_name = f"view0_{getuid()}"
    view1_name = f"view1_{getuid()}"
    view2_name = f"view2_{getuid()}"
    view3_name = f"view3_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"
    table3_name = f"table3_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    modify_query_view_query = "ALTER TABLE {view3_name} MODIFY QUERY SELECT y FROM {view2_name} UNION ALL SELECT y FROM {table3_name}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name},{table2_name},{table3_name}"):
        try:
            with Given("I have some views"):
                node.query(f"CREATE VIEW {view0_name} AS SELECT y FROM {table0_name}")
                node.query(
                    f"CREATE VIEW {view1_name} AS SELECT y FROM {view0_name} WHERE y IN (SELECT y FROM {table1_name} WHERE y<2)"
                )
                node.query(
                    f"CREATE VIEW {view2_name} AS SELECT y FROM {view1_name} JOIN {table2_name} USING y"
                )
                node.query(f"CREATE VIEW {view3_name} AS SELECT 1")

            with When("I grant MODIFY QUERY privilege on view"):
                node.query(
                    f"GRANT ALTER VIEW MODIFY QUERY ON {view3_name} TO {grant_target_name}"
                )
            with Then("I attempt to modify the view query as the user"):
                node.query(
                    modify_query_view_query.format(
                        view3_name=view3_name,
                        view2_name=view2_name,
                        table3_name=table3_name,
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
                    view2_name,
                    table3_name,
                    view1_name,
                    table2_name,
                    view0_name,
                    table1_name,
                    table0_name,
                ) as tables_granted:
                    with When(
                        f"permutation={permutation}, tables granted = {tables_granted}"
                    ):
                        with Then("I attempt to modify the view query as the user"):
                            node.query(
                                modify_query_view_query.format(
                                    view3_name=view3_name,
                                    view2_name=view2_name,
                                    table3_name=table3_name,
                                ),
                                settings=[("user", f"{user_name}")],
                                exitcode=exitcode,
                                message=message,
                            )

            with When("I grant select on all views"):
                with grant_select_on_table(
                    node,
                    max(permutations(table_count=7)) + 1,
                    grant_target_name,
                    view0_name,
                    view1_name,
                    view2_name,
                    table0_name,
                    table1_name,
                    table2_name,
                    table3_name,
                ):
                    with Then("I attempt to modify the view query as the user"):
                        node.query(
                            modify_query_view_query.format(
                                view3_name=view3_name,
                                view2_name=view2_name,
                                table3_name=table3_name,
                            ),
                            settings=[("user", f"{user_name}")],
                        )

        finally:
            with Finally("I drop the views"):
                with When("I drop view0", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view3_name}")
                with And("I drop view1", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view2_name}")
                with And("I drop view2", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view1_name}")
                with And("I drop view3", flags=TE):
                    node.query(f"DROP VIEW IF EXISTS {view0_name}")


@TestSuite
def insert(self, node=None):
    """Check RBAC functionality of INSERT with materialized views."""
    Scenario(
        run=insert_on_source_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=insert_with_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )
    Scenario(
        run=insert_on_target_table_privilege_granted_directly_or_via_role,
        setup=instrument_clickhouse_server_log,
    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Insert_SourceTable("1.0"),
)
def insert_on_source_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to INSERT on the source table of the materialized view with only INSERT privilege on the source table."""
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=insert_on_source_table,
            name="insert on source table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=insert_on_source_table,
            name="insert on source table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def insert_on_source_table(self, grant_target_name, user_name, node=None):
    """Grant SELECT on the source table to the user and check they are able to SELECT from it."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table0_name} AS SELECT * FROM {table1_name}"
                )

            with When("I grant INSERT on the source table"):
                node.query(f"GRANT INSERT ON {table1_name} TO {grant_target_name}")
            with Then("I attempt to insert into the source table"):
                node.query(
                    f"INSERT INTO {table1_name}(d) VALUES ('2020-01-01')",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Insert("1.0"),
)
def insert_with_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to INSERT into materialized view if and only if they have INSERT privilege for the view,
    either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=insert_with_insert_privilege,
            name="insert on view, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=insert_with_insert_privilege,
            name="insert on view, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def insert_with_insert_privilege(self, grant_target_name, user_name, node=None):
    """Grant INSERT and check user is able to INSERT into the materialized view only if they have INSERT privilege for the view."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table0_name} AS SELECT * FROM {table1_name}"
                )

            with When("I grant INSERT on the view"):
                node.query(f"GRANT INSERT ON {view_name} TO {grant_target_name}")
            with Then("I attempt to insert into the view"):
                node.query(
                    f"INSERT INTO {view_name}(d) VALUES ('2020-01-01')",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Insert_TargetTable("1.0"),
)
def insert_on_target_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to INSERT into the target table of a materialized view if and only if they have INSERT privilege for the table,
    either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=insert_on_target_table,
            name="insert on target table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=insert_on_target_table,
            name="insert on target table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def insert_on_target_table(self, grant_target_name, user_name, node=None):
    """Grant INSERT and check user is able to INSERT into target table."""
    view_name = f"view_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    if node is None:
        node = self.context.node
    with table(node, f"{table0_name},{table1_name}"):
        try:
            with Given("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} TO {table0_name} AS SELECT * FROM {table1_name}"
                )

            with When("I grant INSERT on the target table"):
                node.query(f"GRANT INSERT ON {table0_name} TO {grant_target_name}")
            with Then("I attempt to insert into the target table"):
                node.query(
                    f"INSERT INTO {table0_name}(d) VALUES ('2020-01-01')",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView_Insert_TargetTable("1.0"),
)
def insert_on_implicit_target_table_privilege_granted_directly_or_via_role(
    self, node=None
):
    """Check that user is able to INSERT into the implicit target table of a materialized view if and only if they have INSERT privilege for the table,
    either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node
    with user(node, f"{user_name}"):
        Scenario(
            test=insert_on_target_table,
            name="insert on implicit target table, privilege granted directly",
        )(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(
            test=insert_on_target_table,
            name="insert on implicit target table, privilege granted through a role",
        )(grant_target_name=role_name, user_name=user_name)


@TestOutline
def insert_on_target_table(self, grant_target_name, user_name, node=None):
    """Grant INSERT and check user is able to INSERT into implicit target table."""
    view_name = f"view_{getuid()}"
    table_name = f"table0_{getuid()}"
    implicit_table_name = f'\\".inner.{view_name}\\"'

    if node is None:
        node = self.context.node
    with table(node, f"{table_name}"):
        try:
            with Given("I have a view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")
                node.query(
                    f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}"
                )

            with When("I grant INSERT on the target table"):
                node.query(
                    f"GRANT INSERT ON {implicit_table_name} TO {grant_target_name}"
                )
            with Then("I attempt to insert into the target table"):
                node.query(
                    f"INSERT INTO {implicit_table_name}(d) VALUES ('2020-01-01')",
                    settings=[("user", f"{user_name}")],
                )

        finally:
            with Finally("I drop the view"):
                node.query(f"DROP VIEW IF EXISTS {view_name}")


@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_MaterializedView("1.0"),
)
@Name("materialized view")
def feature(self, stress=None, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    if stress is not None:
        self.context.stress = stress

    with Pool(3) as pool:
        try:
            for suite in loads(current_module(), Suite):
                Suite(test=suite, parallel=True, executor=pool)
        finally:
            join()
