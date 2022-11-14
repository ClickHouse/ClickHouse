from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def no_privilege(self, node=None):
    """Check that user doesn't need privileges to execute `KILL MUTATION` with no mutations."""
    if node is None:
        node = self.context.node

    with Scenario("kill mutation on a table"):
        user_name = f"user_{getuid()}"
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with user(node, user_name):

                with When("I grant the user NONE privilege"):
                    node.query(f"GRANT NONE TO {user_name}")

                with And("I grant the user USAGE privilege"):
                    node.query(f"GRANT USAGE ON *.* TO {user_name}")

                with Then("I attempt to kill mutation on table"):
                    node.query(
                        f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                        settings=[("user", user_name)],
                    )

    with Scenario("kill mutation on cluster"):
        user_name = f"user_{getuid()}"
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with user(node, user_name):

                with When("I grant the user NONE privilege"):
                    node.query(f"GRANT NONE TO {user_name}")

                with And("I grant the user USAGE privilege"):
                    node.query(f"GRANT USAGE ON *.* TO {user_name}")

                with Then("I attempt to kill mutation on cluster"):
                    node.query(
                        f"KILL MUTATION ON CLUSTER sharded_cluster WHERE database = 'default' AND table = '{table_name}'",
                        settings=[("user", user_name)],
                    )


@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `KILL MUTATION` on a table with a mutation
    if and only if the user has privilege matching the source of the mutation on that table.
    For example, to execute `KILL MUTATION` after `ALTER UPDATE`, the user needs `ALTER UPDATE` privilege.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(test=update)(user_name=user_name, grant_target_name=user_name)
        Suite(test=delete)(user_name=user_name, grant_target_name=user_name)
        Suite(test=drop_column)(user_name=user_name, grant_target_name=user_name)


@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `KILL MUTATION` on a table with a mutation
    if and only if the user has privilege matching the source of the mutation on that table.
    For example, to execute `KILL MUTATION` after `ALTER UPDATE`, the user needs `ALTER UPDATE` privilege.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(test=update)(user_name=user_name, grant_target_name=role_name)
        Suite(test=delete)(user_name=user_name, grant_target_name=role_name)
        Suite(test=drop_column)(user_name=user_name, grant_target_name=role_name)


@TestSuite
@Requirements(RQ_SRS_006_RBAC_Privileges_KillMutation_AlterUpdate("1.0"))
def update(self, user_name, grant_target_name, node=None):
    """Check that the user is able to execute `KILL MUTATION` after `ALTER UPDATE`
    if and only if the user has `ALTER UPDATE` privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Given("The user has no privilege"):
        node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

    with Scenario("KILL ALTER UPDATE without privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER UPDATE mutation"):
                node.query(f"ALTER TABLE {table_name} UPDATE a = x WHERE 1")

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER UPDATE with privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER UPDATE mutation"):
                node.query(f"ALTER TABLE {table_name} UPDATE a = x WHERE 1")

            with When("I grant the ALTER UPDATE privilege"):
                node.query(f"GRANT ALTER UPDATE ON {table_name} TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                )

    with Scenario("KILL ALTER UPDATE with revoked privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER UPDATE mutation"):
                node.query(f"ALTER TABLE {table_name} UPDATE a = x WHERE 1")

            with When("I grant the ALTER UPDATE privilege"):
                node.query(f"GRANT ALTER UPDATE ON {table_name} TO {grant_target_name}")

            with And("I revoke the ALTER UPDATE privilege"):
                node.query(
                    f"REVOKE ALTER UPDATE ON {table_name} FROM {grant_target_name}"
                )

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER UPDATE with revoked ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER UPDATE mutation"):
                node.query(f"ALTER TABLE {table_name} UPDATE a = x WHERE 1")

            with When("I grant the ALTER UPDATE privilege"):
                node.query(f"GRANT ALTER UPDATE ON {table_name} TO {grant_target_name}")

            with And("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER UPDATE with ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER UPDATE mutation"):
                node.query(f"ALTER TABLE {table_name} UPDATE a = x WHERE 1")

            with When("I grant the ALL privilege"):
                node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                )


@TestSuite
@Requirements(RQ_SRS_006_RBAC_Privileges_KillMutation_AlterDelete("1.0"))
def delete(self, user_name, grant_target_name, node=None):
    """Check that the user is able to execute `KILL MUTATION` after `ALTER DELETE`
    if and only if the user has `ALTER DELETE` privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Given("The user has no privilege"):
        node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

    with Scenario("KILL ALTER DELETE without privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DELETE mutation"):
                node.query(f"ALTER TABLE {table_name} DELETE WHERE 1")

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER DELETE with privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DELETE mutation"):
                node.query(f"ALTER TABLE {table_name} DELETE WHERE 1")

            with When("I grant the ALTER DELETE privilege"):
                node.query(f"GRANT ALTER DELETE ON {table_name} TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                )

    with Scenario("KILL ALTER DELETE with revoked privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DELETE mutation"):
                node.query(f"ALTER TABLE {table_name} DELETE WHERE 1")

            with When("I grant the ALTER DELETE privilege"):
                node.query(f"GRANT ALTER DELETE ON {table_name} TO {grant_target_name}")

            with And("I revoke the ALTER DELETE privilege"):
                node.query(
                    f"REVOKE ALTER DELETE ON {table_name} FROM {grant_target_name}"
                )

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER DELETE with revoked ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DELETE mutation"):
                node.query(f"ALTER TABLE {table_name} DELETE WHERE 1")

            with When("I grant the ALTER DELETE privilege"):
                node.query(f"GRANT ALTER DELETE ON {table_name} TO {grant_target_name}")

            with And("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER DELETE with ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DELETE mutation"):
                node.query(f"ALTER TABLE {table_name} DELETE WHERE 1")

            with When("I grant the ALL privilege"):
                node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                )


@TestSuite
@Requirements(RQ_SRS_006_RBAC_Privileges_KillMutation_AlterDropColumn("1.0"))
def drop_column(self, user_name, grant_target_name, node=None):
    """Check that the user is able to execute `KILL MUTATION` after `ALTER DROP COLUMN`
    if and only if the user has `ALTER DROP COLUMN` privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Given("The user has no privilege"):
        node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

    with Scenario("KILL ALTER DROP COLUMN without privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DROP COLUMN mutation"):
                node.query(f"ALTER TABLE {table_name} DROP COLUMN x")

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER DROP COLUMN with privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DROP COLUMN mutation"):
                node.query(f"ALTER TABLE {table_name} DROP COLUMN x")

            with When("I grant the ALTER DROP COLUMN privilege"):
                node.query(
                    f"GRANT ALTER DROP COLUMN ON {table_name} TO {grant_target_name}"
                )

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                )

    with Scenario("KILL ALTER DROP COLUMN with revoked privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DROP COLUMN mutation"):
                node.query(f"ALTER TABLE {table_name} DROP COLUMN x")

            with When("I grant the ALTER DROP COLUMN privilege"):
                node.query(
                    f"GRANT ALTER DROP COLUMN ON {table_name} TO {grant_target_name}"
                )

            with And("I revoke the ALTER DROP COLUMN privilege"):
                node.query(
                    f"REVOKE ALTER DROP COLUMN ON {table_name} FROM {grant_target_name}"
                )

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER DROP COLUMN with revoked privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DROP COLUMN mutation"):
                node.query(f"ALTER TABLE {table_name} DROP COLUMN x")

            with When("I grant the ALTER DROP COLUMN privilege"):
                node.query(
                    f"GRANT ALTER DROP COLUMN ON {table_name} TO {grant_target_name}"
                )

            with And("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message="Exception: Not allowed to kill mutation.",
                )

    with Scenario("KILL ALTER DROP COLUMN with ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name):

            with Given("I have an ALTER DROP COLUMN mutation"):
                node.query(f"ALTER TABLE {table_name} DROP COLUMN x")

            with When("I grant the ALL privilege"):
                node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

            with Then("I try to KILL MUTATION"):
                node.query(
                    f"KILL MUTATION WHERE database = 'default' AND table = '{table_name}'",
                    settings=[("user", user_name)],
                )


@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_KillMutation("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
@Name("kill mutation")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of KILL MUTATION."""
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    Suite(run=no_privilege, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
