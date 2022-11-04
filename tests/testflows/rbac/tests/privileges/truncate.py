from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors


@TestSuite
def privilege_granted_directly_or_via_role(self, table_type, node=None):
    """Check that user is only able to execute TRUNCATE when they have required privilege, either directly or via role."""
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege"):
        with user(node, user_name):

            with When(
                f"I run checks that {user_name} is only able to execute TRUNCATE with required privileges"
            ):
                privilege_check(
                    grant_target_name=user_name,
                    user_name=user_name,
                    table_type=table_type,
                    node=node,
                )

    with Suite("user with privilege via role"):
        with user(node, user_name), role(node, role_name):

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(
                f"I run checks that {user_name} with {role_name} is only able to execute TRUNCATE with required privileges"
            ):
                privilege_check(
                    grant_target_name=role_name,
                    user_name=user_name,
                    table_type=table_type,
                    node=node,
                )


def privilege_check(grant_target_name, user_name, table_type, node=None):
    """Run scenarios to check the user's access with different privileges."""
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to truncate a table without privilege"):
                node.query(
                    f"TRUNCATE TABLE {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario("user with privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the truncate privilege"):
                node.query(f"GRANT TRUNCATE ON {table_name} TO {grant_target_name}")

            with Then("I attempt to truncate a table"):
                node.query(
                    f"TRUNCATE TABLE {table_name}", settings=[("user", user_name)]
                )

    with Scenario("user with revoked privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the truncate privilege"):
                node.query(f"GRANT TRUNCATE ON {table_name} TO {grant_target_name}")

            with And("I revoke the truncate privilege"):
                node.query(f"REVOKE TRUNCATE ON {table_name} FROM {grant_target_name}")

            with Then("I attempt to truncate a table"):
                node.query(
                    f"TRUNCATE TABLE {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario("user with revoked ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the truncate privilege"):
                node.query(f"GRANT TRUNCATE ON {table_name} TO {grant_target_name}")

            with And("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

            with Then("I attempt to truncate a table"):
                node.query(
                    f"TRUNCATE TABLE {table_name}",
                    settings=[("user", user_name)],
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario("execute on cluster"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the truncate privilege"):
                node.query(f"GRANT TRUNCATE ON {table_name} TO {grant_target_name}")

            with Then("I attempt to truncate a table"):
                node.query(
                    f"TRUNCATE TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster",
                    settings=[("user", user_name)],
                )

    with Scenario("user with ALL privilege"):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I revoke ALL privilege"):
                node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

            with And("I grant ALL privilege"):
                node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

            with Then("I attempt to truncate a table"):
                node.query(
                    f"TRUNCATE TABLE {table_name}", settings=[("user", user_name)]
                )


@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Truncate("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
@Examples("table_type", [(key,) for key in table_types.keys()])
@Name("truncate")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of TRUNCATE."""
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        (table_type,) = example

        if table_type != "MergeTree" and not self.context.stress:
            continue

        with Example(str(example)):
            with Suite(
                test=privilege_granted_directly_or_via_role,
                setup=instrument_clickhouse_server_log,
            ):
                privilege_granted_directly_or_via_role(table_type=table_type)
