from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privilege_granted_directly_or_via_role(self, table_type, node=None):
    """Check that user is only able to execute OPTIMIZE when they have required privilege, either directly or via role.
    """
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege", setup=instrument_clickhouse_server_log):
        with user(node, user_name):

            with When(f"I run checks that {user_name} is only able to execute OPTIMIZE with required privileges"):
                privilege_check(grant_target_name=user_name, user_name=user_name, table_type=table_type, node=node)

    with Suite("user with privilege via role", setup=instrument_clickhouse_server_log):
        with user(node, user_name), role(node, role_name):

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(f"I run checks that {user_name} with {role_name} is only able to execute OPTIMIZE with required privileges"):
                privilege_check(grant_target_name=role_name, user_name=user_name, table_type=table_type, node=node)

def privilege_check(grant_target_name, user_name, table_type, node=None):
    """Run scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I attempt to optimize a table without privilege"):
                node.query(f"OPTIMIZE TABLE {table_name} FINAL", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("user with privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the optimize privilege"):
                node.query(f"GRANT OPTIMIZE ON {table_name} TO {grant_target_name}")

            with Then("I attempt to optimize a table"):
                node.query(f"OPTIMIZE TABLE {table_name}", settings = [("user", user_name)])

    with Scenario("user with revoked privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"

        with table(node, table_name, table_type):

            with When("I grant the optimize privilege"):
                node.query(f"GRANT OPTIMIZE ON {table_name} TO {grant_target_name}")

            with And("I revoke the optimize privilege"):
                node.query(f"REVOKE OPTIMIZE ON {table_name} FROM {grant_target_name}")

            with Then("I attempt to optimize a table"):
                node.query(f"OPTIMIZE TABLE {table_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("execute on cluster", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"

        try:
            with Given("I have a table on a cluster"):
                node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (d DATE, a String, b UInt8, x String, y Int8) ENGINE = MergeTree() PARTITION BY y ORDER BY d")

            with When("I grant the optimize privilege"):
                node.query(f"GRANT OPTIMIZE ON {table_name} TO {grant_target_name}")

            with Then("I attempt to optimize a table"):
                node.query(f"OPTIMIZE TABLE {table_name} ON CLUSTER sharded_cluster", settings = [("user", user_name)])

        finally:
            with Finally("I drop the table from the cluster"):
                node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Optimize("1.0"),
)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Name("optimize")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of OPTIMIZE.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        table_type, = example

        if table_type != "MergeTree" and not self.context.stress:
            continue

        with Example(str(example)):
            with Suite(test=privilege_granted_directly_or_via_role):
                privilege_granted_directly_or_via_role(table_type=table_type)
