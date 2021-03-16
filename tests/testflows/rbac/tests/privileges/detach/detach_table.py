from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is only able to execute DETACH TABLE when they have required privilege, either directly or via role.
    """
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege", setup=instrument_clickhouse_server_log):
        with user(node, user_name):

            with When(f"I run checks that {user_name} is only able to execute DETACH TABLE with required privileges"):
                privilege_check(grant_target_name=user_name, user_name=user_name, node=node)

    with Suite("user with privilege via role", setup=instrument_clickhouse_server_log):
        with user(node, user_name), role(node, role_name):

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(f"I run checks that {user_name} with {role_name} is only able to execute DETACH TABLE with required privileges"):
                privilege_check(grant_target_name=role_name, user_name=user_name, node=node)

def privilege_check(grant_target_name, user_name, node=None):
    """Run scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege", setup=instrument_clickhouse_server_log):
        table_name = f"table_{getuid()}"

        try:
            with Given("I have a table"):
                node.query(f"CREATE TABLE {table_name} (x Int8) ENGINE=Memory")

            with When("I attempt to detach a table without privilege"):
                node.query(f"DETACH TABLE {table_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I reattach the table", flags=TE):
                node.query(f"ATTACH TABLE IF NOT EXISTS {table_name}")
            with And("I drop the table", flags=TE):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

    with Scenario("user with privilege", setup=instrument_clickhouse_server_log):
        table_name = f"table_{getuid()}"

        try:
            with Given("I have a table"):
                node.query(f"CREATE TABLE {table_name} (x Int8) ENGINE=Memory")

            with When("I grant drop table privilege"):
                node.query(f"GRANT DROP TABLE ON *.* TO {grant_target_name}")

            with Then("I attempt to detach a table"):
                node.query(f"DETACH TABLE {table_name}", settings = [("user", user_name)])

        finally:
            with Finally("I reattach the table", flags=TE):
                node.query(f"ATTACH TABLE IF NOT EXISTS {table_name}")
            with And("I drop the table", flags=TE):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

    with Scenario("user with revoked privilege", setup=instrument_clickhouse_server_log):
        table_name = f"table_{getuid()}"

        try:
            with Given("I have a table"):
                node.query(f"CREATE TABLE {table_name} (x Int8) ENGINE=Memory")

            with When("I grant the drop table privilege"):
                node.query(f"GRANT DROP TABLE ON *.* TO {grant_target_name}")

            with And("I revoke the drop table privilege"):
                node.query(f"REVOKE DROP TABLE ON *.* FROM {grant_target_name}")

            with Then("I attempt to detach a table"):
                node.query(f"DETACH TABLE {table_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I reattach the table", flags=TE):
                node.query(f"ATTACH TABLE IF NOT EXISTS {table_name}")
            with And("I drop the table", flags=TE):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_DetachTable("1.0"),
)
@Name("detach table")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of DETACH TABLE.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    with Suite(test=privilege_granted_directly_or_via_role):
        privilege_granted_directly_or_via_role()
