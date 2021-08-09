from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

aliases = {"ALTER MOVE PARTITION", "ALTER MOVE PART", "MOVE PARTITION", "MOVE PART", "ALL"}

@TestSuite
def privilege_granted_directly_or_via_role(self, table_type, privilege, node=None):
    """Check that user is only able to execute ALTER MOVE PARTITION when they have required privilege, either directly or via role.
    """
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege", setup=instrument_clickhouse_server_log):
        with user(node, user_name):

            with When(f"I run checks that {user_name} is only able to execute ALTER MOVE PARTITION with required privileges"):
                privilege_check(grant_target_name=user_name, user_name=user_name, table_type=table_type, privilege=privilege, node=node)

    with Suite("user with privilege via role", setup=instrument_clickhouse_server_log):
        with user(node, user_name), role(node, role_name):

            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with And(f"I run checks that {user_name} with {role_name} is only able to execute ALTER MOVE PARTITION with required privileges"):
                privilege_check(grant_target_name=role_name, user_name=user_name, table_type=table_type, privilege=privilege, node=node)

def privilege_check(grant_target_name, user_name, table_type, privilege, node=None):
    """Run scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege", setup=instrument_clickhouse_server_log):
        source_table_name = f"source_merge_tree_{getuid()}"
        target_table_name = f"target_merge_tree_{getuid()}"

        with table(node, f"{source_table_name},{target_table_name}", table_type):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to move partition without privilege"):
                node.query(f"ALTER TABLE {source_table_name} MOVE PARTITION 1 TO TABLE {target_table_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("user without ALTER MOVE PARTITION privilege", setup=instrument_clickhouse_server_log):
        source_table_name = f"source_merge_tree_{getuid()}"
        target_table_name = f"target_merge_tree_{getuid()}"

        with table(node, f"{source_table_name},{target_table_name}", table_type):

            with When(f"I grant SELECT and ALTER DELETE privileges on {source_table_name} to {grant_target_name}"):
                node.query(f"GRANT SELECT, ALTER DELETE ON {source_table_name} TO {grant_target_name}")

            with And(f"I grant INSERT on {target_table_name} to {grant_target_name}"):
                node.query(f"GRANT INSERT ON {target_table_name} TO {grant_target_name}")

            with Then("I attempt to move partitions without ALTER MOVE privilege"):
                node.query(f"ALTER TABLE {source_table_name} MOVE PARTITION 1 TO TABLE {target_table_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("user with ALTER MOVE PARTITION privilege", setup=instrument_clickhouse_server_log):
        source_table_name = f"source_merge_tree_{getuid()}"
        target_table_name = f"target_merge_tree_{getuid()}"

        with table(node, f"{source_table_name},{target_table_name}", table_type):

            with When(f"I grant SELECT, ALTER DELETE, and ALTER MOVE PARTITION privileges on {source_table_name} to {grant_target_name}"):
                node.query(f"GRANT SELECT, ALTER DELETE, {privilege} ON {source_table_name} TO {grant_target_name}")

            with And(f"I grant INSERT on {target_table_name} to {grant_target_name}"):
                node.query(f"GRANT INSERT ON {target_table_name} TO {grant_target_name}")

            with Then("I attempt to move partitions with ALTER MOVE privilege"):
                node.query(f"ALTER TABLE {source_table_name} MOVE PARTITION 1 TO TABLE {target_table_name}", settings = [("user", user_name)])

    with Scenario("user with revoked ALTER MOVE PARTITION privilege", setup=instrument_clickhouse_server_log):
        source_table_name = f"source_merge_tree_{getuid()}"
        target_table_name = f"target_merge_tree_{getuid()}"

        with table(node, f"{source_table_name},{target_table_name}", table_type):

            with When(f"I grant SELECT, ALTER DELETE, and ALTER MOVE PARTITION privileges on {source_table_name} to {grant_target_name}"):
                node.query(f"GRANT SELECT, ALTER DELETE, {privilege} ON {source_table_name} TO {grant_target_name}")

            with And(f"I grant INSERT on {target_table_name} to {grant_target_name}"):
                node.query(f"GRANT INSERT ON {target_table_name} TO {grant_target_name}")

            with And("I revoke ALTER MOVE PARTITION privilege"):
                node.query(f"REVOKE {privilege} ON {source_table_name} FROM {grant_target_name}")

            with Then("I attempt to move partition"):
                node.query(f"ALTER TABLE {source_table_name} MOVE PARTITION 1 TO TABLE {target_table_name}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("move partition to source table of a materialized view", setup=instrument_clickhouse_server_log):
        source_table_name = f"source_merge_tree_{getuid()}"
        mat_view_name = f"mat_view_{getuid()}"
        mat_view_source_table_name = f"mat_view_source_merge_tree_{getuid()}"

        with table(node, f"{source_table_name},{mat_view_source_table_name}", table_type):

            try:
                with Given("I have a materialized view"):
                    node.query(f"CREATE MATERIALIZED VIEW {mat_view_name} ENGINE = {table_type} PARTITION BY y ORDER BY d AS SELECT * FROM {mat_view_source_table_name}")

                with When(f"I grant SELECT, ALTER DELETE, and ALTER MOVE PARTITION privileges on {source_table_name} to {grant_target_name}"):
                    node.query(f"GRANT SELECT, ALTER DELETE, {privilege} ON {source_table_name} TO {grant_target_name}")

                with And(f"I grant INSERT on {mat_view_source_table_name} to {grant_target_name}"):
                    node.query(f"GRANT INSERT ON {mat_view_source_table_name} TO {grant_target_name}")

                with Then("I attempt to move partitions with ALTER MOVE privilege"):
                    node.query(f"ALTER TABLE {source_table_name} MOVE PARTITION 1 TO TABLE {mat_view_source_table_name}", settings = [("user", user_name)])

            finally:
                with Finally("I drop the materialized view"):
                    node.query(f"DROP VIEW IF EXISTS {mat_view_name}")

    with Scenario("move partition to implicit target table of a materialized view", setup=instrument_clickhouse_server_log):
        source_table_name = f"source_merge_tree_{getuid()}"
        mat_view_name = f"mat_view_{getuid()}"
        mat_view_source_table_name = f"mat_view_source_merge_tree_{getuid()}"
        implicit_table_name = f"\\\".inner.{mat_view_name}\\\""

        with table(node, f"{source_table_name},{mat_view_source_table_name}", table_type):

            try:
                with Given("I have a materialized view"):
                    node.query(f"CREATE MATERIALIZED VIEW {mat_view_name} ENGINE = {table_type} PARTITION BY y ORDER BY d AS SELECT * FROM {mat_view_source_table_name}")

                with When(f"I grant SELECT, ALTER DELETE, and ALTER MOVE PARTITION privileges on {source_table_name} to {grant_target_name}"):
                    node.query(f"GRANT SELECT, ALTER DELETE, {privilege} ON {source_table_name} TO {grant_target_name}")

                with And(f"I grant INSERT on {implicit_table_name} to {grant_target_name}"):
                    node.query(f"GRANT INSERT ON {implicit_table_name} TO {grant_target_name}")

                with Then("I attempt to move partitions with ALTER MOVE privilege"):
                    node.query(f"ALTER TABLE {source_table_name} MOVE PARTITION 1 TO TABLE {implicit_table_name}", settings = [("user", user_name)])

            finally:
                with Finally("I drop the materialized view"):
                    node.query(f"DROP VIEW IF EXISTS {mat_view_name}")

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterMove("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Name("alter move")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of ALTER MOVE.
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
            for alias in aliases:
                with Suite(alias, test=privilege_granted_directly_or_via_role):
                    privilege_granted_directly_or_via_role(table_type=table_type, privilege=alias)
