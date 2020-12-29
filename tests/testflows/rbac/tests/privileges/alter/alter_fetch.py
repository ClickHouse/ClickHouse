from multiprocessing.dummy import Pool

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

aliases = {"ALTER FETCH PARTITION", "FETCH PARTITION"}

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterFetch_Access("1.0"),
)
def privilege_granted_directly_or_via_role(self, table_type, privilege, node=None):
    """Check that user is only able to execute ALTER FETCH PARTITION when they have required privilege, either directly or via role.
    """
    role_name = f"role_{getuid()}"
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with Suite("user with direct privilege", setup=instrument_clickhouse_server_log):
        with user(node, user_name):
            with When(f"I run checks that {user_name} is only able to execute ALTER FETCH PARTITION with required privileges"):
                privilege_check(grant_target_name=user_name, user_name=user_name, table_type=table_type, privilege=privilege, node=node)

    with Suite("user with privilege via role", setup=instrument_clickhouse_server_log):
        with user(node, user_name), role(node, role_name):
            with When("I grant the role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")
            with And(f"I run checks that {user_name} with {role_name} is only able to execute ALTER FETCH PARTITION with required privileges"):
                privilege_check(grant_target_name=role_name, user_name=user_name, table_type=table_type, privilege=privilege, node=node)

def privilege_check(grant_target_name, user_name, table_type, privilege, node=None):
    """Run scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, table_name, table_type):
            with When("I attempt to fetch a partition without privilege"):
                node.query(f"ALTER TABLE {table_name} FETCH PARTITION 1 FROM '/clickhouse/tables/{{shard}}/{table_name}'", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("user with privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, table_name, table_type):
            with When("I grant the fetch privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_target_name}")
            with Then("I attempt to fetch a partition"):
                node.query(f"ALTER TABLE {table_name} FETCH PARTITION 1 FROM '/clickhouse/tables/{{shard}}/{table_name}'", settings = [("user", user_name)],
                    exitcode=231, message="DB::Exception: No node")

    with Scenario("user with revoked privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, table_name, table_type):
            with When("I grant the fetch privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_target_name}")
            with And("I revoke the fetch privilege"):
                node.query(f"REVOKE {privilege} ON {table_name} FROM {grant_target_name}")
            with Then("I attempt to fetch a partition"):
                node.query(f"ALTER TABLE {table_name} FETCH PARTITION 1 FROM '/clickhouse/tables/{{shard}}/{table_name}'", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterFetch("1.0"),
)
@Examples("table_type",[
    ("ReplicatedMergeTree-sharded_cluster",),
    ("ReplicatedMergeTree-one_shard_cluster",),
    ("ReplicatedReplacingMergeTree-sharded_cluster",),
    ("ReplicatedReplacingMergeTree-one_shard_cluster",),
    ("ReplicatedSummingMergeTree-sharded_cluster",),
    ("ReplicatedSummingMergeTree-one_shard_cluster",),
    ("ReplicatedAggregatingMergeTree-sharded_cluster",),
    ("ReplicatedAggregatingMergeTree-one_shard_cluster",),
    ("ReplicatedCollapsingMergeTree-sharded_cluster",),
    ("ReplicatedCollapsingMergeTree-one_shard_cluster",),
    ("ReplicatedVersionedCollapsingMergeTree-sharded_cluster",),
    ("ReplicatedVersionedCollapsingMergeTree-one_shard_cluster",),
    ("ReplicatedGraphiteMergeTree-sharded_cluster",),
    ("ReplicatedGraphiteMergeTree-one_shard_cluster",)
])
@Name("alter fetch")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of ALTER FETCH.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        table_type, = example

        if table_type != "ReplicatedMergeTree-sharded_cluster" and not self.context.stress:
            continue

        with Example(str(example)):
            for alias in aliases:
                with Suite(alias, test=privilege_granted_directly_or_via_role):
                    privilege_granted_directly_or_via_role(table_type=table_type, privilege=alias)
