from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("drop row policy")
def feature(self, node="clickhouse1"):
    """Check drop row policy query syntax.

    ```sql
    DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(policy, on=["default.foo"]):
        try:
            with Given("I have a row policy"):
                for i in policy:
                    for j in on:
                        node.query(f"CREATE ROW POLICY OR REPLACE {i} ON {j}")
            yield
        finally:
            with Finally("I drop the row policy"):
                for i in policy:
                    for j in on:
                        node.query(f"DROP ROW POLICY IF EXISTS {i} ON {j}")

    def cleanup_policy(policy, on="default.foo"):
        with Given(f"I ensure that policy {policy} does not exist"):
            node.query(f"DROP ROW POLICY IF EXISTS {policy} ON {on}")

    try:
        with Given("I have some tables"):
            node.query(f"CREATE TABLE default.foo (x UInt64, y String) Engine=Memory")
            node.query(f"CREATE TABLE default.foo2 (x UInt64, y String) Engine=Memory")

        with Scenario("I drop row policy with no options", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with cleanup(["policy1"]):
                with When("I drop row policy"):
                    node.query("DROP ROW POLICY policy1 ON default.foo")

        with Scenario("I drop row policy using short syntax with no options", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with cleanup(["policy2"]):
                with When("I drop row policy short form"):
                    node.query("DROP POLICY policy2 ON default.foo")

        with Scenario("I drop row policy, does not exist, throws exception", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            policy = "policy1"
            cleanup_policy(policy)
            with When("I drop row policy"):
                exitcode, message = errors.row_policy_not_found_in_disk(name=f"{policy} ON default.foo")
                node.query(f"DROP ROW POLICY {policy} ON default.foo", exitcode=exitcode, message=message)
            del policy

        with Scenario("I drop row policy if exists, policy does exist", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop_IfExists("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with cleanup(["policy3"]):
                with When("I drop row policy if exists"):
                    node.query("DROP ROW POLICY IF EXISTS policy3 ON default.foo")

        with Scenario("I drop row policy if exists, policy doesn't exist", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop_IfExists("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            cleanup_policy("policy3")
            with When("I drop row policy if exists"):
                node.query("DROP ROW POLICY IF EXISTS policy3 ON default.foo")

        with Scenario("I drop multiple row policies", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with cleanup(["policy3", "policy4"]):
                with When("I drop multiple row policies"):
                    node.query("DROP ROW POLICY policy3, policy4 ON default.foo")

        with Scenario("I drop row policy on multiple tables", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with cleanup(["policy3"], ["default.foo","default.foo2"]):
                with When("I drop row policy on multiple tables"):
                    node.query("DROP ROW POLICY policy3 ON default.foo, default.foo2")

        with Scenario("I drop multiple row policies on multiple tables", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with cleanup(["policy3", "policy4"], ["default.foo","default.foo2"]):
                with When("I drop the row policies from the tables"):
                    node.query("DROP ROW POLICY policy3 ON default.foo, policy4 ON default.foo2")

        with Scenario("I drop row policy on cluster", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            try:
                with Given("I have a row policy"):
                    node.query("CREATE ROW POLICY policy13 ON default.foo ON CLUSTER sharded_cluster")
                with When("I run drop row policy command"):
                    node.query("DROP ROW POLICY IF EXISTS policy13 ON CLUSTER sharded_cluster ON default.foo")
            finally:
                with Finally("I drop the row policy in case it still exists"):
                    node.query("DROP ROW POLICY IF EXISTS policy13 ON default.foo ON CLUSTER sharded_cluster")

        with Scenario("I drop row policy on cluster after table", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            try:
                with Given("I have a row policy"):
                    node.query("CREATE ROW POLICY policy12 ON default.foo ON CLUSTER sharded_cluster")
                with When("I run drop row policy command"):
                    node.query("DROP ROW POLICY IF EXISTS policy13 ON default.foo ON CLUSTER sharded_cluster")
            finally:
                with Finally("I drop the row policy in case it still exists"):
                    node.query("DROP ROW POLICY IF EXISTS policy12 ON default.foo ON CLUSTER sharded_cluster")

        with Scenario("I drop row policy on fake cluster throws exception", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Drop_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Drop_On("1.0")]):
            with When("I run drop row policy command"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("DROP ROW POLICY IF EXISTS policy14 ON default.foo ON CLUSTER fake_cluster",
                            exitcode=exitcode, message=message)
    finally:
        with Finally("I drop the tables"):
            node.query(f"DROP TABLE IF EXISTS default.foo")
            node.query(f"DROP TABLE IF EXISTS default.foo2")
