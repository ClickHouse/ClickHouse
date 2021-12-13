from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *

@TestFeature
@Name("show row policies")
def feature(self, node="clickhouse1"):
    """Check show row polices query syntax.

    ```sql
    SHOW [ROW] POLICIES [ON [database.]table]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(policy, on="default.foo"):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {policy} ON {on}")
            yield
        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {policy} ON {on}")

    try:
        with Given("I have a table"):
            node.query(f"CREATE TABLE default.foo (x UInt64, y String) Engine=Memory")

        with Scenario("I show row policies", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies("1.0")]):
            with cleanup("policy0"):
                with When("I run drop row policy command"):
                    node.query("SHOW ROW POLICIES")

        with Scenario("I show row policies using short syntax", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies("1.0")]):
            with cleanup("policy1"):
                with When("I run drop row policy command"):
                    node.query("SHOW POLICIES")

        with Scenario("I show row policies on a database table", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies_On("1.0")]):
            with cleanup("policy0"):
                with When("I run drop row policy command"):
                    node.query("SHOW ROW POLICIES ON default.foo")

        with Scenario("I show row policies on a table", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies_On("1.0")]):
            with cleanup("policy0"):
                with When("I run drop row policy command"):
                    node.query("SHOW ROW POLICIES ON foo")

    finally:
        with Finally("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS default.foo")
