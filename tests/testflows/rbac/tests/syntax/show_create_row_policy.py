from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *

@TestFeature
@Name("show create row policy")
def feature(self, node="clickhouse1"):
    """Check show create row policy query syntax.

    ```sql
    SHOW CREATE [ROW] POLICY name ON [database.]table
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

        with Scenario("I show create row policy", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowCreateRowPolicy("1.0")]):
            with cleanup("policy0"):
                with When("I run show create row policy command"):
                    node.query("SHOW CREATE ROW POLICY policy0 ON default.foo")

        with Scenario("I show create row policy on a table", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowCreateRowPolicy_On("1.0")]):
            with cleanup("policy0"):
                with When("I run show create row policy command"):
                    node.query("SHOW CREATE ROW POLICY policy0 ON default.foo")

        with Scenario("I show create row policy using short syntax on a table", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_ShowCreateRowPolicy_On("1.0")]):
            with cleanup("policy1",on="foo"):
                with When("I run show create row policy command"):
                    node.query("SHOW CREATE POLICY policy1 ON foo")
    finally:
        with Finally("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS default.foo")
