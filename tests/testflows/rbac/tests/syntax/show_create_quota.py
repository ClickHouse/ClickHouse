from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *

@TestFeature
@Name("show create quota")
def feature(self, node="clickhouse1"):
    """Check show create quota query syntax.

    ```sql
    SHOW CREATE QUOTA [name | CURRENT]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(quota):
        try:
            with Given("I have a quota"):
                node.query(f"CREATE QUOTA {quota}")
            yield
        finally:
            with Finally("I drop the quota"):
                node.query(f"DROP QUOTA IF EXISTS {quota}")

    with Scenario("I show create quota", requirements=[
            RQ_SRS_006_RBAC_Quota_ShowCreateQuota_Name("1.0")]):
        with cleanup("quota0"):
            with When("I run show create quota command"):
                node.query("SHOW CREATE QUOTA quota0")

    with Scenario("I show create quota current", requirements=[
            RQ_SRS_006_RBAC_Quota_ShowCreateQuota_Current("1.0")]):
        with cleanup("quota1"):
            with When("I run show create quota command"):
                node.query("SHOW CREATE QUOTA CURRENT")

    with Scenario("I show create quota current short form", requirements=[
            RQ_SRS_006_RBAC_Quota_ShowCreateQuota_Current("1.0")]):
        with cleanup("quota2"):
            with When("I run show create quota command"):
                node.query("SHOW CREATE QUOTA")
