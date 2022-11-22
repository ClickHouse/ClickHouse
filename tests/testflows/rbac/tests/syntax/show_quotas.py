from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *


@TestFeature
@Name("show quotas")
def feature(self, node="clickhouse1"):
    """Check show quotas query syntax.

    ```sql
    SHOW QUOTAS
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(quota):
        try:
            with Given("I have a quota"):
                node.query(f"CREATE QUOTA OR REPLACE {quota}")
            yield
        finally:
            with Finally("I drop the quota"):
                node.query(f"DROP QUOTA IF EXISTS {quota}")

    with Scenario(
        "I show quotas", requirements=[RQ_SRS_006_RBAC_Quota_ShowQuotas("1.0")]
    ):
        with cleanup("quota0"), cleanup("quota1"):
            with When("I run show quota command"):
                node.query("SHOW QUOTAS")

    with Scenario(
        "I show quotas into outfile",
        requirements=[RQ_SRS_006_RBAC_Quota_ShowQuotas_IntoOutfile("1.0")],
    ):
        with cleanup("quota0"), cleanup("quota1"):
            with When("I run show quota command"):
                node.query("SHOW QUOTAS INTO OUTFILE 'quotas.txt'")

    with Scenario(
        "I show quotas with format",
        requirements=[RQ_SRS_006_RBAC_Quota_ShowQuotas_Format("1.0")],
    ):
        with cleanup("quota0"), cleanup("quota1"):
            with When("I run show quota command"):
                node.query("SHOW QUOTAS FORMAT TabSeparated")

    with Scenario(
        "I show quotas with settings",
        requirements=[RQ_SRS_006_RBAC_Quota_ShowQuotas("1.0")],
    ):
        with cleanup("quota0"), cleanup("quota1"):
            with When("I run show quota command"):
                node.query("SHOW QUOTAS SETTINGS max_memory_usage=5")
