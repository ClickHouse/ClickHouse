from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("drop quota")
def feature(self, node="clickhouse1"):
    """Check drop quota query syntax.

    ```sql
    DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
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

    def cleanup_quota(quota):
        with Given(f"I ensure that quota {quota} does not exist"):
            node.query(f"DROP QUOTA IF EXISTS {quota}")

    with Scenario("I drop quota with no options", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop("1.0")]):
        with cleanup("quota0"):
            with When("I run drop quota command"):
                node.query("DROP QUOTA quota0")

    with Scenario("I drop quota, does not exist, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop("1.0")]):
        quota = "quota0"
        cleanup_quota(quota)
        with When("I run drop quota command, throws exception"):
            exitcode, message = errors.quota_not_found_in_disk(name=quota)
            node.query(f"DROP QUOTA {quota}", exitcode=exitcode, message=message)
        del quota

    with Scenario("I drop quota if exists, quota exists", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop_IfExists("1.0")]):
        with cleanup("quota1"):
            with When("I run drop quota command"):
                node.query("DROP QUOTA IF EXISTS quota1")

    with Scenario("I drop quota if exists, quota does not exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop_IfExists("1.0")]):
        cleanup_quota("quota2")
        with When("I run drop quota command, quota does not exist"):
            node.query("DROP QUOTA IF EXISTS quota2")

    with Scenario("I drop default quota, throws error", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop("1.0")]):
        with When("I drop default quota"):
            exitcode, message = errors.cannot_remove_quota_default()
            node.query("DROP QUOTA default", exitcode=exitcode, message=message)

    with Scenario("I drop multiple quotas", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop("1.0")]):
        with cleanup("quota2"), cleanup("quota3"):
            with When("I run drop quota command"):
                node.query("DROP QUOTA quota2, quota3")

    with Scenario("I drop quota on cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop_Cluster("1.0")]):
        try:
            with Given("I have a quota"):
                node.query("CREATE QUOTA quota4 ON CLUSTER sharded_cluster")
            with When("I run drop quota command"):
                node.query("DROP QUOTA quota4 ON CLUSTER sharded_cluster")
        finally:
            with Finally("I drop the quota in case it still exists"):
                node.query("DROP QUOTA IF EXISTS quota4 ON CLUSTER sharded_cluster")

    with Scenario("I drop quota on fake cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Quota_Drop_Cluster("1.0")]):
        with When("I run drop quota command"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("DROP QUOTA quota5 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)
