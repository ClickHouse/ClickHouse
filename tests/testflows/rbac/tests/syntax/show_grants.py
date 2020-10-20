from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *

@TestFeature
@Name("show grants")
def feature(self, node="clickhouse1"):
    """Check show grants query syntax.

    ```sql
    SHOW GRANTS [FOR user_or_role]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(user):
        try:
            with Given("I have a user"):
                node.query(f"CREATE USER {user}")
            yield
        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {user}")

    with Scenario("I show grants for user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Show_Grants_For("1.0")]):
        with setup("user0"):
            with When("I run show grants command"):
                node.query("SHOW GRANTS FOR user0")

    with Scenario("I show grants for current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Show_Grants("1.0")]):
        with When("I show grants"):
            node.query("SHOW GRANTS")