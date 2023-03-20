from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *


@TestFeature
@Name("show create user")
def feature(self, node="clickhouse1"):
    """Check show create user query syntax.

    ```
    SHOW CREATE USER [name | CURRENT_USER]
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

    with Scenario(
        "I run show create on user with no options",
        requirements=[RQ_SRS_006_RBAC_User_ShowCreateUser_For("1.0")],
    ):
        with setup("user0"):
            with When("I run show create user command"):
                node.query("SHOW CREATE USER user0")

    with Scenario(
        "I run show create on current user",
        requirements=[RQ_SRS_006_RBAC_User_ShowCreateUser("1.0")],
    ):
        with When("I show create the current user"):
            node.query("SHOW CREATE USER CURRENT_USER")
