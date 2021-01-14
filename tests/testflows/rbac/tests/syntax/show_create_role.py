from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("show create role")
def feature(self, node="clickhouse1"):
    """Check show create role query syntax.

    ```sql
    SHOW CREATE ROLE name
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(role):
        try:
            with Given("I have a role"):
                node.query(f"CREATE ROLE OR REPLACE {role}")
            yield
        finally:
            with Finally("I drop the role"):
                node.query(f"DROP ROLE IF EXISTS {role}")

    with Scenario("I show create role", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Role_ShowCreate("1.0")]):
        with setup("role0"):
            with When("I run show create role command"):
                node.query("SHOW CREATE ROLE role0")

    with Scenario("I show create role, role doesn't exist, exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Role_ShowCreate("1.0")]):
        with When("I run show create role to catch an exception"):
            exitcode, message = errors.role_not_found_in_disk(name="role0")
            node.query("SHOW CREATE ROLE role0", exitcode=exitcode, message=message)