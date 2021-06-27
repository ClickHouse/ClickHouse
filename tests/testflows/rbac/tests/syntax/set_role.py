from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("set role")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check set role query syntax.

    ```
    SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(roles=0):
        try:
            with Given("I have some roles"):
                for i in range(roles):
                    node.query(f"CREATE ROLE role{i}")
            yield
        finally:
            with Finally("I drop the roles"):
                for i in range(roles):
                    node.query(f"DROP ROLE IF EXISTS role{i}")

    with Scenario("I set default role for current user", requirements=[
            RQ_SRS_006_RBAC_SetRole_Default("1.0")]):
        with When("I set default role for current user"):
            node.query("SET ROLE DEFAULT")

    with Scenario("I set no role for current user", requirements=[
            RQ_SRS_006_RBAC_SetRole_None("1.0")]):
            with When("I set no role for current user"):
                node.query("SET ROLE NONE")

    with Scenario("I set nonexistent role, throws exception", requirements=[
            RQ_SRS_006_RBAC_SetRole_None("1.0")]):
            with Given("I ensure that role role5 does not exist"):
                node.query("DROP ROLE IF EXISTS role5")
            with When("I set nonexistent role for current user"):
                exitcode, message = errors.role_not_found_in_disk("role5")
                node.query("SET ROLE role5", exitcode=exitcode, message=message)

    with Scenario("I set nonexistent role, throws exception", requirements=[
            RQ_SRS_006_RBAC_SetRole_None("1.0")]):
            with Given("I ensure that role role5 does not exist"):
                node.query("DROP ROLE IF EXISTS role5")
            with When("I set nonexistent role for current user"):
                exitcode, message = errors.role_not_found_in_disk("role5")
                node.query("SET ROLE ALL EXCEPT role5", exitcode=exitcode, message=message)

    with Scenario("I set one role for current user", requirements=[
            RQ_SRS_006_RBAC_SetRole("1.0")]):
        with setup(1):
            with Given("I have a user"):
                node.query("CREATE USER OR REPLACE user0")
            with And("I grant user a role"):
                node.query("GRANT role0 TO user0")
            with When("I set role for the user"):
                node.query("SET ROLE role0", settings = [("user","user0")])
            with Finally("I drop the user"):
                node.query("DROP USER user0")

    with Scenario("I set multiple roles for current user", requirements=[
            RQ_SRS_006_RBAC_SetRole("1.0")]):
        with setup(2):
            with Given("I have a user"):
                node.query("CREATE USER OR REPLACE user0")
            with And("I grant user a role"):
                node.query("GRANT role0, role1 TO user0")
            with When("I set roles for the user"):
                node.query("SET ROLE role0, role1", settings = [("user","user0")])
            with Finally("I drop the user"):
                node.query("DROP USER user0")

    with Scenario("I set all roles for current user", requirements=[
            RQ_SRS_006_RBAC_SetRole_All("1.0")]):
        with When("I set all roles for current user"):
            node.query("SET ROLE ALL")

    with Scenario("I set all roles except one for current user", requirements=[
            RQ_SRS_006_RBAC_SetRole_AllExcept("1.0")]):
        with setup(1):
            with When("I run set role command"):
                node.query("SET ROLE ALL EXCEPT role0")