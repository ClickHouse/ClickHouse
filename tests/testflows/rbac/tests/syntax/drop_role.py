from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("drop role")
def feature(self, node="clickhouse1"):
    """Check drop role query syntax.

    ```sql
    DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
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
            with Finally("I confirm the role is dropped"):
                node.query(f"DROP ROLE IF EXISTS {role}")

    def cleanup_role(role):
        with Given(f"I ensure that role {role} does not exist"):
                node.query(f"DROP ROLE IF EXISTS {role}")


    with Scenario("I drop role with no options", requirements=[
            RQ_SRS_006_RBAC_Role_Drop("1.0")]):
        with setup("role0"):
            with When("I drop role"):
                node.query("DROP ROLE role0")

    with Scenario("I drop role that doesn't exist, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Drop("1.0")]):
        role = "role0"
        cleanup_role(role)
        with When(f"I drop role {role}"):
            exitcode, message = errors.role_not_found_in_disk(name=role)
            node.query(f"DROP ROLE {role}", exitcode=exitcode, message=message)
        del role

    with Scenario("I drop multiple roles", requirements=[
            RQ_SRS_006_RBAC_Role_Drop("1.0")]):
        with setup("role1"), setup("role2"):
            with When("I drop multiple roles"):
                node.query("DROP ROLE role1, role2")

    with Scenario("I drop role that does not exist, using if exists", requirements=[
            RQ_SRS_006_RBAC_Role_Drop_IfExists("1.0")]):
        with When("I drop role if exists"):
            node.query("DROP ROLE IF EXISTS role3")

    with Scenario("I drop multiple roles where one does not exist", requirements=[
            RQ_SRS_006_RBAC_Role_Drop_IfExists("1.0")]):
        with setup("role5"):
            with When("I drop multiple roles where one doesnt exist"):
                node.query("DROP ROLE IF EXISTS role3, role5")

    with Scenario("I drop multiple roles where both do not exist", requirements=[
            RQ_SRS_006_RBAC_Role_Drop_IfExists("1.0")]):
        with Given("I ensure role does not exist"):
            node.query("DROP ROLE IF EXISTS role6")
        with When("I drop the nonexistant roles"):
            node.query("DROP USER IF EXISTS role5, role6")

    with Scenario("I drop role on cluster", requirements=[
            RQ_SRS_006_RBAC_Role_Drop_Cluster("1.0")]):
        with Given("I have a role on cluster"):
            node.query("CREATE ROLE OR REPLACE role0 ON CLUSTER sharded_cluster")
        with When("I drop the role from the cluster"):
            node.query("DROP ROLE IF EXISTS role0 ON CLUSTER sharded_cluster")

    with Scenario("I drop role on fake cluster", requirements=[
                RQ_SRS_006_RBAC_Role_Drop_Cluster("1.0")]):
        with When("I run drop role command"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("DROP ROLE role2 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)
