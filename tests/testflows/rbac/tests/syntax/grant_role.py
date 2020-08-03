from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *
import rbac.tests.errors as errors

@TestFeature
@Name("grant role")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check grant query syntax.
    
    ```sql
    GRANT ON CLUSTER [cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(users=0,roles=0):
        try:
            with Given("I have some users and roles"):
                for i in range(users):
                    node.query(f"CREATE USER OR REPLACE user{i}")
                for j in range(roles):
                    node.query(f"CREATE ROLE OR REPLACE role{j}")
            yield
        finally:
            with Finally("I drop the users and roles"):
                for i in range(users):
                    node.query(f"DROP USER IF EXISTS user{i}")
                for j in range(roles):
                    node.query(f"DROP ROLE IF EXISTS role{j}")

    with Scenario("I grant a role to a user",flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role("1.0")]):
        with setup(1,1):
            with When("I grant a role"):
                node.query("GRANT role0 TO user0")

    with Scenario("I grant a nonexistent role to user", requirements=[
            RQ_SRS_006_RBAC_Grant_Role("1.0")]):
        with setup(1,0):
            with When("I grant nonexistent role to a user"):
                exitcode, message = errors.role_not_found_in_disk(name="role0")
                node.query("GRANT role0 TO user0", exitcode=exitcode, message=message)

    # with nonexistent object name, GRANT assumes type role (treats user0 as role)
    with Scenario("I grant a role to a nonexistent user", requirements=[
            RQ_SRS_006_RBAC_Grant_Role("1.0")]):
        with setup(0,1):
            with When("I grant role to a nonexistent user"):
                exitcode, message = errors.role_not_found_in_disk(name="user0")
                node.query("GRANT role0 TO user0", exitcode=exitcode, message=message)

    with Scenario("I grant a nonexistent role to a nonexistent user", requirements=[
            RQ_SRS_006_RBAC_Grant_Role("1.0")]):
        with setup(0,0):
            with When("I grant nonexistent role to a nonexistent user"):
                exitcode, message = errors.role_not_found_in_disk(name="role0")
                node.query("GRANT role0 TO user0", exitcode=exitcode, message=message)

    with Scenario("I grant a role to multiple users", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role("1.0")]):
        with setup(2,1):
            with When("I grant role to a multiple users"):
                node.query("GRANT role0 TO user0, user1")

    with Scenario("I grant multiple roles to multiple users", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role("1.0")]):
        with setup(2,2):
            with When("I grant multiple roles to multiple users"):
                node.query("GRANT role0, role1 TO user0, user1")

    with Scenario("I grant role to current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role_CurrentUser("1.0")]):
        with setup(1,1):
            with Given("I have a user with access management privilege"):
                node.query("GRANT ACCESS MANAGEMENT ON *.* TO user0")
            with When("I grant role to current user"):
                node.query("GRANT role0 TO CURRENT_USER", settings = [("user","user0")])

    with Scenario("I grant role to default user, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role_CurrentUser("1.0")]):
        with setup(1,1):
            with When("I grant role to default user"):
                exitcode, message = errors.cannot_update_default()
                node.query("GRANT role0 TO CURRENT_USER", exitcode=exitcode, message=message)

    with Scenario("I grant role to user with admin option", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role_AdminOption("1.0")]):
        with setup(1,1):
            with When("I grant role to a user with admin option"):
                node.query("GRANT role0 TO user0 WITH ADMIN OPTION")
            
    with Scenario("I grant role to user on cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role_OnCluster("1.0")]):
        try:
            with Given("I have a user and a role on a cluster"):
                node.query("CREATE USER user0 ON CLUSTER sharded_cluster")
                node.query("CREATE ROLE role0 ON CLUSTER sharded_cluster")
            with When("I grant the role to the user"):
                node.query("GRANT ON CLUSTER sharded_cluster role0 TO user0")
        finally:
            with Finally("I drop the user and role"):
                node.query("DROP USER user0 ON CLUSTER sharded_cluster")
                node.query("DROP ROLE role0 ON CLUSTER sharded_cluster")

    with Scenario("I grant role to user on fake cluster, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Role_OnCluster("1.0")]):
        with setup(1,1):
            with When("I grant the role to the user"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("GRANT ON CLUSTER fake_cluster role0 TO user0", exitcode=exitcode, message=message)