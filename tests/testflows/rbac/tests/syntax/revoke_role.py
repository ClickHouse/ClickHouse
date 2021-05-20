from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("revoke role")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check revoke query syntax.

    ```sql
    REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR]
        role [,...] FROM {user | role | CURRENT_USER} [,...]
             | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(users=2,roles=2):
        try:
            with Given("I have some users"):
                for i in range(users):
                    node.query(f"CREATE USER OR REPLACE user{i}")
            with And("I have some roles"):
                for i in range(roles):
                    node.query(f"CREATE ROLE OR REPLACE role{i}")
            yield
        finally:
            with Finally("I drop the users"):
                for i in range(users):
                    node.query(f"DROP USER IF EXISTS user{i}")
            with And("I drop the roles"):
                for i in range(roles):
                    node.query(f"DROP ROLE IF EXISTS role{i}")

    with Scenario("I revoke a role from a user",flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup():
            with When("I revoke a role"):
                node.query("REVOKE role0 FROM user0")

    with Scenario("I revoke a nonexistent role from user", requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup(1,0):
            with When("I revoke nonexistent role from a user"):
                exitcode, message = errors.role_not_found_in_disk(name="role0")
                node.query("REVOKE role0 FROM user0", exitcode=exitcode, message=message)

    # with nonexistent object name, REVOKE assumes type role (treats user0 as role)
    with Scenario("I revoke a role from a nonexistent user", requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup(0,1):
            with When("I revoke role from a nonexistent user"):
                exitcode, message = errors.role_not_found_in_disk(name="user0")
                node.query("REVOKE role0 FROM user0", exitcode=exitcode, message=message)

    # with nonexistent object name, REVOKE assumes type role (treats user0 as role)
    with Scenario("I revoke a role from ALL EXCEPT nonexistent user", requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup(0,1):
            with When("I revoke role from a nonexistent user"):
                exitcode, message = errors.role_not_found_in_disk(name="user0")
                node.query("REVOKE role0 FROM ALL EXCEPT user0", exitcode=exitcode, message=message)

    with Scenario("I revoke a nonexistent role from a nonexistent user", requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup(0,0):
            with When("I revoke nonexistent role from a nonexistent user"):
                exitcode, message = errors.role_not_found_in_disk(name="role0")
                node.query("REVOKE role0 FROM user0", exitcode=exitcode, message=message)

    with Scenario("I revoke a role from multiple users", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup():
            with When("I revoke a role from multiple users"):
                node.query("REVOKE role0 FROM user0, user1")

    with Scenario("I revoke multiple roles from multiple users", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup():
            node.query("REVOKE role0, role1 FROM user0, user1")

    #user is default, expect exception
    with Scenario("I revoke a role from default user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke a role from default user"):
                exitcode, message = errors.cannot_update_default()
                node.query("REVOKE role0 FROM CURRENT_USER", exitcode=exitcode, message=message)

    #user is user0
    with Scenario("I revoke a role from current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke a role from current user"):
                node.query("REVOKE role0 FROM CURRENT_USER", settings = [("user","user0")])

    #user is default, expect exception
    with Scenario("I revoke a role from all", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke a role from all"):
                exitcode, message = errors.cannot_update_default()
                node.query("REVOKE role0 FROM ALL", exitcode=exitcode, message=message)

    #user is default, expect exception
    with Scenario("I revoke multiple roles from all", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke multiple roles from all"):
                exitcode, message = errors.cannot_update_default()
                node.query("REVOKE role0, role1 FROM ALL", exitcode=exitcode, message=message)

    with Scenario("I revoke a role from all but current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke a role from all except current"):
                node.query("REVOKE role0 FROM ALL EXCEPT CURRENT_USER")

    with Scenario("I revoke a role from all but default user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke a role from all except default"):
                node.query("REVOKE role0 FROM ALL EXCEPT default",
                            settings = [("user","user0")])

    with Scenario("I revoke multiple roles from all but default user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Keywords("1.0")]):
        with setup():
            with When("I revoke multiple roles from all except default"):
                node.query("REVOKE role0, role1 FROM ALL EXCEPT default", settings = [("user","user0")])

    with Scenario("I revoke a role from a role", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup():
            with When("I revoke a role from a role"):
                node.query("REVOKE role0 FROM role1")

    with Scenario("I revoke a role from a role and a user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0")]):
        with setup():
            with When("I revoke a role from multiple roles"):
                node.query("REVOKE role0 FROM role1, user0")

    with Scenario("I revoke a role from a user on cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role_Cluster("1.0")]):
        with Given("I have a role and a user on a cluster"):
            node.query("CREATE USER OR REPLACE user0 ON CLUSTER sharded_cluster")
            node.query("CREATE ROLE OR REPLACE role0 ON CLUSTER sharded_cluster")
        with When("I revoke a role from user on a cluster"):
            node.query("REVOKE ON CLUSTER sharded_cluster role0 FROM user0")
        with Finally("I drop the user and role"):
            node.query("DROP USER IF EXISTS user0 ON CLUSTER sharded_cluster")
            node.query("DROP ROLE IF EXISTS role0 ON CLUSTER sharded_cluster")

    with Scenario("I revoke a role on fake cluster, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role_Cluster("1.0")]):
        with When("I revoke a role from user on a cluster"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("REVOKE ON CLUSTER fake_cluster role0 FROM user0", exitcode=exitcode, message=message)

    with Scenario("I revoke multiple roles from multiple users on cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_Role_Cluster("1.0")]):
        with Given("I have multiple roles and multiple users on a cluster"):
            for i in range(2):
                node.query(f"CREATE USER OR REPLACE user{i} ON CLUSTER sharded_cluster")
                node.query(f"CREATE ROLE OR REPLACE role{i} ON CLUSTER sharded_cluster")
        with When("I revoke multiple roles from multiple users on cluster"):
            node.query("REVOKE ON CLUSTER sharded_cluster role0, role1 FROM user0, user1")
        with Finally("I drop the roles and users"):
            for i in range(2):
                node.query(f"DROP USER IF EXISTS user{i} ON CLUSTER sharded_cluster")
                node.query(f"DROP ROLE IF EXISTS role{i} ON CLUSTER sharded_cluster")

    with Scenario("I revoke admin option for role from a user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_AdminOption("1.0")]):
        with setup():
            with When("I revoke admin option for role from a user"):
                node.query("REVOKE ADMIN OPTION FOR role0 FROM user0")

    with Scenario("I revoke admin option for multiple roles from multiple users", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Role("1.0"),
            RQ_SRS_006_RBAC_Revoke_AdminOption("1.0")]):
        with setup():
            with When("I revoke admin option for multiple roles from multiple users"):
                node.query("REVOKE ADMIN OPTION FOR role0, role1 FROM user0, user1")