from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("drop user")
def feature(self, node="clickhouse1"):
    """Check drop user query syntax.

    ```sql
    DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
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

    def cleanup_user(user):
        with Given(f"I ensure that user {user} does not exist"):
            node.query(f"DROP USER IF EXISTS {user}")

    with Scenario("I drop user with no options", requirements=[
            RQ_SRS_006_RBAC_User_Drop("1.0")]):
        with setup("user0"):
            with When("I drop user"):
                node.query("DROP USER user0")

    with Scenario("I drop user, does not exist, throws exception", requirements=[
            RQ_SRS_006_RBAC_User_Drop("1.0")]):
            user = "user0"
            cleanup_user(user)
            with When(f"I drop user {user}"):
                exitcode, message = errors.user_not_found_in_disk(name=user)
                node.query(f"DROP USER {user}", exitcode=exitcode, message=message)
            del user

    with Scenario("I drop multiple users", requirements=[
            RQ_SRS_006_RBAC_User_Drop("1.0")]):
        with setup("user1"), setup("user2"):
            with When("I drop multiple users"):
                node.query("DROP USER user1, user2")

    with Scenario("I drop user if exists, user does exist", requirements=[
            RQ_SRS_006_RBAC_User_Drop_IfExists("1.0")]):
        with setup("user3"):
            with When("I drop user that exists"):
                node.query("DROP USER IF EXISTS user3")

    with Scenario("I drop user if exists, user does not exist", requirements=[
            RQ_SRS_006_RBAC_User_Drop_IfExists("1.0")]):
        cleanup_user("user3")
        with When("I drop nonexistant user"):
            node.query("DROP USER IF EXISTS user3")

    with Scenario("I drop default user, throws error", requirements=[
            RQ_SRS_006_RBAC_User_Drop("1.0")]):
        with When("I drop user"):
            exitcode, message = errors.cannot_remove_user_default()
            node.query("DROP USER default", exitcode=exitcode, message=message)

    with Scenario("I drop multiple users where one does not exist", requirements=[
            RQ_SRS_006_RBAC_User_Drop_IfExists("1.0")]):
        with setup("user3"):
            with When("I drop multiple users where one does not exist"):
                node.query("DROP USER IF EXISTS user3, user4")

    with Scenario("I drop multiple users where both do not exist", requirements=[
            RQ_SRS_006_RBAC_User_Drop_IfExists("1.0")]):
        with When("I drop the nonexistant users"):
            node.query("DROP USER IF EXISTS user5, user6")

    with Scenario("I drop user from specific cluster", requirements=[
            RQ_SRS_006_RBAC_User_Drop_OnCluster("1.0")]):
       try:
            with Given("I have a user on cluster"):
                node.query("CREATE USER user4 ON CLUSTER sharded_cluster")
            with When("I drop a user from the cluster"):
                node.query("DROP USER user4 ON CLUSTER sharded_cluster")
       finally:
           with Finally("I make sure the user is dropped"):
   	        node.query("DROP USER IF EXISTS user4 ON CLUSTER sharded_cluster")

    with Scenario("I drop user from fake cluster", requirements=[
            RQ_SRS_006_RBAC_User_Drop_OnCluster("1.0")]):
        with When("I drop a user from the fake cluster"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("DROP USER user5 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)
