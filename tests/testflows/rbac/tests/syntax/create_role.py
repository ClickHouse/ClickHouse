from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("create role")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check create role query syntax.

    ```sql
    CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(role):
        try:
            with Given("I ensure the role doesn't already exist"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            yield
        finally:
            with Finally("I drop the role"):
                node.query(f"DROP ROLE IF EXISTS {role}")

    def create_role(role):
        with Given(f"I ensure I do have role {role}"):
                node.query(f"CREATE ROLE OR REPLACE {role}")

    with Scenario("I create role with no options", requirements=[
            RQ_SRS_006_RBAC_Role_Create("1.0")]):
        with cleanup("role0"):
            with When("I create role"):
                node.query("CREATE ROLE role0")

    with Scenario("I create role that already exists, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Create("1.0")]):
        role = "role0"
        with cleanup(role):
            with Given(f"I have role {role}"):
                node.query(f"CREATE ROLE {role}")
            with When(f"I create role {role}, throws exception"):
                exitcode, message = errors.cannot_insert_role(name=role)
                node.query(f"CREATE ROLE {role}", exitcode=exitcode, message=message)
        del role

    with Scenario("I create role if not exists, role does not exist", requirements=[
            RQ_SRS_006_RBAC_Role_Create_IfNotExists("1.0")]):
        role = "role1"
        with cleanup(role):
            with When(f"I create role {role} with if not exists"):
                node.query(f"CREATE ROLE IF NOT EXISTS {role}")
        del role

    with Scenario("I create role if not exists, role does exist", requirements=[
            RQ_SRS_006_RBAC_Role_Create_IfNotExists("1.0")]):
        role = "role1"
        with cleanup(role):
            create_role(role)
            with When(f"I create role {role} with if not exists"):
                node.query(f"CREATE ROLE IF NOT EXISTS {role}")
        del role

    with Scenario("I create role or replace, role does not exist", requirements=[
            RQ_SRS_006_RBAC_Role_Create_Replace("1.0")]):
        role = "role2"
        with cleanup(role):
            with When(f"I create role {role} with or replace"):
                node.query(f"CREATE ROLE OR REPLACE {role}")
        del role

    with Scenario("I create role or replace, role does exist", requirements=[
            RQ_SRS_006_RBAC_Role_Create_Replace("1.0")]):
        role = "role2"
        with cleanup(role):
            create_role(role)
            with When(f"I create role {role} with or replace"):
                node.query(f"CREATE ROLE OR REPLACE {role}")
        del role

    with Scenario("I create role on cluster", requirements=[
            RQ_SRS_006_RBAC_Role_Create("1.0")]):
        try:
            with When("I have a role on a cluster"):
                node.query("CREATE ROLE role1 ON CLUSTER sharded_cluster")
            with And("I run create role or replace on a cluster"):
                node.query("CREATE ROLE OR REPLACE role1 ON CLUSTER sharded_cluster")
            with And("I create role with settings on a cluster"):
                node.query("CREATE ROLE role2 ON CLUSTER sharded_cluster SETTINGS max_memory_usage=10000000 READONLY")
        finally:
            with Finally("I drop the role"):
                node.query("DROP ROLE IF EXISTS role1,role2 ON CLUSTER sharded_cluster")

    with Scenario("I create role on nonexistent cluster, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Create("1.0")]):
        with When("I run create role on a cluster"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("CREATE ROLE role1 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)

    with Scenario("I create role with settings profile", requirements=[
            RQ_SRS_006_RBAC_Role_Create_Settings("1.0")]):
        with cleanup("role3"):
            with When("I create role with settings profile"):
                node.query("CREATE ROLE role3 SETTINGS PROFILE default, max_memory_usage=10000000 WRITABLE")

    with Scenario("I create role settings profile, fake profile, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Create_Settings("1.0")]):
        with cleanup("role4a"):
            with Given("I ensure profile profile0 does not exist"):
                node.query("DROP SETTINGS PROFILE IF EXISTS profile0")
            with When("I create role with settings profile that does not exist"):
                exitcode, message = errors.settings_profile_not_found_in_disk("profile0")
                node.query("CREATE ROLE role4a SETTINGS PROFILE profile0", exitcode=exitcode, message=message)

    with Scenario("I create role with settings without profile", requirements=[
            RQ_SRS_006_RBAC_Role_Create_Settings("1.0")]):
        with cleanup("role4"):
            with When("I create role with settings without profile"):
                node.query("CREATE ROLE role4 SETTINGS max_memory_usage=10000000 READONLY")
