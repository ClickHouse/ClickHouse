from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("alter role")
def feature(self, node="clickhouse1"):
    """Check alter role query syntax.

    ```sql
    ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(role, profile=None):
        try:
            with Given("I have a role"):
                node.query(f"CREATE ROLE OR REPLACE {role}")
            if profile != None: #create profile when name is given
                with Given("And I have a profile"):
                    node.query(f"CREATE SETTINGS PROFILE OR REPLACE {profile}")
            yield
        finally:
            with Finally("I drop the role"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            if profile != "":
                with Finally("I drop the profile"):
                    node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")

    def cleanup_role(role):
        with Given(f"I ensure that role {role} does not exist"):
            node.query(f"DROP ROLE IF EXISTS {role}")

    with Scenario("I alter role with no options", requirements=[
            RQ_SRS_006_RBAC_Role_Alter("1.0")]):
        with setup("role0"):
            with When("I alter role"):
                node.query("ALTER ROLE role0")

    with Scenario("I alter role that does not exist, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Alter("1.0")]):
        role = "role0"
        cleanup_role(role)
        with When(f"I alter role {role} that does not exist"):
            exitcode, message = errors.role_not_found_in_disk(name=role)
            node.query(f"ALTER ROLE {role}", exitcode=exitcode, message=message)
        del role

    with Scenario("I alter role if exists, role does exist", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_IfExists("1.0")]):
        with setup("role1"):
            with When("I alter role with if exists"):
                node.query("ALTER ROLE IF EXISTS role1")

    with Scenario("I alter role if exists, role does not exist", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_IfExists("1.0")]):
        role = "role0"
        cleanup_role(role)
        with When(f"I alter role {role} that does not exist"):
            node.query(f"ALTER ROLE IF EXISTS {role}")
        del role

    with Scenario("I alter role on cluster", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Cluster("1.0")]):
        try:
            with Given("I have a role on a cluster"):
                node.query("CREATE ROLE role1 ON CLUSTER sharded_cluster")
            with When("I run alter role on a cluster"):
                node.query("ALTER ROLE role1 ON CLUSTER sharded_cluster")
            with And("I rename role on a cluster"):
                node.query("ALTER ROLE role1 ON CLUSTER sharded_cluster RENAME TO role2")
            with And("I alter role with settings on a cluster"):
                node.query("ALTER ROLE role2 ON CLUSTER sharded_cluster SETTINGS max_memory_usage=10000000 READONLY")
        finally:
            with Finally("I drop the role"):
                node.query("DROP ROLE IF EXISTS role1,role2 ON CLUSTER sharded_cluster")

    with Scenario("I alter role on nonexistent cluster, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Cluster("1.0")]):
        with When("I run alter role on a cluster"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("ALTER ROLE role1 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)

    with Scenario("I alter role to rename, new name is available", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Rename("1.0")]):
        with setup("role2"):
            new_role = "role3"
            try:
                with Given(f"Ensure target name {new_role} is available"):
                    node.query(f"DROP ROLE IF EXISTS {new_role}")
                with When(f"I try to rename to {new_role}"):
                    node.query(f"ALTER ROLE role2 RENAME TO {new_role}")
            finally:
                with Finally(f"I cleanup new name {new_role}"):
                    node.query(f"DROP ROLE IF EXISTS {new_role}")
            del new_role

    with Scenario("I alter role to rename, new name is not available, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Rename("1.0")]):
        with setup("role2a"):
            new_role = "role3a"
            try:
                with Given(f"Ensure target name {new_role} is NOT available"):
                    node.query(f"CREATE ROLE IF NOT EXISTS {new_role}")
                with When(f"I try to rename to {new_role}"):
                    exitcode, message = errors.cannot_rename_role(name="role2a", name_new=new_role)
                    node.query(f"ALTER ROLE role2a RENAME TO {new_role}", exitcode=exitcode, message=message)
            finally:
                with Finally(f"I cleanup target name {new_role}"):
                    node.query(f"DROP ROLE IF EXISTS {new_role}")
            del new_role

    with Scenario("I alter role settings profile", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role4"):
            with When("I alter role with settings profile"):
                node.query("ALTER ROLE role4 SETTINGS PROFILE default, max_memory_usage=10000000 READONLY")

    with Scenario("I alter role settings profile, profile does not exist, throws exception", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role4a"):
            with Given("I ensure profile profile0 does not exist"):
                node.query("DROP SETTINGS PROFILE IF EXISTS profile0")
            with When("I alter role with settings profile that does not exist"):
                exitcode, message = errors.settings_profile_not_found_in_disk("profile0")
                node.query("ALTER ROLE role4a SETTINGS PROFILE profile0", exitcode=exitcode, message=message)

    with Scenario("I alter role settings profile multiple", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role4b", profile="profile0"):
            with When("I alter role with multiple profiles"):
                node.query("ALTER ROLE role4b SETTINGS PROFILE default, PROFILE profile0, \
                    max_memory_usage=10000000 READONLY")

    with Scenario("I alter role settings without profile", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role5"):
            with When("I alter role with settings and no profile"):
                node.query("ALTER ROLE role5 SETTINGS max_memory_usage=10000000 READONLY")

    with Scenario("I alter role settings, variable does not exist, throws exception", requirements=[
                RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role5a"):
            with When("I alter role using settings and nonexistent value"):
                exitcode, message = errors.unknown_setting("fake_setting")
                node.query("ALTER ROLE role5a SETTINGS fake_setting = 100000001", exitcode=exitcode, message=message)


    with Scenario("I alter role settings without profile multiple", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role6"):
            with When("I alter role with multiple settings and no profile"):
                node.query("ALTER ROLE role6 SETTINGS max_memory_usage=10000000 READONLY, \
                    max_rows_to_read MIN 20 MAX 25")

    with Scenario("I alter role settings with multiple profiles multiple variables", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role7", profile="profile1"):
            with When("I alter role with multiple settings and profiles"):
                node.query("ALTER ROLE role7 SETTINGS PROFILE default, PROFILE profile1, \
                    max_memory_usage=10000000 READONLY, max_rows_to_read MIN 20 MAX 25")

    with Scenario("I alter role settings readonly", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role8"):
            with When("I alter role with readonly"):
                node.query("ALTER ROLE role8 SETTINGS max_memory_usage READONLY")

    with Scenario("I alter role settings writable", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role9"):
            with When("I alter role with writable"):
                node.query("ALTER ROLE role9 SETTINGS max_memory_usage WRITABLE")

    with Scenario("I alter role settings min, with and without = sign", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role10"):
            with When("I set min, no equals"):
                node.query("ALTER ROLE role10 SETTINGS max_memory_usage MIN 200")
            with When("I set min, yes equals"):
                node.query("ALTER ROLE role10 SETTINGS max_memory_usage MIN = 200")

    with Scenario("I alter role settings max, with and without = sign", requirements=[
            RQ_SRS_006_RBAC_Role_Alter_Settings("1.0")]):
        with setup("role11"):
            with When("I set max, no equals"):
                node.query("ALTER ROLE role11 SETTINGS max_memory_usage MAX 2000")
            with When("I set max, yes equals"):
                node.query("ALTER ROLE role11 SETTINGS max_memory_usage MAX = 200")