import hashlib
from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("create user")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check create user query syntax.

    ```sql
    CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | NAME REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(user):
        try:
            with Given("I ensure the user does not already exist", flags=TE):
                node.query(f"DROP USER IF EXISTS {user}")
            yield
        finally:
            with Finally("I drop the user", flags=TE):
                node.query(f"DROP USER IF EXISTS {user}")

    def create_user(user):
        with Given(f"I ensure I do have user {user}"):
                node.query(f"CREATE USER OR REPLACE {user}")

    with Scenario("I create user with no options", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create("1.0"),
    		RQ_SRS_006_RBAC_User_Create_Host_Default("1.0")]):
        with cleanup("user0"):
            with When("I create a user with no options"):
                node.query("CREATE USER user0")

    with Scenario("I create user that already exists, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create("1.0"),
    		RQ_SRS_006_RBAC_User_Create_Host_Default("1.0")]):
        user = "user0"
        with cleanup(user):
            create_user(user)
            with When(f"I create a user {user} that already exists without IF EXISTS, throws exception"):
                exitcode, message = errors.cannot_insert_user(name=user)
                node.query(f"CREATE USER {user}", exitcode=exitcode, message=message)
        del user

    with Scenario("I create user with if not exists, user does not exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_IfNotExists("1.0")]):
        user = "user0"
        with cleanup(user):
            with When(f"I create a user {user} with if not exists"):
                node.query(f"CREATE USER IF NOT EXISTS {user}")
        del user

    #Bug exists, mark as xfail
    with Scenario("I create user with if not exists, user does exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_IfNotExists("1.0")]):
        user = "user0"
        with cleanup(user):
            create_user(user)
            with When(f"I create a user {user} with if not exists"):
                node.query(f"CREATE USER IF NOT EXISTS {user}")
        del user

    with Scenario("I create user or replace, user does not exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Replace("1.0")]):
        user = "user0"
        with cleanup(user):
            with When(f"I create a user {user} with or replace"):
                node.query(f"CREATE USER OR REPLACE {user}")
        del user

    with Scenario("I create user or replace, user does exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Replace("1.0")]):
        user = "user0"
        with cleanup(user):
            create_user(user)
            with When(f"I create a user {user} with or replace"):
                node.query(f"CREATE USER OR REPLACE {user}")
        del user

    with Scenario("I create user with no password", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_NoPassword("1.0")]):
        with cleanup("user1"):
            with When("I create a user with no password"):
                node.query("CREATE USER user1 IDENTIFIED WITH NO_PASSWORD")

    with Scenario("I create user with plaintext password", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_PlainText("1.0")]):
        with cleanup("user1"):
            with When("I create a user with plaintext password"):
                node.query("CREATE USER user1 IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'mypassword'")

    with Scenario("I create user with sha256 password", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_Sha256Password("1.0")]):
        with cleanup("user2"):
            with When("I create a user with sha256 password"):
                password = hashlib.sha256("mypassword".encode("utf-8")).hexdigest()
                node.query(f"CREATE USER user2 IDENTIFIED WITH SHA256_PASSWORD BY '{password}'")

    with Scenario("I create user with sha256 password using IDENTIFIED BY", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_Sha256Password("1.0")]):
        with cleanup("user2"):
            with When("I create a user with sha256 password using short form"):
                password = hashlib.sha256("mypassword".encode("utf-8")).hexdigest()
                node.query(f"CREATE USER user2 IDENTIFIED BY '{password}'")

    with Scenario("I create user with sha256_hash password", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_Sha256Hash("1.0")]):
        with cleanup("user3"):
            with When("I create a user with sha256_hash"):
                def hash(password):
                    return hashlib.sha256(password.encode("utf-8")).hexdigest()
                password = hash(hash("mypassword"))
                node.query(f"CREATE USER user3 IDENTIFIED WITH SHA256_HASH BY '{password}'")

    with Scenario("I create user with double sha1 password", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_DoubleSha1Password("1.0")]):
        with cleanup("user3"):
            with When("I create a user with double_sha1_password"):
                node.query(f"CREATE USER user3 IDENTIFIED WITH DOUBLE_SHA1_PASSWORD BY 'mypassword'")

    with Scenario("I create user with double sha1 hash", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Password_DoubleSha1Hash("1.0")]):
        with cleanup("user3"):
            with When("I create a user with double_sha1_hash"):
                def hash(password):
                    return hashlib.sha1(password.encode("utf-8")).hexdigest()
                password = hash(hash("mypassword"))
                node.query(f"CREATE USER user3 IDENTIFIED WITH DOUBLE_SHA1_HASH BY '{password}'")

    with Scenario("I create user with host name", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_Name("1.0")]):
        with cleanup("user4"):
            with When("I create a user with host name"):
                node.query("CREATE USER user4 HOST NAME 'localhost', NAME 'clickhouse.com'")

    with Scenario("I create user with host regexp", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_Regexp("1.0")]):
        with cleanup("user5"):
            with When("I create a user with host regexp"):
                node.query("CREATE USER user5 HOST REGEXP 'lo.?*host', REGEXP 'lo*host'")

    with Scenario("I create user with host ip", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_IP("1.0")]):
        with cleanup("user6"):
            with When("I create a user with host ip"):
                node.query("CREATE USER user6 HOST IP '127.0.0.1', IP '127.0.0.2'")

    with Scenario("I create user with host like", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_Like("1.0")]):
        with cleanup("user7"):
            with When("I create a user with host like"):
                node.query("CREATE USER user7 HOST LIKE 'local%'")

    with Scenario("I create user with host none", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_None("1.0")]):
        with cleanup("user7"):
            with When("I create a user with host none"):
                node.query("CREATE USER user7 HOST NONE")

    with Scenario("I create user with host local", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_Local("1.0")]):
        with cleanup("user7"):
            with When("I create a user with host local"):
                node.query("CREATE USER user7 HOST LOCAL")

    with Scenario("I create user with host any", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Host_Any("1.0")]):
        with cleanup("user7"):
            with When("I create a user with host any"):
                node.query("CREATE USER user7 HOST ANY")

    with Scenario("I create user with default role set to none", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_DefaultRole_None("1.0")]):
        with cleanup("user8"):
            with When("I create a user with no default role"):
                node.query("CREATE USER user8 DEFAULT ROLE NONE")

    with Scenario("I create user with default role", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_DefaultRole("1.0")]):
        with Given("I have a role"):
            node.query("CREATE ROLE default")
        with cleanup("user9"):
            with When("I create a user with a default role"):
                node.query("CREATE USER user9 DEFAULT ROLE default")
        with Finally("I drop the role"):
            node.query("DROP ROLE default")

    with Scenario("I create user default role, role doesn't exist, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_DefaultRole("1.0")]):
        with cleanup("user12"):
            role = "role0"
            with Given(f"I ensure that role {role} does not exist"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with When(f"I create user with default role {role}"):
                exitcode, message = errors.role_not_found_in_disk(role)
                node.query(f"CREATE USER user12 DEFAULT ROLE {role}",exitcode=exitcode, message=message)
            del role

    with Scenario("I create user default role, all except role doesn't exist, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_DefaultRole("1.0")]):
        with cleanup("user12"):
            role = "role0"
            with Given(f"I ensure that role {role} does not exist"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with When(f"I create user with default role {role}"):
                exitcode, message = errors.role_not_found_in_disk(role)
                node.query(f"CREATE USER user12 DEFAULT ROLE ALL EXCEPT {role}",exitcode=exitcode, message=message)
            del role

    with Scenario("I create user with all roles set to default", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_DefaultRole_All("1.0")]):
        with cleanup("user10"):
            with When("I create a user with all roles as default"):
                node.query("CREATE USER user10 DEFAULT ROLE ALL")

    with Scenario("I create user with settings profile", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Settings("1.0")]):
        with cleanup("user11"):
            with When("I create a user with a settings profile"):
                node.query("CREATE USER user11 SETTINGS PROFILE default, max_memory_usage=10000000 READONLY")

    with Scenario("I create user settings profile, fake profile, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Settings("1.0")]):
        with cleanup("user18a"):
            profile = "profile0"
            with Given(f"I ensure that profile {profile} does not exist"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")
            with When(f"I create user with Settings and set profile to fake profile {profile}"):
                exitcode, message = errors.settings_profile_not_found_in_disk(profile)
                node.query("CREATE USER user18a SETTINGS PROFILE profile0", exitcode=exitcode, message=message)
            del profile

    with Scenario("I create user settings with a fake setting, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_User_Create_Settings("1.0")]):
        with cleanup("user18b"):
            with When("I create settings profile using settings and nonexistent value"):
                exitcode, message = errors.unknown_setting("fake_setting")
                node.query("CREATE USER user18b SETTINGS fake_setting = 100000001", exitcode=exitcode, message=message)

    with Scenario("I create user with settings without profile", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_Settings("1.0")]):
        with cleanup("user12"):
            with When("I create a user with settings and no profile"):
                node.query("CREATE USER user12 SETTINGS max_memory_usage=10000000 READONLY")

    with Scenario("I create user on cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_OnCluster("1.0")]):
        try:
            with When("I create user on cluster"):
                node.query("CREATE USER user13 ON CLUSTER sharded_cluster")
        finally:
            with Finally("I drop the user"):
                node.query("DROP USER user13 ON CLUSTER sharded_cluster")

    with Scenario("I create user on fake cluster, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_User_Create_OnCluster("1.0")]):
            with When("I create user on fake cluster"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("CREATE USER user14 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)
