import hashlib
from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *


@TestFeature
@Name("alter user")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check alter user query syntax.

    ```sql
    ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def setup(user):
        try:
            with Given("I have a user"):
                node.query(f"CREATE USER OR REPLACE {user}")
            yield
        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {user}")

    with Scenario(
        "I alter user, base command", requirements=[RQ_SRS_006_RBAC_User_Alter("1.0")]
    ):
        with setup("user0"):
            with When("I alter user"):
                node.query("ALTER USER user0")

    with Scenario(
        "I alter user that does not exist without if exists, throws exception",
        requirements=[RQ_SRS_006_RBAC_User_Alter("1.0")],
    ):
        with When("I run alter user command, expecting error 192"):
            exitcode, message = errors.user_not_found_in_disk(name="user0")
            node.query(f"ALTER USER user0", exitcode=exitcode, message=message)

    with Scenario(
        "I alter user with if exists",
        requirements=[RQ_SRS_006_RBAC_User_Alter_IfExists("1.0")],
    ):
        with setup("user0"):
            with When(f"I alter user with if exists"):
                node.query(f"ALTER USER IF EXISTS user0")

    with Scenario(
        "I alter user that does not exist with if exists",
        requirements=[RQ_SRS_006_RBAC_User_Alter_IfExists("1.0")],
    ):
        user = "user0"
        with Given("I don't have a user"):
            node.query(f"DROP USER IF EXISTS {user}")
        with When(f"I alter user {user} with if exists"):
            node.query(f"ALTER USER IF EXISTS {user}")
        del user

    with Scenario(
        "I alter user on a cluster",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Cluster("1.0")],
    ):
        with Given("I have a user on a cluster"):
            node.query("CREATE USER OR REPLACE user0 ON CLUSTER sharded_cluster")
        with When("I alter user on a cluster"):
            node.query("ALTER USER user0 ON CLUSTER sharded_cluster")
        with Finally("I drop user from cluster"):
            node.query("DROP USER IF EXISTS user0 ON CLUSTER sharded_cluster")

    with Scenario(
        "I alter user on a fake cluster, throws exception",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Cluster("1.0")],
    ):
        with When("I alter user on a fake cluster"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query(
                "ALTER USER user0 ON CLUSTER fake_cluster",
                exitcode=exitcode,
                message=message,
            )

    with Scenario(
        "I alter user to rename, target available",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Rename("1.0")],
    ):
        with setup("user15"):
            with When("I alter user name"):
                node.query("ALTER USER user15 RENAME TO user15")

    with Scenario(
        "I alter user to rename, target unavailable",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Rename("1.0")],
    ):
        with setup("user15"):
            new_user = "user16"
            try:
                with Given(f"Ensure target name {new_user} is NOT available"):
                    node.query(f"CREATE USER IF NOT EXISTS {new_user}")
                with When(f"I try to rename to {new_user}"):
                    exitcode, message = errors.cannot_rename_user(
                        name="user15", name_new=new_user
                    )
                    node.query(
                        f"ALTER USER user15 RENAME TO {new_user}",
                        exitcode=exitcode,
                        message=message,
                    )
            finally:
                with Finally(f"I cleanup target name {new_user}"):
                    node.query(f"DROP USER IF EXISTS {new_user}")
            del new_user

    with Scenario(
        "I alter user password plaintext password",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Password_PlainText("1.0")],
    ):
        with setup("user1"):
            with When("I alter user with plaintext password"):
                node.query(
                    "ALTER USER user1 IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'mypassword'",
                    step=When,
                )

    with Scenario(
        "I alter user password to sha256",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Password_Sha256Password("1.0")],
    ):
        with setup("user2"):
            with When("I alter user with sha256_password"):
                password = hashlib.sha256("mypassword".encode("utf-8")).hexdigest()
                node.query(
                    f"ALTER USER user2 IDENTIFIED WITH SHA256_PASSWORD BY '{password}'",
                    step=When,
                )

    with Scenario(
        "I alter user password to double_sha1_password",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Password_DoubleSha1Password("1.0")],
    ):
        with setup("user3"):
            with When("I alter user with double_sha1_password"):

                def hash(password):
                    return hashlib.sha1(password.encode("utf-8")).hexdigest()

                password = hash(hash("mypassword"))
                node.query(
                    f"ALTER USER user3 IDENTIFIED WITH DOUBLE_SHA1_PASSWORD BY '{password}'",
                    step=When,
                )

    with Scenario(
        "I alter user host local",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_Local("1.0")],
    ):
        with setup("user4"):
            with When("I alter user with host local"):
                node.query("ALTER USER user4 HOST LOCAL")

    with Scenario(
        "I alter user host name",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_Name("1.0")],
    ):
        with setup("user5"):
            with When("I alter user with host name"):
                node.query(
                    "ALTER USER user5 HOST NAME 'localhost', NAME 'clickhouse.com'"
                )

    with Scenario(
        "I alter user host regexp",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_Regexp("1.0")],
    ):
        with setup("user6"):
            with When("I alter user with host regexp"):
                node.query("ALTER USER user6 HOST REGEXP 'lo..*host', 'lo*host'")

    with Scenario(
        "I alter user host ip", requirements=[RQ_SRS_006_RBAC_User_Alter_Host_IP("1.0")]
    ):
        with setup("user7"):
            with When("I alter user with host ip"):
                node.query("ALTER USER user7 HOST IP '127.0.0.1', IP '127.0.0.2'")

    with Scenario(
        "I alter user host like",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_Like("1.0")],
    ):
        with setup("user8"):
            with When("I alter user with host like"):
                node.query("ALTER USER user8 HOST LIKE '%.clickhouse.com'")

    with Scenario(
        "I alter user host any",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_Any("1.0")],
    ):
        with setup("user9"):
            with When("I alter user with host any"):
                node.query("ALTER USER user9 HOST ANY")

    with Scenario(
        "I alter user host many hosts",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_Like("1.0")],
    ):
        with setup("user11"):
            with When("I alter user with multiple hosts"):
                node.query(
                    "ALTER USER user11 HOST LIKE '%.clickhouse.com', \
                    IP '127.0.0.2', NAME 'localhost', REGEXP 'lo*host'"
                )

    with Scenario(
        "I alter user default role set to none",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_None("1.0")],
    ):
        with setup("user12"):
            with When("I alter user with default role none"):
                node.query("ALTER USER user12 DEFAULT ROLE NONE")

    with Scenario(
        "I alter user default role set to all",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole_All("1.0")],
    ):
        with setup("user13"):
            with When("I alter user with all roles set to default"):
                node.query("ALTER USER user13 DEFAULT ROLE ALL")

    @contextmanager
    def setup_role(role):
        try:
            with Given(f"I have a role {role}"):
                node.query(f"CREATE ROLE OR REPLACE {role}")
            yield
        finally:
            with Finally(f"I drop the role {role}", flags=TE):
                node.query(f"DROP ROLE IF EXISTS {role}")

    with Scenario(
        "I alter user default role",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole("1.0")],
    ):
        with setup("user14"), setup_role("role2"):
            with Given("I have a user with a role"):
                node.query("GRANT role2 TO user14")
            with When("I alter user default role"):
                node.query("ALTER USER user14 DEFAULT ROLE role2")

    with Scenario(
        "I alter user default role, setting default role",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole("1.0")],
    ):
        with setup("user14a"), setup_role("default"):
            with Given("I grant default role to the user"):
                node.query("GRANT default TO user14a")
            with When("I alter user default role"):
                node.query("ALTER USER user14a DEFAULT ROLE default")

    with Scenario(
        "I alter user default role, role doesn't exist, throws exception",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole("1.0")],
    ):
        with setup("user12"):
            role = "role0"
            with Given(f"I ensure that role {role} does not exist"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with When(f"I alter user with default role {role}"):
                exitcode, message = errors.role_not_found_in_disk(role)
                node.query(
                    f"ALTER USER user12 DEFAULT ROLE {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

    with Scenario(
        "I alter user default role, all except role doesn't exist, throws exception",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole("1.0")],
    ):
        with setup("user12"):
            role = "role0"
            with Given(f"I ensure that role {role} does not exist"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with When(f"I alter user with default role {role}"):
                exitcode, message = errors.role_not_found_in_disk(role)
                node.query(
                    f"ALTER USER user12 DEFAULT ROLE ALL EXCEPT {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

    with Scenario(
        "I alter user default role multiple",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole("1.0")],
    ):
        with setup("user15"), setup_role("second"), setup_role("third"):
            with Given("I have a user with multiple roles"):
                node.query("GRANT second,third TO user15")
            with When("I alter user default role to second, third"):
                node.query("ALTER USER user15 DEFAULT ROLE second, third")

    with Scenario(
        "I alter user default role set to all except",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole_AllExcept("1.0")],
    ):
        with setup("user16"), setup_role("second"):
            with Given("I have a user with a role"):
                node.query("GRANT second TO user16")
            with When("I alter user default role"):
                node.query("ALTER USER user16 DEFAULT ROLE ALL EXCEPT second")

    with Scenario(
        "I alter user default role multiple all except",
        requirements=[RQ_SRS_006_RBAC_User_Alter_DefaultRole_AllExcept("1.0")],
    ):
        with setup("user17"), setup_role("second"), setup_role("third"):
            with Given("I have a user with multiple roles"):
                node.query("GRANT second,third TO user17")
            with When("I alter user default role to all except second"):
                node.query("ALTER USER user17 DEFAULT ROLE ALL EXCEPT second")

    with Scenario(
        "I alter user settings profile",
        requirements=[
            RQ_SRS_006_RBAC_User_Alter_Settings("1.0"),
            RQ_SRS_006_RBAC_User_Alter_Settings_Profile("1.0"),
        ],
    ):
        with setup("user18"):
            try:
                with Given("I have a profile"):
                    node.query(f"CREATE SETTINGS PROFILE profile10")
                with When("I alter user with settings and set profile to profile1"):
                    node.query(
                        "ALTER USER user18 SETTINGS PROFILE profile10, max_memory_usage = 100 MIN 0 MAX 1000 READONLY"
                    )
            finally:
                with Finally("I drop the profile"):
                    node.query(f"DROP SETTINGS PROFILE profile10")

    with Scenario(
        "I alter user settings profile, fake profile, throws exception",
        requirements=[
            RQ_SRS_006_RBAC_User_Alter_Settings("1.0"),
            RQ_SRS_006_RBAC_User_Alter_Settings_Profile("1.0"),
        ],
    ):
        with setup("user18a"):
            profile = "profile0"
            with Given(f"I ensure that profile {profile} does not exist"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")
            with When(
                f"I alter user with Settings and set profile to fake profile {profile}"
            ):
                exitcode, message = errors.settings_profile_not_found_in_disk(profile)
                node.query(
                    "ALTER USER user18a SETTINGS PROFILE profile0",
                    exitcode=exitcode,
                    message=message,
                )
            del profile

    with Scenario(
        "I alter user settings with a fake setting, throws exception",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Settings("1.0")],
    ):
        with setup("user18b"):
            with When("I alter settings profile using settings and nonexistent value"):
                exitcode, message = errors.unknown_setting("fake_setting")
                node.query(
                    "ALTER USER user18b SETTINGS fake_setting = 100000001",
                    exitcode=exitcode,
                    message=message,
                )

    with Scenario(
        "I alter user settings without profile (no equals)",
        requirements=[
            RQ_SRS_006_RBAC_User_Alter_Settings("1.0"),
            RQ_SRS_006_RBAC_User_Alter_Settings_Min("1.0"),
            RQ_SRS_006_RBAC_User_Alter_Settings_Max("1.0"),
        ],
    ):
        with setup("user19"):
            with When("I alter user with settings without profile using no equals"):
                node.query(
                    "ALTER USER user19 SETTINGS max_memory_usage=10000000 MIN 100000 MAX 1000000000 READONLY"
                )

    # equals sign (=) syntax verify
    with Scenario(
        "I alter user settings without profile (yes equals)",
        requirements=[
            RQ_SRS_006_RBAC_User_Alter_Settings("1.0"),
            RQ_SRS_006_RBAC_User_Alter_Settings_Min("1.0"),
            RQ_SRS_006_RBAC_User_Alter_Settings_Max("1.0"),
        ],
    ):
        with setup("user20"):
            with When("I alter user with settings without profile using equals"):
                node.query(
                    "ALTER USER user20 SETTINGS max_memory_usage=10000000 MIN=100000 MAX=1000000000 READONLY"
                )

    # Add requirement to host: add/drop
    with Scenario(
        "I alter user to add host",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_AddDrop("1.0")],
    ):
        with setup("user21"):
            with When("I alter user by adding local host"):
                node.query("ALTER USER user21 ADD HOST LOCAL")
            with And("I alter user by adding no host"):
                node.query("ALTER USER user21 ADD HOST NONE")
            with And("I alter user by adding host like"):
                node.query("ALTER USER user21 ADD HOST LIKE 'local%'")
            with And("I alter user by adding host ip"):
                node.query("ALTER USER user21 ADD HOST IP '127.0.0.1'")
            with And("I alter user by adding host name"):
                node.query("ALTER USER user21 ADD HOST NAME 'localhost'")

    with Scenario(
        "I alter user to remove host",
        requirements=[RQ_SRS_006_RBAC_User_Alter_Host_AddDrop("1.0")],
    ):
        with setup("user22"):
            with When("I alter user by removing local host"):
                node.query("ALTER USER user22 DROP HOST LOCAL")
            with And("I alter user by removing no host"):
                node.query("ALTER USER user22 DROP HOST NONE")
            with And("I alter user by removing like host"):
                node.query("ALTER USER user22 DROP HOST LIKE 'local%'")
            with And("I alter user by removing host ip"):
                node.query("ALTER USER user22 DROP HOST IP '127.0.0.1'")
            with And("I alter user by removing host name"):
                node.query("ALTER USER user22 DROP HOST NAME 'localhost'")
