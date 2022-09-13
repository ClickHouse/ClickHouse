from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *


@TestFeature
@Name("create settings profile")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check create settings profile query syntax.

    ```sql
    CREATE [SETTINGS] PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value]
        [READONLY] | [INHERIT|PROFILE 'profile_name']] [,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(profile):
        try:
            with Given(f"I ensure the profile {profile} does not exist"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")
            yield
        finally:
            with Finally("I drop the profile"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")

    def create_profile(profile):
        with Given(f"I ensure I do have profile {profile}"):
            node.query(f"CREATE SETTINGS PROFILE OR REPLACE {profile}")

    try:
        with Given("I have a user and a role"):
            node.query(f"CREATE USER user0")
            node.query(f"CREATE ROLE role0")

        with Scenario(
            "I create settings profile with no options",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create("1.0")],
        ):
            with cleanup("profile0"):
                with When("I create settings profile"):
                    node.query("CREATE SETTINGS PROFILE profile0")

        with Scenario(
            "I create settings profile that already exists, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create("1.0")],
        ):
            profile = "profile0"
            with cleanup(profile):
                create_profile(profile)
                with When(f"I create settings profile {profile} that already exists"):
                    exitcode, message = errors.cannot_insert_settings_profile(
                        name=profile
                    )
                    node.query(
                        f"CREATE SETTINGS PROFILE {profile}",
                        exitcode=exitcode,
                        message=message,
                    )
            del profile

        with Scenario(
            "I create settings profile if not exists, profile does not exist",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_IfNotExists("1.0")],
        ):
            with cleanup("profile1"):
                with When("I create settings profile with if not exists"):
                    node.query("CREATE SETTINGS PROFILE IF NOT EXISTS profile1")

        with Scenario(
            "I create settings profile if not exists, profile does exist",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_IfNotExists("1.0")],
        ):
            profile = "profile1"
            with cleanup(profile):
                create_profile(profile)
                with When(f"I create settings profile {profile} with if not exists"):
                    node.query(f"CREATE SETTINGS PROFILE IF NOT EXISTS {profile}")
            del profile

        with Scenario(
            "I create settings profile or replace, profile does not exist",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Replace("1.0")],
        ):
            with cleanup("profile2"):
                with When("I create settings policy with or replace"):
                    node.query("CREATE SETTINGS PROFILE OR REPLACE profile2")

        with Scenario(
            "I create settings profile or replace, profile does exist",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Replace("1.0")],
        ):
            with cleanup("profile2"):
                create_profile("profile2")
                with When("I create settings policy with or replace"):
                    node.query("CREATE SETTINGS PROFILE OR REPLACE profile2")

        with Scenario(
            "I create settings profile short form",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create("1.0")],
        ):
            with cleanup("profile3"):
                with When("I create settings profile short form"):
                    node.query("CREATE PROFILE profile3")

        with Scenario(
            "I create settings profile with a setting value",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables("1.0"),
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Value("1.0"),
            ],
        ):
            with cleanup("profile4"):
                with When("I create settings profile with settings"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile4 SETTINGS max_memory_usage = 100000001"
                    )

        with Scenario(
            "I create settings profile with a setting value, does not exist, throws exception",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables("1.0"),
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Value("1.0"),
            ],
        ):
            with When("I create settings profile using settings and nonexistent value"):
                exitcode, message = errors.unknown_setting("fake_setting")
                node.query(
                    "CREATE SETTINGS PROFILE profile0 SETTINGS fake_setting = 100000001",
                    exitcode=exitcode,
                    message=message,
                )

        with Scenario(
            "I create settings profile with a min setting value",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile5"), cleanup("profile6"):
                with When(
                    "I create settings profile with min setting with and without equals"
                ):
                    node.query(
                        "CREATE SETTINGS PROFILE profile5 SETTINGS max_memory_usage MIN 100000001"
                    )
                    node.query(
                        "CREATE SETTINGS PROFILE profile6 SETTINGS max_memory_usage MIN = 100000001"
                    )

        with Scenario(
            "I create settings profile with a max setting value",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile7"), cleanup("profile8"):
                with When(
                    "I create settings profile with max setting with and without equals"
                ):
                    node.query(
                        "CREATE SETTINGS PROFILE profile7 SETTINGS max_memory_usage MAX 100000001"
                    )
                    node.query(
                        "CREATE SETTINGS PROFILE profile8 SETTINGS max_memory_usage MAX = 100000001"
                    )

        with Scenario(
            "I create settings profile with min and max setting values",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile9"):
                with When("I create settings profile with min and max setting"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile9 SETTINGS max_memory_usage MIN 100000001 MAX 200000001"
                    )

        with Scenario(
            "I create settings profile with a readonly setting",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile10"):
                with When("I create settings profile with readonly"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile10 SETTINGS max_memory_usage READONLY"
                    )

        with Scenario(
            "I create settings profile with a writable setting",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile21"):
                with When("I create settings profile with writable"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile21 SETTINGS max_memory_usage WRITABLE"
                    )

        with Scenario(
            "I create settings profile with inherited settings",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Inherit("1.0")],
        ):
            with cleanup("profile11"):
                with When("I create settings profile with inherit"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile11 SETTINGS INHERIT 'default'"
                    )

        with Scenario(
            "I create settings profile with inherit/from profile, fake profile, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Inherit("1.0")],
        ):
            profile = "profile3"
            with Given(f"I ensure that profile {profile} does not exist"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")
            sources = {"INHERIT", "PROFILE"}
            for source in sources:
                with When(
                    f"I create settings profile {source} from nonexistant parent"
                ):
                    exitcode, message = errors.settings_profile_not_found_in_disk(
                        profile
                    )
                    node.query(
                        f"CREATE PROFILE profile0 SETTINGS {source} {profile}",
                        exitcode=exitcode,
                        message=message,
                    )
            del profile

        with Scenario(
            "I create settings profile with inherited settings other form",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Inherit("1.0")],
        ):
            with cleanup("profile12"):
                with When("I create settings profile with inherit short form"):
                    node.query("CREATE PROFILE profile12 SETTINGS PROFILE 'default'")

        with Scenario(
            "I create settings profile with multiple settings",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile13"):
                with When("I create settings profile with multiple settings"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile13"
                        " SETTINGS max_memory_usage = 100000001"
                        " SETTINGS max_memory_usage_for_user = 100000001"
                    )

        with Scenario(
            "I create settings profile with multiple settings short form",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints("1.0")
            ],
        ):
            with cleanup("profile14"):
                with When(
                    "I create settings profile with multiple settings short form"
                ):
                    node.query(
                        "CREATE SETTINGS PROFILE profile14"
                        " SETTINGS max_memory_usage = 100000001,"
                        " max_memory_usage_for_user = 100000001"
                    )

        with Scenario(
            "I create settings profile assigned to one role",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment("1.0")],
        ):
            with cleanup("profile15"):
                with When("I create settings profile for a role"):
                    node.query("CREATE SETTINGS PROFILE profile15 TO role0")

        with Scenario(
            "I create settings profile to assign to role that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment("1.0")],
        ):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(
                f"I create a settings profile, assign to role {role}, which does not exist"
            ):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(
                    f"CREATE SETTINGS PROFILE profile0 TO {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

        with Scenario(
            "I create settings profile to assign to all except role that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment("1.0")],
        ):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(
                f"I create a settings profile, assign to all except role {role}, which does not exist"
            ):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(
                    f"CREATE SETTINGS PROFILE profile0 TO ALL EXCEPT {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

        with Scenario(
            "I create settings profile assigned to multiple roles",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment("1.0")],
        ):
            with cleanup("profile16"):
                with When("I create settings profile for multiple roles"):
                    node.query("CREATE SETTINGS PROFILE profile16 TO role0, user0")

        with Scenario(
            "I create settings profile assigned to all",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_All("1.0")],
        ):
            with cleanup("profile17"):
                with When("I create settings profile for all"):
                    node.query("CREATE SETTINGS PROFILE profile17 TO ALL")

        with Scenario(
            "I create settings profile assigned to all except one role",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_AllExcept("1.0")
            ],
        ):
            with cleanup("profile18"):
                with When("I create settings profile for all except one role"):
                    node.query("CREATE SETTINGS PROFILE profile18 TO ALL EXCEPT role0")

        with Scenario(
            "I create settings profile assigned to all except multiple roles",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_AllExcept("1.0")
            ],
        ):
            with cleanup("profile19"):
                with When("I create settings profile for all except multiple roles"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile19 TO ALL EXCEPT role0, user0"
                    )

        with Scenario(
            "I create settings profile assigned to none",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_None("1.0")
            ],
        ):
            with cleanup("profile22"):
                with When("I create settings profile for none"):
                    node.query("CREATE SETTINGS PROFILE profile22 TO NONE")

        with Scenario(
            "I create settings profile on cluster",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_OnCluster("1.0")],
        ):
            try:
                with When("I run create settings profile command"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile20 ON CLUSTER sharded_cluster"
                    )
                    node.query(
                        "CREATE SETTINGS PROFILE OR REPLACE profile20 ON CLUSTER sharded_cluster SETTINGS max_memory_usage = 100000001"
                    )
                    node.query(
                        "CREATE SETTINGS PROFILE OR REPLACE profile20 ON CLUSTER sharded_cluster SETTINGS INHERIT 'default'"
                    )
                    node.query(
                        "CREATE SETTINGS PROFILE OR REPLACE profile20 ON CLUSTER sharded_cluster TO ALL"
                    )
            finally:
                with Finally("I drop the settings profile"):
                    node.query(
                        "DROP SETTINGS PROFILE IF EXISTS profile20 ON CLUSTER sharded_cluster"
                    )

        with Scenario(
            "I create settings profile on fake cluster, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Create_OnCluster("1.0")],
        ):
            with When("I run create settings profile command"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query(
                    "CREATE SETTINGS PROFILE profile1 ON CLUSTER fake_cluster",
                    exitcode=exitcode,
                    message=message,
                )
    finally:
        with Finally("I drop all the users and roles"):
            node.query(f"DROP USER IF EXISTS user0")
            node.query(f"DROP ROLE IF EXISTS role0")
