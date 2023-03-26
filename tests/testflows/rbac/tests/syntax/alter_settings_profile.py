from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *


@TestFeature
@Name("alter settings profile")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check alter settings profile query syntax.

    ```sql
    ALTER SETTINGS PROFILE [IF EXISTS] name
    [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
    [TO {user_or_role [,...] | NONE | ALL | ALL EXCEPT user_or_role [,...]]}
    ```
    """
    node = self.context.cluster.node(node)

    def cleanup_profile(profile):
        with Given(f"I ensure that profile {profile} does not exist"):
            node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")

    try:
        with Given("I have a profile and some users and roles"):
            node.query(f"CREATE SETTINGS PROFILE profile0")
            node.query(f"CREATE USER user0")
            node.query(f"CREATE ROLE role0")

        with Scenario(
            "I alter settings profile with no options",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter("1.0")],
        ):
            with When("I alter settings profile"):
                node.query("ALTER SETTINGS PROFILE profile0")

        with Scenario(
            "I alter settings profile short form",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter("1.0")],
        ):
            with When("I short form alter settings profile"):
                node.query("ALTER PROFILE profile0")

        with Scenario(
            "I alter settings profile that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter("1.0")],
        ):
            profile = "profile1"

            cleanup_profile(profile)
            with When(f"I alter settings profile {profile} that doesn't exist"):
                exitcode, message = errors.settings_profile_not_found_in_disk(
                    name=profile
                )
                node.query(
                    f"ALTER SETTINGS PROFILE {profile}",
                    exitcode=exitcode,
                    message=message,
                )
            del profile

        with Scenario(
            "I alter settings profile if exists",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_IfExists("1.0")],
        ):
            with When("I alter settings profile using if exists"):
                node.query("ALTER SETTINGS PROFILE IF EXISTS profile0")

        with Scenario(
            "I alter settings profile if exists, profile does not exist",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_IfExists("1.0")],
        ):
            profile = "profile1"

            cleanup_profile(profile)
            with When(f"I alter settings profile {profile} using if exists"):
                node.query(f"ALTER SETTINGS PROFILE IF EXISTS {profile}")

            del profile

        with Scenario(
            "I alter settings profile to rename, target available",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Rename("1.0")],
        ):
            with When("I alter settings profile by renaming it"):
                node.query("ALTER SETTINGS PROFILE profile0 RENAME TO profile0")

        with Scenario(
            "I alter settings profile to rename, target unavailable",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Rename("1.0")],
        ):
            new_profile = "profile1"

            try:
                with Given(f"Ensure target name {new_profile} is NOT available"):
                    node.query(f"CREATE SETTINGS PROFILE IF NOT EXISTS {new_profile}")

                with When(f"I try to rename to {new_profile}"):
                    exitcode, message = errors.cannot_rename_settings_profile(
                        name="profile0", name_new=new_profile
                    )
                    node.query(
                        f"ALTER SETTINGS PROFILE profile0 RENAME TO {new_profile}",
                        exitcode=exitcode,
                        message=message,
                    )
            finally:
                with Finally(f"I cleanup target name {new_profile}"):
                    node.query(f"DROP SETTINGS PROFILE IF EXISTS {new_profile}")

            del new_profile

        with Scenario(
            "I alter settings profile with a setting value",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables("1.0"),
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Value("1.0"),
            ],
        ):
            with When("I alter settings profile using settings"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage = 100000001"
                )

        with Scenario(
            "I alter settings profile with a setting value, does not exist, throws exception",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables("1.0"),
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Value("1.0"),
            ],
        ):
            with When("I alter settings profile using settings and nonexistent value"):
                exitcode, message = errors.unknown_setting("fake_setting")
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS fake_setting = 100000001",
                    exitcode=exitcode,
                    message=message,
                )

        with Scenario(
            "I alter settings profile with a min setting value",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints("1.0")
            ],
        ):
            with When("I alter settings profile using 2 minimum formats"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage MIN 100000001"
                )
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage MIN = 100000001"
                )

        with Scenario(
            "I alter settings profile with a max setting value",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints("1.0")
            ],
        ):
            with When("I alter settings profile using 2 maximum formats"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage MAX 100000001"
                )
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage MAX = 100000001"
                )

        with Scenario(
            "I alter settings profile with min and max setting values",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints("1.0")
            ],
        ):
            with When("I alter settings profile with both min and max"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage MIN 100000001 MAX 200000001"
                )

        with Scenario(
            "I alter settings profile with a readonly setting",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints("1.0")
            ],
        ):
            with When("I alter settings profile with with readonly"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage READONLY"
                )

        with Scenario(
            "I alter settings profile with a writable setting",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints("1.0")
            ],
        ):
            with When("I alter settings profile with writable"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0 SETTINGS max_memory_usage WRITABLE"
                )

        with Scenario(
            "I alter settings profile with inherited settings",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_Inherit("1.0")
            ],
        ):
            with When("I alter settings profile with inherit"):
                node.query("ALTER SETTINGS PROFILE profile0 SETTINGS INHERIT 'default'")

        with Scenario(
            "I alter settings profile with inherit, parent profile does not exist, throws exception",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_Inherit("1.0")
            ],
        ):
            profile = "profile3"
            with Given(f"I ensure that profile {profile} does not exist"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")
            with When("I alter settings profile inherit from nonexistant parent"):
                exitcode, message = errors.settings_profile_not_found_in_disk(profile)
                node.query(
                    f"ALTER PROFILE profile0 SETTINGS INHERIT {profile}",
                    exitcode=exitcode,
                    message=message,
                )
            del profile

        with Scenario(
            "I alter settings profile with multiple settings",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables("1.0"),
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Value("1.0"),
            ],
        ):
            with When("I alter settings profile with multiple settings"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0"
                    " SETTINGS max_memory_usage = 100000001"
                    " SETTINGS max_memory_usage_for_user = 100000001"
                )

        with Scenario(
            "I alter settings profile with multiple settings short form",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables("1.0"),
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Value("1.0"),
            ],
        ):
            with When("I alter settings profile with short form multiple settings"):
                node.query(
                    "ALTER SETTINGS PROFILE profile0"
                    " SETTINGS max_memory_usage = 100000001,"
                    " max_memory_usage_for_user = 100000001"
                )

        with Scenario(
            "I alter settings profile assigned to one role",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment("1.0")],
        ):
            with When("I alter settings profile with assignment to role"):
                node.query("ALTER SETTINGS PROFILE profile0 TO role0")

        with Scenario(
            "I alter settings profile to assign to role that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment("1.0")],
        ):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(
                f"I alter a settings profile, assign to role {role}, which does not exist"
            ):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(
                    f"ALTER SETTINGS PROFILE profile0 TO {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

        with Scenario(
            "I alter settings profile to assign to all except role that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment("1.0")],
        ):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(
                f"I alter a settings profile, assign to all except role {role}, which does not exist"
            ):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(
                    f"ALTER SETTINGS PROFILE profile0 TO ALL EXCEPT {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

        with Scenario(
            "I alter settings profile assigned to multiple roles",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment("1.0")],
        ):
            with When("I alter settings profile with assignment to multiple roles"):
                node.query("ALTER SETTINGS PROFILE profile0 TO role0, user0")

        with Scenario(
            "I alter settings profile assigned to all",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_All("1.0")],
        ):
            with When("I alter settings profile with assignment to all"):
                node.query("ALTER SETTINGS PROFILE profile0 TO ALL")

        with Scenario(
            "I alter settings profile assigned to all except one role",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_AllExcept("1.0")
            ],
        ):
            with When("I alter settings profile with assignment to all except a role"):
                node.query("ALTER SETTINGS PROFILE profile0 TO ALL EXCEPT role0")

        with Scenario(
            "I alter settings profile assigned to all except multiple roles",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_AllExcept("1.0")
            ],
        ):
            with When(
                "I alter settings profile with assignmentto all except multiple roles"
            ):
                node.query("ALTER SETTINGS PROFILE profile0 TO ALL EXCEPT role0, user0")

        with Scenario(
            "I alter settings profile assigned to none",
            requirements=[RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_None("1.0")],
        ):
            with When("I alter settings profile with assignment to none"):
                node.query("ALTER SETTINGS PROFILE profile0 TO NONE")

        with Scenario(
            "I alter settings profile on cluster",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_OnCluster("1.0")
            ],
        ):
            try:
                with Given("I have a settings profile on cluster"):
                    node.query(
                        "CREATE SETTINGS PROFILE profile1 ON CLUSTER sharded_cluster"
                    )
                with When("I run alter settings profile command"):
                    node.query(
                        "ALTER SETTINGS PROFILE profile1 ON CLUSTER sharded_cluster"
                    )
                with And("I alter settings profile with settings"):
                    node.query(
                        "ALTER SETTINGS PROFILE profile1 ON CLUSTER sharded_cluster SETTINGS max_memory_usage = 100000001"
                    )
                with And("I alter settings profile with inherit"):
                    node.query(
                        "ALTER SETTINGS PROFILE profile1 ON CLUSTER sharded_cluster SETTINGS INHERIT 'default'"
                    )
                with And("I alter settings profile to all"):
                    node.query(
                        "ALTER SETTINGS PROFILE profile1 ON CLUSTER sharded_cluster TO ALL"
                    )
            finally:
                with Finally("I drop the settings profile"):
                    node.query(
                        "DROP SETTINGS PROFILE IF EXISTS profile1 ON CLUSTER sharded_cluster"
                    )

        with Scenario(
            "I alter settings profile on fake cluster, throws exception",
            requirements=[
                RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_OnCluster("1.0")
            ],
        ):
            with When("I run alter settings profile command"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query(
                    "ALTER SETTINGS PROFILE profile1 ON CLUSTER fake_cluster",
                    exitcode=exitcode,
                    message=message,
                )

    finally:
        with Finally("I drop the profile and all the users and roles"):
            node.query(f"DROP SETTINGS PROFILE IF EXISTS profile0")
            node.query(f"DROP USER IF EXISTS user0")
            node.query(f"DROP ROLE IF EXISTS role0")
