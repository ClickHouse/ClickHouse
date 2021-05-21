from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("drop settings profile")
def feature(self, node="clickhouse1"):
    """Check drop settings profile query syntax.

    ```sql
    DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(profile):
        try:
            with Given("I have a settings profile"):
                node.query(f"CREATE SETTINGS PROFILE {profile}")
            yield
        finally:
            with Finally("I drop the settings profile"):
                node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")

    def cleanup_profile(profile):
        with Given(f"I ensure that profile {profile} does not exist"):
            node.query(f"DROP SETTINGS PROFILE IF EXISTS {profile}")

    with Scenario("I drop settings profile with no options", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop("1.0")]):
        with cleanup("profile0"):
            with When("I drop settings profile"):
                node.query("DROP SETTINGS PROFILE profile0")

    with Scenario("I drop settings profile, does not exist, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop("1.0")]):
        profile = "profile0"
        cleanup_profile(profile)
        with When("I drop settings profile"):
            exitcode, message = errors.settings_profile_not_found_in_disk(name=profile)
            node.query("DROP SETTINGS PROFILE profile0", exitcode=exitcode, message=message)
        del profile

    with Scenario("I drop settings profile short form", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop("1.0")]):
        with cleanup("profile1"):
            with When("I drop settings profile short form"):
                node.query("DROP PROFILE profile1")

    with Scenario("I drop settings profile if exists, profile does exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop_IfExists("1.0")]):
        with cleanup("profile2"):
            with When("I drop settings profile if exists"):
                node.query("DROP SETTINGS PROFILE IF EXISTS profile2")

    with Scenario("I drop settings profile if exists, profile does not exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop_IfExists("1.0")]):
        cleanup_profile("profile2")
        with When("I drop settings profile if exists"):
            node.query("DROP SETTINGS PROFILE IF EXISTS profile2")

    with Scenario("I drop default settings profile, throws error", requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop("1.0")]):
        with When("I drop default profile"):
            exitcode, message = errors.cannot_remove_settings_profile_default()
            node.query("DROP SETTINGS PROFILE default", exitcode=exitcode, message=message)

    with Scenario("I drop multiple settings profiles", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop("1.0")]):
        with cleanup("profile3"), cleanup("profile4"):
            with When("I drop multiple settings profiles"):
                node.query("DROP SETTINGS PROFILE profile3, profile4")

    with Scenario("I drop settings profile on cluster", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop_OnCluster("1.0")]):
        try:
            with Given("I have a settings profile"):
                node.query("CREATE SETTINGS PROFILE profile5 ON CLUSTER sharded_cluster")
            with When("I run drop settings profile command"):
                node.query("DROP SETTINGS PROFILE profile5 ON CLUSTER sharded_cluster")
        finally:
            with Finally("I drop the profile in case it still exists"):
                node.query("DROP SETTINGS PROFILE IF EXISTS profile5 ON CLUSTER sharded_cluster")

    with Scenario("I drop settings profile on fake cluster, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_Drop_OnCluster("1.0")]):
        with When("I run drop settings profile command"):
            exitcode, message = errors.cluster_not_found("fake_cluster")
            node.query("DROP SETTINGS PROFILE profile6 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)
