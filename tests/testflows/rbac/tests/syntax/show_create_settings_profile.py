from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *

@TestFeature
@Name("show create settings profile")
def feature(self, node="clickhouse1"):
    """Check show create settings profile query syntax.

    ```sql
    SHOW CREATE [SETTINGS] PROFILE name
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

    with Scenario("I show create settings profile", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_ShowCreateSettingsProfile("1.0")]):
        with cleanup("profile0"):
            with When("I run show create settings profile command"):
                node.query("SHOW CREATE SETTINGS PROFILE profile0")

    with Scenario("I show create settings profile short form", flags=TE, requirements=[
            RQ_SRS_006_RBAC_SettingsProfile_ShowCreateSettingsProfile("1.0")]):
        with cleanup("profile1"):
            with When("I run show create settings profile command"):
                node.query("SHOW CREATE PROFILE profile1")
