from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@contextmanager
def settings_profile(node, name):
    try:
        with Given("I have a settings_profile"):
            node.query(f"CREATE SETTINGS PROFILE {name}")

        yield

    finally:
        with Finally("I drop the settings_profile"):
            node.query(f"DROP SETTINGS PROFILE IF EXISTS {name}")

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SHOW SETTINGS PROFILES` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=check_privilege,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in check_privilege.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SHOW SETTINGS PROFILES` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_privilege,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in check_privilege.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("ACCESS MANAGEMENT",),
    ("SHOW ACCESS",),
    ("SHOW SETTINGS PROFILES",),
    ("SHOW PROFILES",),
    ("SHOW CREATE SETTINGS PROFILE",),
    ("SHOW CREATE PROFILE",),
])
def check_privilege(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for commands that require SHOW SETTINGS PROFILES privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=show_settings_profiles)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)
    Suite(test=show_create)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowSettingsProfiles_RequiredPrivilege("1.0"),
)
def show_settings_profiles(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SHOW SETTINGS PROFILES` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SHOW SETTINGS PROFILES without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use SHOW SETTINGS PROFILES"):
            node.query(f"SHOW SETTINGS PROFILES", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("SHOW SETTINGS PROFILES with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use SHOW SETTINGS PROFILES"):
            node.query(f"SHOW SETTINGS PROFILES", settings = [("user", f"{user_name}")])

    with Scenario("SHOW SETTINGS PROFILES with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use SHOW SETTINGS PROFILES"):
            node.query(f"SHOW SETTINGS PROFILES", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowCreateSettingsProfile_RequiredPrivilege("1.0"),
)
def show_create(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SHOW CREATE SETTINGS PROFILE` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SHOW CREATE SETTINGS PROFILE without privilege"):
        target_settings_profile_name = f"target_settings_profile_{getuid()}"

        with settings_profile(node, target_settings_profile_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't use SHOW CREATE SETTINGS PROFILE"):
                node.query(f"SHOW CREATE SETTINGS PROFILE {target_settings_profile_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("SHOW CREATE SETTINGS PROFILE with privilege"):
        target_settings_profile_name = f"target_settings_profile_{getuid()}"

        with settings_profile(node, target_settings_profile_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can use SHOW CREATE SETTINGS PROFILE"):
                node.query(f"SHOW CREATE SETTINGS PROFILE {target_settings_profile_name}", settings = [("user", f"{user_name}")])

    with Scenario("SHOW CREATE SETTINGS PROFILE with revoked privilege"):
        target_settings_profile_name = f"target_settings_profile_{getuid()}"

        with settings_profile(node, target_settings_profile_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot use SHOW CREATE SETTINGS PROFILE"):
                node.query(f"SHOW CREATE SETTINGS PROFILE {target_settings_profile_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("show settings profiles")
@Requirements(
    RQ_SRS_006_RBAC_ShowSettingsProfiles_Privilege("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW SETTINGS PROFILES.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
