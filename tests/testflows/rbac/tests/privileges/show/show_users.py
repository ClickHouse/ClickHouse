from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SHOW USERS` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=check_privilege, flags=TE,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in check_privilege.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SHOW USERS` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_privilege, flags=TE,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in check_privilege.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ACCESS MANAGEMENT",),
    ("SHOW ACCESS",),
    ("SHOW USERS",),
    ("SHOW CREATE USER",),
])
def check_privilege(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for commands that require SHOW USERS privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=show_users, setup=instrument_clickhouse_server_log)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)
    Suite(test=show_create, setup=instrument_clickhouse_server_log)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowUsers_Query("1.0"),
)
def show_users(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SHOW USERS` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SHOW USERS without privilege"):

        with When("I check the user can't use SHOW USERS"):
            node.query(f"SHOW USERS", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("SHOW USERS with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use SHOW USERS"):
            node.query(f"SHOW USERS", settings = [("user", f"{user_name}")])

    with Scenario("SHOW USERS with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use SHOW USERS"):
            node.query(f"SHOW USERS", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowCreateUser("1.0"),
)
def show_create(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SHOW CREATE USER` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SHOW CREATE USER without privilege"):
        target_user_name = f"target_user_{getuid()}"

        with user(node, target_user_name):

            with When("I check the user can't use SHOW CREATE USER"):
                node.query(f"SHOW CREATE USER {target_user_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("SHOW CREATE USER with privilege"):
        target_user_name = f"target_user_{getuid()}"

        with user(node, target_user_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can use SHOW CREATE USER"):
                node.query(f"SHOW CREATE USER {target_user_name}", settings = [("user", f"{user_name}")])

    with Scenario("SHOW CREATE USER with revoked privilege"):
        target_user_name = f"target_user_{getuid()}"

        with user(node, target_user_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot use SHOW CREATE USER"):
                node.query(f"SHOW CREATE USER {target_user_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("show users")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowUsers("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW USERS.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
