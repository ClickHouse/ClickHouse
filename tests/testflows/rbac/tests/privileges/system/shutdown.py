import time

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Run checks with privileges granted directly.
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
    """Run checks with privileges granted through a role.
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
    ("SYSTEM",),
    ("SYSTEM SHUTDOWN",),
    ("SHUTDOWN",),
    ("SYSTEM KILL",),
])
def check_privilege(self, privilege, grant_target_name, user_name, node=None):
    """Run checks for commands that require SYSTEM SHUTDOWN privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=shutdown)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)
    Suite(test=kill)(privilege=privilege, grant_target_name=grant_target_name, user_name=user_name)

@TestSuite
def shutdown(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM SHUTDOWN` when they have the necessary privilege.
    """
    cluster = self.context.cluster

    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM SHUTDOWN without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use SYSTEM SHUTDOWN"):
            node.query(f"SYSTEM SHUTDOWN", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("SYSTEM SHUTDOWN with privilege"):
        timeout = 60

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can use SYSTEM SHUTDOWN"):
                node.query(f"SYSTEM SHUTDOWN", settings = [("user", f"{user_name}")])

            with And("I close all connections to the node"):
                node.close_bashes()

            with And("I check that system is down"):
                command = f"echo -e \"SELECT 1\" | {cluster.docker_compose} exec -T {node.name} clickhouse client -n"

                start_time = time.time()

                while True:
                    r = cluster.bash(None)(command)
                    if r.exitcode != 0:
                        break
                    if time.time() - start_time > timeout:
                        break
                    time.sleep(1)

                assert r.exitcode != 0, error(r.output)

        finally:
            with Finally("I relaunch the server"):
                node.restart(safe=False)

    with Scenario("SYSTEM SHUTDOWN with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use SYSTEM SHUTDOWN"):
            node.query(f"SYSTEM SHUTDOWN", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def kill(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM KILL` when they have the necessary privilege.
    """
    cluster = self.context.cluster

    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("SYSTEM KILL without privilege"):

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use SYSTEM KILL"):
            node.query(f"SYSTEM KILL", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("SYSTEM KILL with privilege"):
        timeout = 60

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can use SYSTEM KILL"):
                command = f"echo -e \"SYSTEM KILL\" | clickhouse client -n"
                with By("executing command", description=command):
                    self.context.cluster.bash(node.name).send(command)

            with And("I close all connections to the node"):
                node.close_bashes()

            with And("I check that system is down"):
                command = f"echo -e \"SELECT 1\" | {cluster.docker_compose} exec -T {node.name} clickhouse client -n"

                start_time = time.time()

                while True:
                    r = cluster.bash(None)(command)
                    if r.exitcode != 0:
                        break
                    if time.time() - start_time > timeout:
                        break
                    time.sleep(1)

                assert r.exitcode != 0, error(r.output)

        finally:
            with Finally("I relaunch the server"):
                node.restart(safe=False)

    with Scenario("SYSTEM KILL with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use SYSTEM KILL"):
            node.query(f"SYSTEM KILL", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestFeature
@Name("system shutdown")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Shutdown("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM SHUTDOWN.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
