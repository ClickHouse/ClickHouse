from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM TTL MERGES` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        table_name = f"table_name_{getuid()}"

        Suite(run=check_privilege,
            examples=Examples("privilege on grant_target_name user_name table_name", [
                tuple(list(row)+[user_name,user_name,table_name]) for row in check_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM TTL MERGES` commands if and only if
    the privilege has been granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        table_name = f"table_name_{getuid()}"

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_privilege,
            examples=Examples("privilege on grant_target_name user_name table_name", [
                tuple(list(row)+[role_name,user_name,table_name]) for row in check_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("SYSTEM", "*.*"),
    ("SYSTEM TTL MERGES", "table"),
    ("SYSTEM STOP TTL MERGES", "table"),
    ("SYSTEM START TTL MERGES", "table"),
    ("START TTL MERGES", "table"),
    ("STOP TTL MERGES", "table"),
])
def check_privilege(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Run checks for commands that require SYSTEM TTL MERGES privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=start_ttl_merges)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)
    Suite(test=stop_ttl_merges)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)

@TestSuite
def start_ttl_merges(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is only able to execute `SYSTEM START TTL MERGES` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name):

        with Scenario("SYSTEM START TTL MERGES without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't start merges"):
                node.query(f"SYSTEM START TTL MERGES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM START TTL MERGES with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can start merges"):
                node.query(f"SYSTEM START TTL MERGES {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM START TTL MERGES with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't start merges"):
                node.query(f"SYSTEM START TTL MERGES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestSuite
def stop_ttl_merges(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is only able to execute `SYSTEM STOP TTL MERGES` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name):

        with Scenario("SYSTEM STOP TTL MERGES without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't stop merges"):
                node.query(f"SYSTEM STOP TTL MERGES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM STOP TTL MERGES with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can stop merges"):
                node.query(f"SYSTEM STOP TTL MERGES {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM STOP TTL MERGES with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't stop merges"):
                node.query(f"SYSTEM STOP TTL MERGES {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("system ttl merges")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_TTLMerges("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM TTL MERGES.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
