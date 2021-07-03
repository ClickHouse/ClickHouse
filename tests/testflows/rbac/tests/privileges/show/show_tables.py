from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def table_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `CHECK` and `EXISTS`
    commands on a table and see the table when they execute `SHOW TABLE` command
    if and only if they have any privilege on that table granted directly.
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
def table_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `CHECK` and `EXISTS`
    commands on a table and see the table when they execute `SHOW TABLE` command
    if and only if they have any privilege on that table granted via role.
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
    ("SHOW", "*.*"),
    ("SHOW TABLES", "table"),
    ("SELECT", "table"),
    ("INSERT", "table"),
    ("ALTER", "table"),
    ("SELECT(a)", "table"),
    ("INSERT(a)", "table"),
    ("ALTER(a)", "table"),
])
def check_privilege(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Run checks for commands that require SHOW TABLE privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=show_tables)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)
    Suite(test=exists)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)
    Suite(test=check)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ShowTables_RequiredPrivilege("1.0"),
)
def show_tables(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is only able to see a table in SHOW TABLES when they have a privilege on that table.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name):

        with Scenario("SHOW TABLES without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user doesn't see the table"):
                output = node.query("SHOW TABLES", settings = [("user", f"{user_name}")]).output
                assert output == '', error()

        with Scenario("SHOW TABLES with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user does see a table"):
                node.query("SHOW TABLES", settings = [("user", f"{user_name}")], message=f"{table_name}")

        with Scenario("SHOW TABLES with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user does not see a table"):
                output = node.query("SHOW TABLES", settings = [("user", f"{user_name}")]).output
                assert output == '', error()

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_ExistsTable_RequiredPrivilege("1.0"),
)
def exists(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is able to execute EXISTS on a table if and only if the user has SHOW TABLE privilege
    on that table.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    if on == "table":
        on = f"{table_name}"

    with table(node, table_name):

        with Scenario("EXISTS without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then(f"I check if {table_name} EXISTS"):
                node.query(f"EXISTS {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        with Scenario("EXISTS with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then(f"I check if {table_name} EXISTS"):
                node.query(f"EXISTS {table_name}", settings=[("user",user_name)])

        with Scenario("EXISTS with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then(f"I check if {table_name} EXISTS"):
                node.query(f"EXISTS {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_CheckTable_RequiredPrivilege("1.0"),
)
def check(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is able to execute CHECK on a table if and only if the user has SHOW TABLE privilege
    on that table.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    if on == "table":
        on = f"{table_name}"

    with table(node, table_name):

        with Scenario("CHECK without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then(f"I CHECK {table_name}"):
                node.query(f"CHECK TABLE {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        with Scenario("CHECK with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then(f"I CHECK {table_name}"):
                node.query(f"CHECK TABLE {table_name}", settings=[("user",user_name)])

        with Scenario("CHECK with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then(f"I CHECK {table_name}"):
                node.query(f"CHECK TABLE {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("show tables")
@Requirements(
    RQ_SRS_006_RBAC_ShowTables_Privilege("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW TABLES.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=table_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=table_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
