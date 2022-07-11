from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def replicated_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM REPLICATED SENDS` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=check_replicated_privilege,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in check_replicated_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def replicated_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM REPLICATED SENDS` commands if and only if
    the privilege has been granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_replicated_privilege,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in check_replicated_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("SYSTEM", "*.*"),
    ("SYSTEM SENDS", "*.*"),
    ("SYSTEM START SENDS", "*.*"),
    ("SYSTEM STOP SENDS", "*.*"),
    ("START SENDS", "*.*"),
    ("STOP SENDS", "*.*"),
    ("SYSTEM REPLICATED SENDS", "table"),
    ("SYSTEM STOP REPLICATED SENDS", "table"),
    ("SYSTEM START REPLICATED SENDS", "table"),
    ("START REPLICATED SENDS", "table"),
    ("STOP REPLICATED SENDS", "table"),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Sends_Replicated("1.0"),
)
def check_replicated_privilege(self, privilege, on, grant_target_name, user_name, node=None):
    """Run checks for commands that require SYSTEM REPLICATED SENDS privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=start_replicated_sends)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name)
    Suite(test=stop_replicated_sends)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name)

@TestSuite
def start_replicated_sends(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM START REPLICATED SENDS` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table_name = f"table_name_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name, "ReplicatedMergeTree-sharded_cluster"):

        with Scenario("SYSTEM START REPLICATED SENDS without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't start sends"):
                node.query(f"SYSTEM START REPLICATED SENDS {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM START REPLICATED SENDS with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can start sends"):
                node.query(f"SYSTEM START REPLICATED SENDS {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM START REPLICATED SENDS with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't start sends"):
                node.query(f"SYSTEM START REPLICATED SENDS {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestSuite
def stop_replicated_sends(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM STOP REPLICATED SENDS` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table_name = f"table_name_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name, "ReplicatedMergeTree-sharded_cluster"):

        with Scenario("SYSTEM STOP REPLICATED SENDS without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't stop sends"):
                node.query(f"SYSTEM STOP REPLICATED SENDS {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM STOP REPLICATED SENDS with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can stop sends"):
                node.query(f"SYSTEM STOP REPLICATED SENDS {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM STOP REPLICATED SENDS with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't stop sends"):
                node.query(f"SYSTEM STOP REPLICATED SENDS {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestSuite
def distributed_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM DISTRIBUTED SENDS` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        table_name = f"table_name_{getuid()}"

        Suite(run=check_distributed_privilege,
            examples=Examples("privilege on grant_target_name user_name table_name", [
                tuple(list(row)+[user_name,user_name,table_name]) for row in check_distributed_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def distributed_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM DISTRIBUTED SENDS` commands if and only if
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

        Suite(run=check_distributed_privilege,
            examples=Examples("privilege on grant_target_name user_name table_name", [
                tuple(list(row)+[role_name,user_name,table_name]) for row in check_distributed_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("SYSTEM", "*.*"),
    ("SYSTEM SENDS", "*.*"),
    ("SYSTEM START SENDS", "*.*"),
    ("SYSTEM STOP SENDS", "*.*"),
    ("START SENDS", "*.*"),
    ("STOP SENDS", "*.*"),
    ("SYSTEM DISTRIBUTED SENDS", "table"),
    ("SYSTEM STOP DISTRIBUTED SENDS", "table"),
    ("SYSTEM START DISTRIBUTED SENDS", "table"),
    ("START DISTRIBUTED SENDS", "table"),
    ("STOP DISTRIBUTED SENDS", "table"),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Sends_Distributed("1.0"),
)
def check_distributed_privilege(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Run checks for commands that require SYSTEM DISTRIBUTED SENDS privilege.
    """

    if node is None:
        node = self.context.node

    Suite(test=start_distributed_moves)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)
    Suite(test=stop_distributed_moves)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, table_name=table_name)

@TestSuite
def start_distributed_moves(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is only able to execute `SYSTEM START DISTRIBUTED SENDS` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table0_name = f"table0_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table0_name):
        try:
            with Given("I have a distributed table"):
                node.query(f"CREATE TABLE {table_name} (a UInt64) ENGINE = Distributed(sharded_cluster, default, {table0_name}, rand())")

            with Scenario("SYSTEM START DISTRIBUTED SENDS without privilege"):

                with When("I grant the user NONE privilege"):
                    node.query(f"GRANT NONE TO {grant_target_name}")

                with And("I grant the user USAGE privilege"):
                    node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

                with Then("I check the user can't start merges"):
                    node.query(f"SYSTEM START DISTRIBUTED SENDS {table_name}", settings = [("user", f"{user_name}")],
                        exitcode=exitcode, message=message)

            with Scenario("SYSTEM START DISTRIBUTED SENDS with privilege"):

                with When(f"I grant {privilege} on the table"):
                    node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

                with Then("I check the user can start merges"):
                    node.query(f"SYSTEM START DISTRIBUTED SENDS {table_name}", settings = [("user", f"{user_name}")])

            with Scenario("SYSTEM START DISTRIBUTED SENDS with revoked privilege"):

                with When(f"I grant {privilege} on the table"):
                    node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

                with And(f"I revoke {privilege} on the table"):
                    node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

                with Then("I check the user can't start merges"):
                    node.query(f"SYSTEM START DISTRIBUTED SENDS {table_name}", settings = [("user", f"{user_name}")],
                        exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the distributed table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

@TestSuite
def stop_distributed_moves(self, privilege, on, grant_target_name, user_name, table_name, node=None):
    """Check that user is only able to execute `SYSTEM STOP DISTRIBUTED SENDS` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table0_name = f"table0_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table0_name):
        try:
            with Given("I have a distributed table"):
                node.query(f"CREATE TABLE {table_name} (a UInt64) ENGINE = Distributed(sharded_cluster, default, {table0_name}, rand())")

            with Scenario("SYSTEM STOP DISTRIBUTED SENDS without privilege"):

                with When("I grant the user NONE privilege"):
                    node.query(f"GRANT NONE TO {grant_target_name}")

                with And("I grant the user USAGE privilege"):
                    node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

                with Then("I check the user can't stop merges"):
                    node.query(f"SYSTEM STOP DISTRIBUTED SENDS {table_name}", settings = [("user", f"{user_name}")],
                        exitcode=exitcode, message=message)

            with Scenario("SYSTEM STOP DISTRIBUTED SENDS with privilege"):

                with When(f"I grant {privilege} on the table"):
                    node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

                with Then("I check the user can stop merges"):
                    node.query(f"SYSTEM STOP DISTRIBUTED SENDS {table_name}", settings = [("user", f"{user_name}")])

            with Scenario("SYSTEM STOP DISTRIBUTED SENDS with revoked privilege"):

                with When(f"I grant {privilege} on the table"):
                    node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

                with And(f"I revoke {privilege} on the table"):
                    node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

                with Then("I check the user can't stop merges"):
                    node.query(f"SYSTEM STOP DISTRIBUTED SENDS {table_name}", settings = [("user", f"{user_name}")],
                        exitcode=exitcode, message=message)
        finally:
            with Finally("I drop the distributed table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")

@TestFeature
@Name("system sends")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_Sends("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM SENDS.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=replicated_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=replicated_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=distributed_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=distributed_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
