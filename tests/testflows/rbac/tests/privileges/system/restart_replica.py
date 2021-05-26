from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SYSTEM RESTART REPLICA` commands if and only if
    the privilege has been granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=restart_replica,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in restart_replica.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SYSTEM RESTART REPLICA` commands if and only if
    the privilege has been granted via role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=restart_replica,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in restart_replica.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("SYSTEM", "*.*"),
    ("SYSTEM RESTART REPLICA", "table"),
    ("RESTART REPLICA", "table"),
])
def restart_replica(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `SYSTEM RESTARTE REPLICA` when they have privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)
    table_name = f"table_name_{getuid()}"

    if node is None:
        node = self.context.node

    on = on.replace("table", f"{table_name}")

    with table(node, table_name, "ReplicatedMergeTree-sharded_cluster"):

        with Scenario("SYSTEM RESTART REPLICA without privilege"):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't restart replica"):
                node.query(f"SYSTEM RESTART REPLICA {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with Scenario("SYSTEM RESTART REPLICA with privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user can restart replica"):
                node.query(f"SYSTEM RESTART REPLICA {table_name}", settings = [("user", f"{user_name}")])

        with Scenario("SYSTEM RESTART REPLICA with revoked privilege"):

            with When(f"I grant {privilege} on the table"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the table"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user can't restart replica"):
                node.query(f"SYSTEM RESTART REPLICA {table_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("system restart replica")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_System_RestartReplica("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SYSTEM RESTART REPLICA.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)
