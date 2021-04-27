from contextlib import contextmanager

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@contextmanager
def quota(node, name):
    try:
        with Given("I have a quota"):
            node.query(f"CREATE QUOTA {name}")

        yield

    finally:
        with Finally("I drop the quota"):
            node.query(f"DROP QUOTA IF EXISTS {name}")

@TestSuite
def alter_quota_granted_directly(self, node=None):
    """Check that a user is able to execute `ALTER QUOTA` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=alter_quota,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in alter_quota.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def alter_quota_granted_via_role(self, node=None):
    """Check that a user is able to execute `ALTER QUOTA` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=alter_quota,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in alter_quota.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("ACCESS MANAGEMENT",),
    ("ALTER QUOTA",),
])
def alter_quota(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `ALTER QUOTA` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("ALTER QUOTA without privilege"):
        alter_quota_name = f"alter_quota_{getuid()}"

        with quota(node, alter_quota_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't alter a quota"):
                node.query(f"ALTER QUOTA {alter_quota_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("ALTER QUOTA with privilege"):
        alter_quota_name = f"alter_quota_{getuid()}"

        with quota(node, alter_quota_name):

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can alter a user"):
                node.query(f"ALTER QUOTA {alter_quota_name}", settings = [("user", f"{user_name}")])

    with Scenario("ALTER QUOTA on cluster"):
        alter_quota_name = f"alter_quota_{getuid()}"

        try:
            with Given("I have a quota on a cluster"):
                node.query(f"CREATE QUOTA {alter_quota_name} ON CLUSTER sharded_cluster")

            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can alter a quota"):
                node.query(f"ALTER QUOTA {alter_quota_name} ON CLUSTER sharded_cluster",
                    settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the quota"):
                node.query(f"DROP QUOTA IF EXISTS {alter_quota_name} ON CLUSTER sharded_cluster")

    with Scenario("ALTER QUOTA with revoked privilege"):
        alter_quota_name = f"alter_quota_{getuid()}"

        with quota(node, alter_quota_name):

            with When(f"I grant {privilege} on the database"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege} on the database"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user can't alter a quota"):
                node.query(f"ALTER QUOTA {alter_quota_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("alter quota")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterQuota("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of ALTER QUOTA.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=alter_quota_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=alter_quota_granted_via_role, setup=instrument_clickhouse_server_log)
