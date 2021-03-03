from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def describe_with_privilege_granted_directly(self, node=None):
    """Check that user is able to execute DESCRIBE on a table if and only if
    they have SHOW COLUMNS privilege for that table granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        table_name = f"table_name_{getuid()}"

        Suite(test=describe, setup=instrument_clickhouse_server_log)(grant_target_name=user_name, user_name=user_name, table_name=table_name)

@TestSuite
def describe_with_privilege_granted_via_role(self, node=None):
    """Check that user is able to execute DESCRIBE on a table if and only if
    they have SHOW COLUMNS privilege for that table granted through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        table_name = f"table_name_{getuid()}"

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(test=describe, setup=instrument_clickhouse_server_log)(grant_target_name=role_name, user_name=user_name, table_name=table_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_DescribeTable("1.0"),
)
def describe(self, grant_target_name, user_name, table_name, node=None):
    """Check that user is able to execute DESCRIBE only when they have SHOW COLUMNS privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Scenario("DESCRIBE table without privilege"):
            with When(f"I attempt to DESCRIBE {table_name}"):
                node.query(f"DESCRIBE {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        with Scenario("DESCRIBE with privilege"):
            with When(f"I grant SHOW COLUMNS on the table"):
                node.query(f"GRANT SHOW COLUMNS ON {table_name} TO {grant_target_name}")

            with Then(f"I attempt to DESCRIBE {table_name}"):
                node.query(f"DESCRIBE TABLE {table_name}", settings=[("user",user_name)])

        with Scenario("DESCRIBE with revoked privilege"):
            with When(f"I grant SHOW COLUMNS on the table"):
                node.query(f"GRANT SHOW COLUMNS ON {table_name} TO {grant_target_name}")

            with And(f"I revoke SHOW COLUMNS on the table"):
                node.query(f"REVOKE SHOW COLUMNS ON {table_name} FROM {grant_target_name}")

            with Then(f"I attempt to DESCRIBE {table_name}"):
                node.query(f"DESCRIBE {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestSuite
def show_create_with_privilege_granted_directly(self, node=None):
    """Check that user is able to execute SHOW CREATE on a table if and only if
    they have SHOW COLUMNS privilege for that table granted directly.
    """
    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        table_name = f"table_name_{getuid()}"

        Suite(test=show_create, setup=instrument_clickhouse_server_log)(grant_target_name=user_name, user_name=user_name, table_name=table_name)

@TestSuite
def show_create_with_privilege_granted_via_role(self, node=None):
    """Check that user is able to execute SHOW CREATE on a table if and only if
    they have SHOW COLUMNS privilege for that table granted directly.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        table_name = f"table_name_{getuid()}"

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(test=show_create, setup=instrument_clickhouse_server_log)(grant_target_name=role_name, user_name=user_name, table_name=table_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowCreateTable("1.0"),
)
def show_create(self, grant_target_name, user_name, table_name, node=None):
    """Check that user is able to execute SHOW CREATE on a table only when they have SHOW COLUMNS privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with table(node, table_name):

        with Scenario("SHOW CREATE without privilege"):
            with When(f"I attempt to SHOW CREATE {table_name}"):
                node.query(f"SHOW CREATE TABLE {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        with Scenario("SHOW CREATE with privilege"):
            with When(f"I grant SHOW COLUMNS on the table"):
                node.query(f"GRANT SHOW COLUMNS ON {table_name} TO {grant_target_name}")

            with Then(f"I attempt to SHOW CREATE {table_name}"):
                node.query(f"SHOW CREATE TABLE {table_name}", settings=[("user",user_name)])

        with Scenario("SHOW CREATE with revoked privilege"):
            with When(f"I grant SHOW COLUMNS on the table"):
                node.query(f"GRANT SHOW COLUMNS ON {table_name} TO {grant_target_name}")

            with And(f"I revoke SHOW COLUMNS on the table"):
                node.query(f"REVOKE SHOW COLUMNS ON {table_name} FROM {grant_target_name}")

            with Then(f"I attempt to SHOW CREATE {table_name}"):
                node.query(f"SHOW CREATE TABLE {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Name("show columns")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowColumns("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW COLUMNS.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=describe_with_privilege_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=describe_with_privilege_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=show_create_with_privilege_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=show_create_with_privilege_granted_via_role, setup=instrument_clickhouse_server_log)
