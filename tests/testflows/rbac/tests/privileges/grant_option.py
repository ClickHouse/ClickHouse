from multiprocessing.dummy import Pool

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def grant_option(self, table_type, privilege, node=None):
    """Check that user is able to execute GRANT and REVOKE privilege statements if and only if they have the privilege WITH GRANT OPTION,
    either directly or through a role.
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    role0_name = f"role0_{getuid()}"
    role1_name = f"role1_{getuid()}"
    if node is None:
        node = self.context.node

    with Suite("user with direct privilege granting to user"):
        with user(node, f"{user0_name},{user1_name}"):
            with When(f"I run checks that grant and revoke privilege from {user0_name} to {user1_name}"):
                grant_option_check(grant_option_target=user0_name, grant_target=user1_name, user_name=user0_name, table_type=table_type, privilege=privilege, node=node)

    with Suite("user with direct privilege granting to role"):
        with user(node, user0_name), role(node, role1_name):
            with When(f"I run checks that grant and revoke privilege from {user0_name} to {role1_name}"):
                grant_option_check(grant_option_target=user0_name, grant_target=role1_name, user_name=user0_name, table_type=table_type, privilege=privilege, node=node)

    with Suite("user with privilege via role granting to user"):
        with user(node, f"{user0_name},{user1_name}"), role(node, role0_name):
            with When("I grant the role to the user"):
                node.query(f"GRANT {role0_name} TO {user0_name}")
            with When(f"I run checks that grant and revoke privilege from {user0_name} with {role0_name} to {user1_name}"):
                grant_option_check(grant_option_target=role0_name, grant_target=user1_name, user_name=user0_name, table_type=table_type, privilege=privilege, node=node)

    with Suite("user with privilege via role granting to role"):
        with user(node, user0_name), role(node, f"{role0_name},{role1_name}"):
            with When("I grant the role to the user"):
                node.query(f"GRANT {role0_name} TO {user0_name}")
            with When(f"I run checks that grant and revoke privilege from {user0_name} with {role0_name} to {role1_name}"):
                grant_option_check(grant_option_target=role0_name, grant_target=role1_name, user_name=user0_name, table_type=table_type, privilege=privilege, node=node)

def grant_option_check(grant_option_target, grant_target, user_name, table_type, privilege, node=None):
    """Run different scenarios to check the user's access with different privileges.
    """
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("grant by user without privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, name=table_name, table_type_name=table_type):
            with Then("I attempt to grant delete privilege without privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_target}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

    with Scenario("grant by user with grant option privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, name=table_name, table_type_name=table_type):
            with When("I grant delete privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_option_target} WITH GRANT OPTION")
            with Then("I attempt to grant delete privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_target}", settings = [("user", user_name)])

    with Scenario("revoke by user with grant option privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, name=table_name, table_type_name=table_type):
            with When("I grant delete privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_option_target} WITH GRANT OPTION")
            with Then("I attempt to revoke delete privilege"):
                node.query(f"REVOKE {privilege} ON {table_name} FROM {grant_target}", settings = [("user", user_name)])

    with Scenario("grant by user with revoked grant option privilege", setup=instrument_clickhouse_server_log):
        table_name = f"merge_tree_{getuid()}"
        with table(node, name=table_name, table_type_name=table_type):
            with When(f"I grant delete privilege with grant option to {grant_option_target}"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_option_target} WITH GRANT OPTION")
            with And(f"I revoke delete privilege with grant option from {grant_option_target}"):
                node.query(f"REVOKE {privilege} ON {table_name} FROM {grant_option_target}")
            with Then("I attempt to grant delete privilege"):
                node.query(f"GRANT {privilege} ON {table_name} TO {grant_target}", settings = [("user", user_name)],
                    exitcode=exitcode, message=message)

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_GrantOption("1.0"),
)
@Examples("privilege", [
    ("ALTER MOVE PARTITION",), ("ALTER MOVE PART",), ("MOVE PARTITION",), ("MOVE PART",),
    ("ALTER DELETE",), ("DELETE",),
    ("ALTER FETCH PARTITION",), ("FETCH PARTITION",),
    ("ALTER FREEZE PARTITION",), ("FREEZE PARTITION",),
    ("ALTER UPDATE",), ("UPDATE",),
    ("ALTER ADD COLUMN",), ("ADD COLUMN",),
    ("ALTER CLEAR COLUMN",), ("CLEAR COLUMN",),
    ("ALTER MODIFY COLUMN",), ("MODIFY COLUMN",),
    ("ALTER RENAME COLUMN",), ("RENAME COLUMN",),
    ("ALTER COMMENT COLUMN",), ("COMMENT COLUMN",),
    ("ALTER DROP COLUMN",), ("DROP COLUMN",),
    ("ALTER COLUMN",),
    ("ALTER SETTINGS",), ("ALTER SETTING",), ("ALTER MODIFY SETTING",), ("MODIFY SETTING",),
    ("ALTER ORDER BY",), ("ALTER MODIFY ORDER BY",), ("MODIFY ORDER BY",),
    ("ALTER SAMPLE BY",), ("ALTER MODIFY SAMPLE BY",), ("MODIFY SAMPLE BY",),
    ("ALTER ADD INDEX",), ("ADD INDEX",),
    ("ALTER MATERIALIZE INDEX",), ("MATERIALIZE INDEX",),
    ("ALTER CLEAR INDEX",), ("CLEAR INDEX",),
    ("ALTER DROP INDEX",), ("DROP INDEX",),
    ("ALTER INDEX",), ("INDEX",),
    ("ALTER TTL",), ("ALTER MODIFY TTL",), ("MODIFY TTL",),
    ("ALTER MATERIALIZE TTL",), ("MATERIALIZE TTL",),
    ("ALTER ADD CONSTRAINT",), ("ADD CONSTRAINT",),
    ("ALTER DROP CONSTRAINT",), ("DROP CONSTRAINT",),
    ("ALTER CONSTRAINT",), ("CONSTRAINT",),
    ("INSERT",),
    ("SELECT",),
])
@Name("grant option")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of privileges with GRANT OPTION.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    pool = Pool(12)
    try:
        tasks = []
        try:
            for example in self.examples:
                privilege, = example
                run_scenario(pool, tasks, Suite(test=grant_option, name=privilege, setup=instrument_clickhouse_server_log), {"table_type": "MergeTree", "privilege": privilege})
        finally:
            join(tasks)
    finally:
        pool.close()
