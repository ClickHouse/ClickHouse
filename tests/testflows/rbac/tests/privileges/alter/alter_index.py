import json
import random

from testflows._core.cli.arg.parser import parser
from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors
from rbac.helper.tables import table_types

subprivileges = {
    "ORDER BY" : 1 << 0,
    "SAMPLE BY": 1 << 1,
    "ADD INDEX" : 1 << 2,
    "MATERIALIZE INDEX" : 1 << 3,
    "CLEAR INDEX": 1 << 4,
    "DROP INDEX": 1 << 5,
}

aliases = {
    "ORDER BY" : ["ALTER ORDER BY", "ALTER MODIFY ORDER BY", "MODIFY ORDER BY"],
    "SAMPLE BY": ["ALTER SAMPLE BY", "ALTER MODIFY SAMPLE BY", "MODIFY SAMPLE BY"],
    "ADD INDEX" : ["ALTER ADD INDEX", "ADD INDEX"],
    "MATERIALIZE INDEX" : ["ALTER MATERIALIZE INDEX", "MATERIALIZE INDEX"],
    "CLEAR INDEX": ["ALTER CLEAR INDEX", "CLEAR INDEX"],
    "DROP INDEX": ["ALTER DROP INDEX", "DROP INDEX"],
    "ALTER INDEX": ["ALTER INDEX", "INDEX"] # super-privilege
}

# Extra permutation is for 'ALTER INDEX' super-privilege
permutation_count = (1 << len(subprivileges))

def permutations(table_type):
    """Uses stress flag and table type, returns list of all permutations to run

    Stress test: All permutations, all tables (when stress=True)
    Sanity test: All permutations for MergeTree, selected permutations* for other tables
    """
    if current().context.stress or table_type == "MergeTree":
        return [*range(permutation_count + len(aliases["ALTER INDEX"]))]
    else:
        # *Selected permutations currently stand as [1,2,4,8,16,32,0,42,63,64,65].
        # Testing ["ORDER BY", "SAMPLE BY", "ADD INDEX", "MATERIALIZE INDEX", "CLEAR INDEX",
        # "DROP INDEX", "NONE", {"DROP, MATERIALIZE, SAMPLE BY"}, all, "ALTER INDEX", and "INDEX"]
        return [1 << index for index in range(len(subprivileges))] + \
            [0, int('101010', 2), permutation_count-1, permutation_count, permutation_count+1]

def alter_index_privileges(grants: int):
    """Takes in an integer, and returns the corresponding set of tests to grant and
    not grant using the binary string. Each integer corresponds to a unique permutation
    of grants.
    """
    note(grants)
    privileges = []

    # Extra iteration for ALTER INDEX
    if grants >= permutation_count:
        privileges.append(aliases["ALTER INDEX"][grants-permutation_count])
    elif grants==0: # No privileges
        privileges.append("NONE")
    else:
        if (grants & subprivileges["ORDER BY"]):
            privileges.append(aliases["ORDER BY"][grants % len(aliases["ORDER BY"])])
        if (grants & subprivileges["SAMPLE BY"]):
            privileges.append(aliases["SAMPLE BY"][grants % len(aliases["SAMPLE BY"])])
        if (grants & subprivileges["ADD INDEX"]):
            privileges.append(aliases["ADD INDEX"][grants % len(aliases["ADD INDEX"])])
        if (grants & subprivileges["MATERIALIZE INDEX"]):
            privileges.append(aliases["MATERIALIZE INDEX"][grants % len(aliases["MATERIALIZE INDEX"])])
        if (grants & subprivileges["CLEAR INDEX"]):
            privileges.append(aliases["CLEAR INDEX"][grants % len(aliases["CLEAR INDEX"])])
        if (grants & subprivileges["DROP INDEX"]):
            privileges.append(aliases["DROP INDEX"][grants % len(aliases["DROP INDEX"])])

    note(f"Testing these privileges: {privileges}")
    return ', '.join(privileges)

def alter_index_privilege_handler(grants, table, user, node):
    """For all 5 subprivileges, if the privilege is granted: run test to ensure correct behavior,
    and if the privilege is not granted, run test to ensure correct behavior there as well.
    """
    # Testing ALTER INDEX and INDEX is the same as testing all subprivileges
    if grants > permutation_count-1:
        grants = permutation_count-1

    if (grants & subprivileges["ORDER BY"]):
        with When("I check order by when privilege is granted"):
            check_order_by_when_privilege_is_granted(table, user, node)
    else:
        with When("I check order by when privilege is not granted"):
            check_order_by_when_privilege_is_not_granted(table, user, node)
    if (grants & subprivileges["SAMPLE BY"]):
        with When("I check sample by when privilege is granted"):
            check_sample_by_when_privilege_is_granted(table, user, node)
    else:
        with When("I check sample by when privilege is not granted"):
            check_sample_by_when_privilege_is_not_granted(table, user, node)
    if (grants & subprivileges["ADD INDEX"]):
        with When("I check add index when privilege is granted"):
            check_add_index_when_privilege_is_granted(table, user, node)
    else:
        with When("I check add index when privilege is not granted"):
            check_add_index_when_privilege_is_not_granted(table, user, node)
    if (grants & subprivileges["MATERIALIZE INDEX"]):
        with When("I check materialize index when privilege is granted"):
            check_materialize_index_when_privilege_is_granted(table, user, node)
    else:
        with When("I check materialize index when privilege is not granted"):
            check_materialize_index_when_privilege_is_not_granted(table, user, node)
    if (grants & subprivileges["CLEAR INDEX"]):
        with When("I check clear index when privilege is granted"):
            check_clear_index_when_privilege_is_granted(table, user, node)
    else:
        with When("I check clear index when privilege is not granted"):
            check_clear_index_when_privilege_is_not_granted(table, user, node)
    if (grants & subprivileges["DROP INDEX"]):
        with When("I check drop index when privilege is granted"):
            check_drop_index_when_privilege_is_granted(table, user, node)
    else:
        with When("I check drop index when privilege is not granted"):
            check_drop_index_when_privilege_is_not_granted(table, user, node)

def check_order_by_when_privilege_is_granted(table, user, node):
    """Ensures ORDER BY runs as expected when the privilege is granted to the specified user
    """
    column = "order"

    with Given("I run sanity check"):
        node.query(f"ALTER TABLE {table} MODIFY ORDER BY d", settings = [("user", user)])

    with And("I add new column and modify order using that column"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} UInt32, MODIFY ORDER BY (d, {column})")

    with When(f"I insert random data into the ordered-by column {column}"):
        data = random.sample(range(1,1000),100)
        values = ', '.join(f'({datum})' for datum in data)
        node.query(f"INSERT INTO {table}({column}) VALUES {values}")

    with Then("I synchronize with optimize table"):
        node.query(f"OPTIMIZE TABLE {table} final")

    with And("I verify that the added data is ordered in the table"):
        data.sort()
        note(data)
        column_data = node.query(f"SELECT {column} FROM {table} FORMAT JSONEachRow").output
        column_data = column_data.split('\n')
        for row, datum in zip(column_data[:10], data[:10]):
            assert json.loads(row) == {column:datum}, error()

    with And("I verify that the sorting key is present in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert f"ORDER BY (d, {column})" in output['statement'], error()

    with But(f"I cannot drop the required column {column}"):
        exitcode, message = errors.missing_columns(column)
        node.query(f"ALTER TABLE {table} DROP COLUMN {column}",
            exitcode=exitcode, message=message)

def check_sample_by_when_privilege_is_granted(table, user, node):
    """Ensures SAMPLE BY runs as expected when the privilege is granted to the specified user
    """
    column = 'sample'

    with Given(f"I add new column {column}"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} UInt32")

    with When(f"I add sample by clause"):
        node.query(f"ALTER TABLE {table} MODIFY SAMPLE BY (d, {column})",
            settings = [("user", user)])

    with Then("I verify that the sample is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert f"SAMPLE BY (d, {column})" in output['statement'], error()

    with But(f"I cannot drop the required column {column}"):
        exitcode, message = errors.missing_columns(column)
        node.query(f"ALTER TABLE {table} DROP COLUMN {column}",
            exitcode=exitcode, message=message)

def check_add_index_when_privilege_is_granted(table, user, node):
    """Ensures ADD INDEX runs as expected when the privilege is granted to the specified user
    """
    index = "add"

    with Given(f"I add index '{index}'"): # Column x: String
        node.query(f"ALTER TABLE {table} ADD INDEX {index}(x) TYPE set(0) GRANULARITY 1",
            settings = [("user", user)])

    with Then("I verify that the index is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert f"INDEX {index} x TYPE set(0) GRANULARITY 1" in output['statement'], error()

    with Finally(f"I drop index {index}"):
        node.query(f"ALTER TABLE {table} DROP INDEX {index}")

def check_materialize_index_when_privilege_is_granted(table, user, node):
    """Ensures MATERIALIZE INDEX runs as expected when the privilege is granted to the specified user
    """
    index = "materialize"

    with Given(f"I add index '{index}'"):
        node.query(f"ALTER TABLE {table} ADD INDEX {index}(x) TYPE set(0) GRANULARITY 1")

    with When(f"I materialize index '{index}'"):
        node.query(f"ALTER TABLE {table} MATERIALIZE INDEX {index} IN PARTITION 1 SETTINGS mutations_sync = 2",
            settings = [("user", user)])

    with Then("I verify that the index is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert f"INDEX {index} x TYPE set(0) GRANULARITY 1" in output['statement'], error()

    with Finally(f"I drop index {index}"):
        node.query(f"ALTER TABLE {table} DROP INDEX {index}")

def check_clear_index_when_privilege_is_granted(table, user, node):
    """Ensures CLEAR INDEX runs as expected when the privilege is granted to the specified user
    """
    index = "clear"

    with Given(f"I add index '{index}'"): # Column x: String
        node.query(f"ALTER TABLE {table} ADD INDEX {index}(x) TYPE set(0) GRANULARITY 1")

    with When(f"I clear index {index}"):
        node.query(f"ALTER TABLE {table} CLEAR INDEX {index} IN PARTITION 1")

    with Then("I verify that the index is in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert f"INDEX {index} x TYPE set(0) GRANULARITY 1" in output['statement'], error()

    with Finally(f"I drop index {index}"):
        node.query(f"ALTER TABLE {table} DROP INDEX {index}")

def check_drop_index_when_privilege_is_granted(table, user, node):
    """Ensures DROP INDEX runs as expected when the privilege is granted to the specified user
    """
    with When("I try to drop nonexistent index, throws exception"):
        exitcode, message = errors.wrong_index_name("fake_index")
        node.query(f"ALTER TABLE {table} DROP INDEX fake_index",
            settings = [("user", user)], exitcode=exitcode, message=message)

    index = "drop"

    with Given(f"I add the index"):
        node.query(f"ALTER TABLE {table} ADD INDEX {index}(x) TYPE set(0) GRANULARITY 1")

    with Then(f"I drop index {index} which exists"):
        node.query(f"ALTER TABLE {table} DROP INDEX {index}",
            settings = [("user", user)])

    with And("I verify that the index is not in the table"):
        output = json.loads(node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output)
        assert f"INDEX {index} x TYPE set(0) GRANULARITY 1" not in output['statement'], error()

def check_order_by_when_privilege_is_not_granted(table, user, node):
    """Ensures ORDER BY errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} MODIFY ORDER BY d",
                    settings = [("user", user)], exitcode=exitcode, message=message)

def check_sample_by_when_privilege_is_not_granted(table, user, node):
    """Ensures SAMPLE BY errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} MODIFY SAMPLE BY d",
                    settings = [("user", user)], exitcode=exitcode, message=message)

def check_add_index_when_privilege_is_not_granted(table, user, node):
    """Ensures ADD INDEX errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} ADD INDEX index1 b * length(x) TYPE set(1000) GRANULARITY 4",
                    settings = [("user", user)], exitcode=exitcode, message=message)

def check_materialize_index_when_privilege_is_not_granted(table, user, node):
    """Ensures MATERIALIZE INDEX errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} MATERIALIZE INDEX index1",
                    settings = [("user", user)], exitcode=exitcode, message=message)

def check_clear_index_when_privilege_is_not_granted(table, user, node):
    """Ensures CLEAR INDEX errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} CLEAR INDEX index1 IN PARTITION 1",
                    settings = [("user", user)], exitcode=exitcode, message=message)

def check_drop_index_when_privilege_is_not_granted(table, user, node):
    """Ensures DROP INDEX errors as expected without the required privilege for the specified user
    """
    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(f"ALTER TABLE {table} DROP INDEX index1",
                    settings = [("user", user)], exitcode=exitcode, message=message)

@TestScenario
@Flags(TE)
def user_with_some_privileges(self, table_type, node=None):
    """Check that user with any permutation of ALTER INDEX subprivileges is able
    to alter the table for privileges granted, and not for privileges not granted.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations(table_type):
        privileges = alter_index_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name):
                with Given("I first grant the privileges needed"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

                with Then(f"I try to ALTER INDEX with given privileges"):
                    alter_index_privilege_handler(permutation, table_name, user_name, node)

@TestScenario
@Flags(TE)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterIndex_Revoke("1.0"),
)
def user_with_revoked_privileges(self, table_type, node=None):
    """Check that user is unable to ALTER INDEX on table after ALTER INDEX privilege
    on that table has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations(table_type):
        privileges = alter_index_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name):
                with Given("I first grant the privileges"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

                with And("I then revoke the privileges"):
                    node.query(f"REVOKE {privileges} ON {table_name} FROM {user_name}")

                with When(f"I try to ALTER INDEX with given privileges"):
                    # Permutation 0: no privileges
                    alter_index_privilege_handler(0, table_name, user_name, node)

@TestScenario
@Flags(TE)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterIndex_Grant("1.0"),
)
def role_with_some_privileges(self, table_type, node=None):
    """Check that user can ALTER INDEX on a table after it is granted a role that
    has the ALTER INDEX privilege for that table.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    for permutation in permutations(table_type):
        privileges = alter_index_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name), role(node, role_name):
                with Given("I grant the ALTER INDEX privilege to a role"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

                with And("I grant role to the user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with Then(f"I try to ALTER INDEX with given privileges"):
                    alter_index_privilege_handler(permutation, table_name, user_name, node)

@TestScenario
@Flags(TE)
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has ALTER INDEX privilege on a table is unable to
    ALTER INDEX from that table after the role with privilege has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    for permutation in permutations(table_type):
        privileges = alter_index_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name), role(node, role_name):
                with When("I grant privileges to a role"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

                with And("I grant the role to a user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with And("I revoke the role from the user"):
                    node.query(f"REVOKE {role_name} FROM {user_name}")

                with And("I ALTER INDEX on the table"):
                    # Permutation 0: no privileges for any permutation on these columns
                    alter_index_privilege_handler(0, table_name, user_name, node)

@TestScenario
@Flags(TE)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterIndex_Cluster("1.0"),
)
def user_with_privileges_on_cluster(self, table_type, node=None):
    """Check that user is able to ALTER INDEX on a table with
    privilege granted on a cluster.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations(table_type):
        privileges = alter_index_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type):
                try:
                    with Given("I have a user on a cluster"):
                        node.query(f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster")

                    with When("I grant ALTER INDEX privileges needed for iteration on a cluster"):
                        node.query(f"GRANT ON CLUSTER sharded_cluster {privileges} ON {table_name} TO {user_name}")

                    with Then(f"I try to ALTER INDEX with given privileges"):
                        alter_index_privilege_handler(permutation, table_name, user_name, node)
                finally:
                    with Finally("I drop the user on cluster"):
                        node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterIndex("1.0"),
    RQ_SRS_006_RBAC_Privileges_AlterIndex_TableEngines("1.0")
)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Flags(TE)
@Name("alter index")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        table_type, = example

        if table_type != "MergeTree" and not self.context.stress:
            continue

        with Example(str(example)):
            pool = Pool(5)
            try:
                tasks = []
                try:
                    for scenario in loads(current_module(), Scenario):
                        run_scenario(pool, tasks, Scenario(test=scenario, setup=instrument_clickhouse_server_log), {"table_type" : table_type})
                finally:
                    join(tasks)
            finally:
                pool.close()
