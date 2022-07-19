import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors
from rbac.helper.tables import table_types

subprivileges = {
    "ADD CONSTRAINT": 1 << 0,
    "DROP CONSTRAINT": 1 << 1,
}

aliases = {
    "ADD CONSTRAINT": ["ALTER ADD CONSTRAINT", "ADD CONSTRAINT"],
    "DROP CONSTRAINT": ["ALTER DROP CONSTRAINT", "DROP CONSTRAINT"],
    "ALTER CONSTRAINT": ["ALTER CONSTRAINT", "CONSTRAINT", "ALL"],  # super-privilege
}

# Extra permutation is for 'ALTER CONSTRAINT' super-privilege
permutation_count = 1 << len(subprivileges)


def permutations():
    """Returns list of all permutations to run.
    Currently includes NONE, ADD, DROP, both, and ALTER CONSTRAINT
    """
    return [*range(permutation_count + len(aliases["ALTER CONSTRAINT"]))]


def alter_constraint_privileges(grants: int):
    """Takes in an integer, and returns the corresponding set of tests to grant and
    not grant using the binary string. Each integer corresponds to a unique permutation
    of grants.
    """
    note(grants)
    privileges = []

    # Extra iteration for ALTER CONSTRAINT
    if grants >= permutation_count:
        privileges.append(aliases["ALTER CONSTRAINT"][grants - permutation_count])
    elif grants == 0:  # No privileges
        privileges.append("NONE")
    else:
        if grants & subprivileges["ADD CONSTRAINT"]:
            privileges.append(
                aliases["ADD CONSTRAINT"][grants % len(aliases["ADD CONSTRAINT"])]
            )
        if grants & subprivileges["DROP CONSTRAINT"]:
            privileges.append(
                aliases["DROP CONSTRAINT"][grants % len(aliases["DROP CONSTRAINT"])]
            )

    note(f"Testing these privileges: {privileges}")
    return ", ".join(privileges)


def alter_constraint_privilege_handler(grants, table, user, node):
    """For all 2 subprivileges, if the privilege is granted: run test to ensure correct behavior,
    and if the privilege is not granted, run test to ensure correct behavior there as well
    """
    # Testing ALTER CONSTRAINT and CONSTRAINT is the same as testing all subprivileges
    if grants > permutation_count - 1:
        grants = permutation_count - 1

    if grants & subprivileges["ADD CONSTRAINT"]:
        with When("I check add constraint when privilege is granted"):
            check_add_constraint_when_privilege_is_granted(table, user, node)
    else:
        with When("I check add constraint when privilege is not granted"):
            check_add_constraint_when_privilege_is_not_granted(table, user, node)
    if grants & subprivileges["DROP CONSTRAINT"]:
        with When("I check drop constraint when privilege is granted"):
            check_drop_constraint_when_privilege_is_granted(table, user, node)
    else:
        with When("I check drop constraint when privilege is not granted"):
            check_drop_constraint_when_privilege_is_not_granted(table, user, node)


def check_add_constraint_when_privilege_is_granted(table, user, node):
    """Ensures ADD CONSTRAINT runs as expected when the privilege is granted to the specified user"""
    constraint = "add"

    with Given(f"I add constraint '{constraint}'"):
        node.query(
            f"ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK x>5",
            settings=[("user", user)],
        )

    with Then("I verify that the constraint is in the table"):
        output = json.loads(
            node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output
        )
        assert f"CONSTRAINT {constraint} CHECK x > 5" in output["statement"], error()

    with Finally(f"I drop constraint {constraint}"):
        node.query(f"ALTER TABLE {table} DROP constraint {constraint}")


def check_drop_constraint_when_privilege_is_granted(table, user, node):
    """Ensures DROP CONSTRAINT runs as expected when the privilege is granted to the specified user"""
    with But("I try to drop nonexistent constraint, throws exception"):
        exitcode, message = errors.wrong_constraint_name("fake_constraint")
        node.query(
            f"ALTER TABLE {table} DROP CONSTRAINT fake_constraint",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    constraint = "drop"

    with Given(f"I add the constraint for this test"):
        node.query(f"ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK x>5")

    with Then(f"I drop constraint {constraint} which exists"):
        node.query(
            f"ALTER TABLE {table} DROP CONSTRAINT {constraint}",
            settings=[("user", user)],
        )

    with Then("I verify that the constraint is not in the table"):
        output = json.loads(
            node.query(f"SHOW CREATE TABLE {table} FORMAT JSONEachRow").output
        )
        assert (
            f"CONSTRAINT {constraint} CHECK x > 5" not in output["statement"]
        ), error()


def check_add_constraint_when_privilege_is_not_granted(table, user, node):
    """Ensures ADD CONSTRAINT errors as expected without the required privilege for the specified user"""
    constraint = "add"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK x>5",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_drop_constraint_when_privilege_is_not_granted(table, user, node):
    """Ensures DROP CONSTRAINT errors as expected without the required privilege for the specified user"""
    constraint = "drop"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} DROP CONSTRAINT {constraint}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


@TestScenario
def user_with_some_privileges(self, table_type, node=None):
    """Check that user with any permutation of ALTER CONSTRAINT subprivileges is able
    to alter the table for privileges granted, and not for privileges not granted.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations():
        privileges = alter_constraint_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name):
                with Given("I first grant the privileges"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

                with Then(f"I try to ALTER CONSTRAINT"):
                    alter_constraint_privilege_handler(
                        permutation, table_name, user_name, node
                    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterConstraint_Revoke("1.0"),
)
def user_with_revoked_privileges(self, table_type, node=None):
    """Check that user is unable to ALTER CONSTRAINTs on table after ALTER CONSTRAINT privilege
    on that table has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations():
        privileges = alter_constraint_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name):
                with Given("I first grant the privileges"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

                with And("I then revoke the privileges"):
                    node.query(f"REVOKE {privileges} ON {table_name} FROM {user_name}")

                with When(f"I try to ALTER CONSTRAINT"):
                    # Permutation 0: no privileges
                    alter_constraint_privilege_handler(0, table_name, user_name, node)


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterConstraint_Grant("1.0"),
)
def role_with_some_privileges(self, table_type, node=None):
    """Check that user can ALTER CONSTRAINT on a table after it is granted a role that
    has the ALTER CONSTRAINT privilege for that table.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    for permutation in permutations():
        privileges = alter_constraint_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name), role(
                node, role_name
            ):
                with Given("I grant the ALTER CONSTRAINT privilege to a role"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

                with And("I grant role to the user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with Then(f"I try to ALTER CONSTRAINT"):
                    alter_constraint_privilege_handler(
                        permutation, table_name, user_name, node
                    )


@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has ALTER CONSTRAINT privilege on a table is unable to
    ALTER CONSTRAINT from that table after the role with privilege has been revoked from the user.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    for permutation in permutations():
        privileges = alter_constraint_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type), user(node, user_name), role(
                node, role_name
            ):
                with When("I grant privileges to a role"):
                    node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

                with And("I grant the role to a user"):
                    node.query(f"GRANT {role_name} TO {user_name}")

                with And("I revoke the role from the user"):
                    node.query(f"REVOKE {role_name} FROM {user_name}")

                with And("I ALTER CONSTRAINT on the table"):
                    # Permutation 0: no privileges for any permutation
                    alter_constraint_privilege_handler(0, table_name, user_name, node)


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterConstraint_Cluster("1.0"),
)
def user_with_privileges_on_cluster(self, table_type, node=None):
    """Check that user is able to ALTER CONSTRAINT on a table with
    privilege granted on a cluster.
    """
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    for permutation in permutations():
        privileges = alter_constraint_privileges(permutation)

        with When(f"granted={privileges}"):
            with table(node, table_name, table_type):
                try:
                    with Given("I have a user on a cluster"):
                        node.query(
                            f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster"
                        )

                    with When("I grant ALTER CONSTRAINT privileges on a cluster"):
                        node.query(
                            f"GRANT ON CLUSTER sharded_cluster {privileges} ON {table_name} TO {user_name}"
                        )

                    with Then(f"I try to ALTER CONSTRAINT"):
                        alter_constraint_privilege_handler(
                            permutation, table_name, user_name, node
                        )
                finally:
                    with Finally("I drop the user on a cluster"):
                        node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")


@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterConstraint("1.0"),
    RQ_SRS_006_RBAC_Privileges_AlterConstraint_TableEngines("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
@Examples("table_type", [(key,) for key in table_types.keys()])
@Name("alter constraint")
def feature(self, stress=None, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        (table_type,) = example

        if table_type != "MergeTree" and not self.context.stress:
            continue

        args = {"table_type": table_type}

        with Example(str(example)):
            with Pool(5) as pool:
                try:
                    for scenario in loads(current_module(), Scenario):
                        Scenario(
                            test=scenario,
                            setup=instrument_clickhouse_server_log,
                            parallel=True,
                            executor=pool,
                        )(**args)
                finally:
                    join()
