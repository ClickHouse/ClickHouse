import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors
from rbac.helper.tables import table_types

subprivileges = {
    "ADD COLUMN": 1 << 0,
    "CLEAR COLUMN": 1 << 1,
    "MODIFY COLUMN": 1 << 2,
    "RENAME COLUMN": 1 << 3,
    "COMMENT COLUMN": 1 << 4,
    "DROP COLUMN": 1 << 5,
}

aliases = {
    "ADD COLUMN": ["ALTER ADD COLUMN", "ADD COLUMN"],
    "CLEAR COLUMN": ["ALTER CLEAR COLUMN", "CLEAR COLUMN"],
    "MODIFY COLUMN": ["ALTER MODIFY COLUMN", "MODIFY COLUMN"],
    "RENAME COLUMN": ["ALTER RENAME COLUMN", "RENAME COLUMN"],
    "COMMENT COLUMN": ["ALTER COMMENT COLUMN", "COMMENT COLUMN"],
    "DROP COLUMN": ["ALTER DROP COLUMN", "DROP COLUMN"],
    "ALTER COLUMN": ["ALTER COLUMN", "ALL"],  # super-privilege
}

# extra permutation is for 'ALTER COLUMN' super-privilege
permutation_count = 1 << len(subprivileges)


def permutations(table_type):
    """Uses stress flag and table type, returns list of all permutations to run

    Stress test (stress=True): all permutations for all tables
    """
    if current().context.stress:
        return [*range(permutation_count + len(aliases["ALTER COLUMN"]))]
    else:
        # Selected permutations currently stand as [1,2,4,8,16,32,0,42,63,64] that maps to
        # testing
        # [
        #   "ADD COLUMN", "CLEAR COLUMN", "MODIFY COLUMN", "RENAME COLUMN",
        #   "COMMENT COLUMN", "DROP COLUMN", "NONE", "DROP, RENAME, CLEAR", all, and
        #   "ALTER COLUMN"
        # ]
        return [1 << index for index in range(len(subprivileges))] + [
            0,
            int("101010", 2),
            permutation_count - 1,
            permutation_count,
        ]


def alter_column_privileges(grants: int):
    """Takes in an integer, and returns the corresponding set of tests to grant and
    not grant using the binary string. Each integer corresponds to a unique permutation
    of grants.
    Columns represents columns to grant privileges on, with format "col1,col2,col3"
    """
    note(grants)
    privileges = []

    # extra iteration for ALTER COLUMN
    if grants >= permutation_count:
        privileges.append(aliases["ALTER COLUMN"][grants - permutation_count])
    elif grants == 0:  # No privileges
        privileges.append("NONE")
    else:
        if grants & subprivileges["ADD COLUMN"]:
            privileges.append(
                aliases["ADD COLUMN"][grants % len(aliases["ADD COLUMN"])]
            )
        if grants & subprivileges["CLEAR COLUMN"]:
            privileges.append(
                aliases["CLEAR COLUMN"][grants % len(aliases["CLEAR COLUMN"])]
            )
        if grants & subprivileges["MODIFY COLUMN"]:
            privileges.append(
                aliases["MODIFY COLUMN"][grants % len(aliases["MODIFY COLUMN"])]
            )
        if grants & subprivileges["RENAME COLUMN"]:
            privileges.append(
                aliases["RENAME COLUMN"][grants % len(aliases["RENAME COLUMN"])]
            )
        if grants & subprivileges["COMMENT COLUMN"]:
            privileges.append(
                aliases["COMMENT COLUMN"][grants % len(aliases["COMMENT COLUMN"])]
            )
        if grants & subprivileges["DROP COLUMN"]:
            privileges.append(
                aliases["DROP COLUMN"][grants % len(aliases["DROP COLUMN"])]
            )

    note(f"Testing privileges: {privileges}")
    return ", ".join(privileges)


def on_columns(privileges, columns):
    """For column-based tests. Takes in string output of alter_column_privileges()
    and adds columns for those privileges.
    """
    privileges = privileges.split(",")
    privileges = [privilege + f"({columns})" for privilege in privileges]
    return ", ".join(privileges)


def alter_column_privilege_handler(grants, table, user, node, columns=None):
    """For all 6 subprivileges, if the privilege is granted: run test to ensure correct behavior,
    and if the privilege is not granted, run test to ensure correct behavior there as well

    If `columns` are passed in, they must be columns that do not exist on the table.
    This is necessary for full testing (add column, drop column, modify column, etc.).
    """
    note(f"GRANTS: {grants}")

    # testing ALTER COLUMN is the same as testing all subprivileges
    if grants > permutation_count - 1:
        grants = permutation_count - 1

    # if 'columns' is not passed then one iteration with column = None
    columns = columns.split(",") if columns != None else [None]

    for column in columns:
        # will always run 6 tests per column depending on granted privileges
        if grants & subprivileges["ADD COLUMN"]:
            with When("I check add column when privilege is granted"):
                check_add_column_when_privilege_is_granted(table, user, node, column)
        else:
            with When("I check add column when privilege is not granted"):
                check_add_column_when_privilege_is_not_granted(
                    table, user, node, column
                )
        if grants & subprivileges["CLEAR COLUMN"]:
            with When("I check clear column when privilege is granted"):
                check_clear_column_when_privilege_is_granted(table, user, node, column)
        else:
            with When("I check clear column when privilege is not granted"):
                check_clear_column_when_privilege_is_not_granted(
                    table, user, node, column
                )
        if grants & subprivileges["MODIFY COLUMN"]:
            with When("I check modify column when privilege is granted"):
                check_modify_column_when_privilege_is_granted(table, user, node, column)
        else:
            with When("I check modify column when privilege is not granted"):
                check_modify_column_when_privilege_is_not_granted(
                    table, user, node, column
                )
        if grants & subprivileges["RENAME COLUMN"]:
            with When("I check rename column when privilege is granted"):
                check_rename_column_when_privilege_is_granted(table, user, node, column)
        else:
            with When("I check rename column when privilege is not granted"):
                check_rename_column_when_privilege_is_not_granted(
                    table, user, node, column
                )
        if grants & subprivileges["COMMENT COLUMN"]:
            with When("I check comment column when privilege is granted"):
                check_comment_column_when_privilege_is_granted(
                    table, user, node, column
                )
        else:
            with When("I check comment column when privilege is not granted"):
                check_comment_column_when_privilege_is_not_granted(
                    table, user, node, column
                )
        if grants & subprivileges["DROP COLUMN"]:
            with When("I check drop column when privilege is granted"):
                check_drop_column_when_privilege_is_granted(table, user, node, column)
        else:
            with When("I check drop column when privilege is not granted"):
                check_drop_column_when_privilege_is_not_granted(
                    table, user, node, column
                )


def check_add_column_when_privilege_is_granted(table, user, node, column=None):
    """Ensures ADD COLUMN runs as expected when the privilege is granted
    to the specified user.
    """
    if column is None:
        column = "add"

    with Given(f"I add column '{column}'"):
        node.query(
            f"ALTER TABLE {table} ADD COLUMN {column} String", settings=[("user", user)]
        )

    with Then("I insert data to tree"):
        node.query(f"INSERT INTO {table} ({column}) VALUES ('3.4')")  # String

    with Then("I verify that the column was successfully added"):
        column_data = node.query(
            f"SELECT {column} FROM {table} FORMAT JSONEachRow"
        ).output
        column_data_list = column_data.split("\n")
        output_rows = [{f"{column}": "3.4"}, {f"{column}": ""}]

        for row in column_data_list:
            assert json.loads(row) in output_rows, error()

    with Finally(f"I drop column '{column}'"):
        node.query(f"ALTER TABLE {table} DROP COLUMN {column}")


def check_clear_column_when_privilege_is_granted(table, user, node, column=None):
    """Ensures CLEAR COLUMN runs as expected when the privilege is granted
    to the specified user.
    """
    if column is None:
        column = "clear"

    with Given(f"I add the column {column}"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} String")

    with And("I add some data to column"):
        node.query(f"INSERT INTO {table} ({column}) VALUES ('ready to be cleared')")

    with When(f"I clear column '{column}'"):
        node.query(
            f"ALTER TABLE {table} CLEAR COLUMN {column}", settings=[("user", user)]
        )

    with Then("I verify that the column was successfully cleared"):
        column_data = node.query(
            f"SELECT {column} FROM {table} FORMAT JSONEachRow"
        ).output
        column_data_list = column_data.split("\n")

        for row in column_data_list:
            assert json.loads(row) == {f"{column}": ""}, error()

    with Finally(f"I drop column '{column}'"):
        node.query(f"ALTER TABLE {table} DROP COLUMN {column}")


def check_modify_column_when_privilege_is_granted(table, user, node, column=None):
    """Ensures MODIFY COLUMN runs as expected when the privilege is granted
    to the specified user.
    """
    if column is None:
        column = "modify"

    with Given(f"I add the column {column}"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} String DEFAULT '0'")

    with When(f"I insert some data into column {column}"):
        node.query(f"INSERT INTO {table} ({column}) VALUES ('3.4')")

    with When(f"I modify column '{column}' to type Float"):
        node.query(
            f"ALTER TABLE {table} MODIFY COLUMN {column} Float64",
            settings=[("user", user)],
        )

    with And("I run optimize table to ensure above UPDATE command is done"):
        node.query(f"OPTIMIZE TABLE {table} FINAL", timeout=900)

    with Then("I verify that the column type was modified"):
        with When(
            f"I try to insert a String (old type) to column {column}, throws exception"
        ):
            exitcode, message = errors.cannot_parse_string_as_float("hello")
            node.query(
                f"INSERT INTO {table} ({column}) VALUES ('hello')",
                exitcode=exitcode,
                message=message,
            )

        with And(
            f"I try to insert float data (correct type) to column {column}, will accept"
        ):
            node.query(f"INSERT INTO {table} ({column}) VALUES (30.01)")

        with And("I verify that the date was inserted correctly"):
            column_data = node.query(
                f"SELECT {column} FROM {table} FORMAT JSONEachRow"
            ).output
            column_data_list = column_data.split("\n")
            output_rows = [{f"{column}": 30.01}, {f"{column}": 3.4}, {f"{column}": 0}]

            for row in column_data_list:
                assert json.loads(row) in output_rows, error()

    with Finally(f"I drop column '{column}'"):
        node.query(f"ALTER TABLE {table} DROP COLUMN {column}")


def check_rename_column_when_privilege_is_granted(table, user, node, column=None):
    """Ensures RENAME COLUMN runs as expected when the privilege is granted
    to the specified user.
    """
    if column is None:
        column = "rename"

    new_column = f"{column}_new"

    with Given(f"I add the column {column}"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} String")

    with And("I get the initial contents of the column"):
        # could be either str or float depending on MODIFY COLUMN
        initial_column_data = node.query(
            f"SELECT {column} FROM {table} ORDER BY {column}" " FORMAT JSONEachRow"
        ).output

    with When(f"I rename column '{column}' to '{new_column}'"):
        node.query(
            f"ALTER TABLE {table} RENAME COLUMN {column} TO {new_column}",
            settings=[("user", user)],
        )

    with Then("I verify that the column was successfully renamed"):
        with When("I verify that the original column does not exist"):
            exitcode, message = errors.missing_columns(column)
            node.query(
                f"SELECT {column} FROM {table} FORMAT JSONEachRow",
                exitcode=exitcode,
                message=message,
            )

        with And(
            "I verify that the new column does exist as expected, with same values"
        ):
            new_column_data = node.query(
                f"SELECT {new_column} FROM {table} ORDER BY"
                f" {new_column} FORMAT JSONEachRow"
            ).output

            if initial_column_data == "":
                assert initial_column_data == new_column_data, error()
            else:
                new_column_data_list = new_column_data.split("\n")
                initial_column_data_list = initial_column_data.split("\n")

                for new, initial in zip(new_column_data_list, initial_column_data_list):
                    assert (
                        json.loads(new)[new_column] == json.loads(initial)[column]
                    ), error()

    with Finally(f"I use default user to undo rename"):
        node.query(f"ALTER TABLE {table} RENAME COLUMN {new_column} TO {column}")

    with Finally(f"I drop column '{column}'"):
        node.query(f"ALTER TABLE {table} DROP COLUMN {column}")


def check_comment_column_when_privilege_is_granted(table, user, node, column="x"):
    """Ensures COMMENT COLUMN runs as expected when the privilege is granted
    to the specified user.
    """
    if column is None:
        column = "comment"

    with Given(f"I add the column {column}"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} String")

    with And(f"I alter {column} with comment"):
        node.query(
            f"ALTER TABLE {table} COMMENT COLUMN {column} 'This is a comment.'",
            settings=[("user", user)],
        )

    with Then(f"I verify that the specified comment is present for {column}"):
        table_data = node.query(f"DESCRIBE TABLE {table} FORMAT JSONEachRow").output
        table_data_list = table_data.split("\n")

        for row in table_data_list:
            row = json.loads(row)
            if row["name"] == column:
                assert row["comment"] == "This is a comment.", error()

                with Finally(f"I drop column '{column}'"):
                    node.query(f"ALTER TABLE {table} DROP COLUMN {column}")
                return

        # did not find a match, so cleanup column and throw an error
        with Finally(f"I drop column '{column}'"):
            node.query(f"ALTER TABLE {table} DROP COLUMN {column}")

        error()


def check_drop_column_when_privilege_is_granted(table, user, node, column=None):
    """Ensures DROP COLUMN runs as expected when the privilege is granted
    to the specified user.
    """
    with When("I try to drop nonexistent column, throws exception"):
        # if user has privilege for all columns, error is 'wrong column name'
        if column:
            exitcode, message = errors.not_enough_privileges(user)
        else:
            exitcode, message = errors.wrong_column_name("fake_column")

        node.query(
            f"ALTER TABLE {table} DROP COLUMN fake_column",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    if column is None:
        column = "drop"

    with Given(f"I add the column {column}"):
        node.query(f"ALTER TABLE {table} ADD COLUMN {column} String")

    with Then(f"I drop column {column} which exists"):
        node.query(
            f"ALTER TABLE {table} DROP COLUMN {column}", settings=[("user", user)]
        )

    with And(f"I verify that {column} has been dropped"):
        exitcode, message = errors.wrong_column_name(column)
        node.query(
            f"ALTER TABLE {table} DROP COLUMN {column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_add_column_when_privilege_is_not_granted(table, user, node, column=None):
    """Ensures ADD COLUMN errors as expected without the required privilege
    for the specified user.
    """
    if column is None:
        column = "add"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} ADD COLUMN {column} String",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    with Then("I try to ADD COLUMN"):
        node.query(
            f"ALTER TABLE {table} ADD COLUMN {column} String",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_clear_column_when_privilege_is_not_granted(table, user, node, column=None):
    """Ensures CLEAR COLUMN errors as expected without the required privilege
    for the specified user.
    """
    if column is None:
        column = "clear"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} CLEAR COLUMN {column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    with And(f"I grant NONE to the user"):
        node.query(f"GRANT NONE TO {user}")

    with Then("I try to CLEAR COLUMN"):
        node.query(
            f"ALTER TABLE {table} CLEAR COLUMN {column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_modify_column_when_privilege_is_not_granted(table, user, node, column=None):
    """Ensures MODIFY COLUMN errors as expected without the required privilege
    for the specified user.
    """
    if column is None:
        column = "modify"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} MODIFY COLUMN {column} String",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    with And(f"I grant NONE to the user"):
        node.query(f"GRANT NONE TO {user}")

    with Then("I try to MODIFY COLUMN"):
        node.query(
            f"ALTER TABLE {table} MODIFY COLUMN {column} String",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_rename_column_when_privilege_is_not_granted(table, user, node, column=None):
    """Ensures RENAME COLUMN errors as expected without the required privilege
    for the specified user.
    """
    if column is None:
        column = "rename"

    new_column = f"{column}_new"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} RENAME COLUMN {column} TO {new_column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    with And(f"I grant NONE to the user"):
        node.query(f"GRANT NONE TO {user}")

    with Then("I try to RENAME COLUMN"):
        node.query(
            f"ALTER TABLE {table} RENAME COLUMN {column} TO {new_column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_comment_column_when_privilege_is_not_granted(table, user, node, column=None):
    """Ensures COMMENT COLUMN errors as expected without the required privilege
    for the specified user.
    """
    if column is None:
        column = "comment"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} COMMENT COLUMN {column} 'This is a comment.'",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    with And(f"I grant NONE to the user"):
        node.query(f"GRANT NONE TO {user}")

    with When("I try to COMMENT COLUMN"):
        node.query(
            f"ALTER TABLE {table} COMMENT COLUMN {column} 'This is a comment.'",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


def check_drop_column_when_privilege_is_not_granted(table, user, node, column=None):
    """Ensures DROP COLUMN errors as expected without the required privilege
    for the specified user.
    """
    if column is None:
        column = "drop"

    with When("I try to use privilege that has not been granted"):
        exitcode, message = errors.not_enough_privileges(user)
        node.query(
            f"ALTER TABLE {table} DROP COLUMN {column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )

    with And(f"I grant NONE to the user"):
        node.query(f"GRANT NONE TO {user}")

    with Then("I try to DROP COLUMN"):
        node.query(
            f"ALTER TABLE {table} DROP COLUMN {column}",
            settings=[("user", user)],
            exitcode=exitcode,
            message=message,
        )


@TestScenario
def user_with_some_privileges(self, permutation, table_type, node=None):
    """Check that user with some privileges of ALTER COLUMN is able
    to alter the table for privileges granted, and not for privileges not granted.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    with When(f"granted={privileges}"):
        with table(node, table_name, table_type), user(node, user_name):
            with Given("I first grant the privileges"):
                node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

            with Then(f"I try to ALTER COLUMN"):
                alter_column_privilege_handler(permutation, table_name, user_name, node)


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterColumn_Revoke("1.0"),
)
def user_with_revoked_privileges(self, permutation, table_type, node=None):
    """Check that user is unable to alter columns on table after ALTER COLUMN privilege
    on that table has been revoked from the user.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    with When(f"granted={privileges}"):
        with table(node, table_name, table_type), user(node, user_name):
            with Given("I first grant the privileges"):
                node.query(f"GRANT {privileges} ON {table_name} TO {user_name}")

            with And("I then revoke the privileges"):
                node.query(f"REVOKE {privileges} ON {table_name} FROM {user_name}")

            with When(f"I try to ALTER COLUMN"):
                # No privileges granted
                alter_column_privilege_handler(0, table_name, user_name, node)


@TestScenario
@Examples(
    "grant_columns revoke_columns alter_columns_fail",
    [
        ("t1", "t1", "t2"),
        ("t1,t3", "t1", "t2"),
        ("t1,t3,t4", "t1,t3,t4", "t2"),
    ],
)
def user_with_privileges_on_columns(self, table_type, permutation, node=None):
    """Passes in examples to user_column_privileges() below to test granting
    of sub-privileges on columns
    """
    examples = Examples(
        "grant_columns revoke_columns alter_columns_fail table_type permutation",
        [tuple(list(row) + [table_type, permutation]) for row in self.examples],
    )

    Scenario(test=user_column_privileges, examples=examples)()


@TestOutline
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterColumn_Column("1.0"),
)
def user_column_privileges(
    self,
    grant_columns,
    revoke_columns,
    alter_columns_fail,
    table_type,
    permutation,
    node=None,
):
    """Check that user is able to alter on granted columns
    and unable to alter on not granted or revoked columns.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    privileges_on_columns = on_columns(privileges, grant_columns)

    with When(f"granted={privileges_on_columns}"):
        with table(node, table_name, table_type), user(node, user_name):
            with When(f"I grant subprivileges"):
                node.query(
                    f"GRANT {privileges_on_columns} ON {table_name} TO {user_name}"
                )

            if alter_columns_fail is not None:
                with When(f"I try to alter on not granted columns, fails"):
                    # Permutation 0: no privileges for any permutation on these columns
                    alter_column_privilege_handler(
                        0, table_name, user_name, node, columns=alter_columns_fail
                    )

            with Then(f"I try to ALTER COLUMN"):
                alter_column_privilege_handler(
                    permutation, table_name, user_name, node, columns=grant_columns
                )

            if revoke_columns is not None:
                with When(f"I revoke alter column privilege for columns"):
                    node.query(
                        f"REVOKE {privileges_on_columns} ON {table_name} FROM {user_name}"
                    )

                with And("I try to alter revoked columns"):
                    alter_column_privilege_handler(
                        0, table_name, user_name, node, columns=alter_columns_fail
                    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterColumn_Grant("1.0"),
)
def role_with_some_privileges(self, permutation, table_type, node=None):
    """Check that user can alter column on a table after it is granted a role that
    has the alter column privilege for that table.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    with When(f"granted={privileges}"):
        with table(node, table_name, table_type), user(node, user_name), role(
            node, role_name
        ):
            with Given("I grant the alter column privilege to a role"):
                node.query(f"GRANT {privileges} ON {table_name} TO {role_name}")

            with And("I grant role to the user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            with Then(f"I try to ALTER COLUMN"):
                alter_column_privilege_handler(permutation, table_name, user_name, node)


@TestScenario
def user_with_revoked_role(self, permutation, table_type, node=None):
    """Check that user with a role that has alter column privilege on a table is unable to
    alter column from that table after the role with privilege has been revoked from the user.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

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

            with And("I alter column on the table"):
                # Permutation 0: no privileges for any permutation on these columns
                alter_column_privilege_handler(0, table_name, user_name, node)


@TestScenario
@Examples(
    "grant_columns revoke_columns alter_columns_fail",
    [
        ("t1", "t1", "t2"),
        ("t1,t3", "t1", "t2"),
        ("t1,t3,t4", "t1,t3,t4", "t2"),
    ],
)
def role_with_privileges_on_columns(self, table_type, permutation, node=None):
    """Passes in examples to role_column_privileges() below to test granting
    of subprivileges on columns
    """
    examples = Examples(
        "grant_columns revoke_columns alter_columns_fail table_type permutation",
        [tuple(list(row) + [table_type, permutation]) for row in self.examples],
    )

    Scenario(test=user_column_privileges, examples=examples)()


@TestOutline
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterColumn_Column("1.0"),
)
def role_column_privileges(
    self,
    grant_columns,
    revoke_columns,
    alter_columns_fail,
    table_type,
    permutation,
    node=None,
):
    """Check that user is able to alter column from granted columns and unable
    to alter column from not granted or revoked columns.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    privileges_on_columns = on_columns(privileges, grant_columns)

    with When(f"granted={privileges_on_columns}"):
        with table(node, table_name, table_type), user(node, user_name), role(
            node, role_name
        ):
            with When(f"I grant subprivileges"):
                node.query(
                    f"GRANT {privileges_on_columns} ON {table_name} TO {role_name}"
                )

            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")

            if alter_columns_fail is not None:
                with When(f"I try to alter on not granted columns, fails"):
                    # Permutation 0: no privileges for any permutation on these columns
                    alter_column_privilege_handler(
                        0, table_name, user_name, node, columns=alter_columns_fail
                    )

            with Then(f"I try to ALTER COLUMN"):
                alter_column_privilege_handler(
                    permutation, table_name, user_name, node, columns=grant_columns
                )

            if revoke_columns is not None:
                with When(f"I revoke alter column privilege for columns"):
                    node.query(
                        f"REVOKE {privileges_on_columns} ON {table_name} FROM {role_name}"
                    )

                with And("I try to alter failed columns"):
                    alter_column_privilege_handler(
                        0, table_name, user_name, node, columns=revoke_columns
                    )


@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterColumn_Cluster("1.0"),
)
def user_with_privileges_on_cluster(self, permutation, table_type, node=None):
    """Check that user is able to alter column on a table with
    privilege granted on a cluster.
    """
    privileges = alter_column_privileges(permutation)
    if node is None:
        node = self.context.node

    table_name = f"merge_tree_{getuid()}"
    user_name = f"user_{getuid()}"

    with When(f"granted={privileges}"):
        with table(node, table_name, table_type):
            try:
                with Given("I have a user on a cluster"):
                    node.query(
                        f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster"
                    )

                with When("I grant alter column privileges on a cluster"):
                    node.query(
                        f"GRANT ON CLUSTER sharded_cluster {privileges} ON {table_name} TO {user_name}"
                    )

                with Then(f"I try to ALTER COLUMN"):
                    alter_column_privilege_handler(
                        permutation, table_name, user_name, node
                    )
            finally:
                with Finally("I drop the user on a cluster"):
                    node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")


@TestSuite
def scenario_parallelization(self, table_type, permutation):
    args = {"table_type": table_type, "permutation": permutation}
    with Pool(7) as pool:
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


@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_AlterColumn("1.0"),
    RQ_SRS_006_RBAC_Privileges_AlterColumn_TableEngines("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0"),
)
@Examples("table_type", [(key,) for key in table_types.keys()])
@Name("alter column")
def feature(self, stress=None, node="clickhouse1"):
    """Runs test suites above which check correctness over scenarios and permutations."""
    self.context.node = self.context.cluster.node(node)

    if stress is not None:
        self.context.stress = stress

    for example in self.examples:
        (table_type,) = example

        if table_type != "MergeTree" and not self.context.stress:
            continue

        with Example(str(example)):
            with Pool(10) as pool:
                try:
                    for permutation in permutations(table_type):
                        privileges = alter_column_privileges(permutation)
                        args = {"table_type": table_type, "permutation": permutation}
                        Suite(
                            test=scenario_parallelization,
                            name=privileges,
                            parallel=True,
                            executor=pool,
                        )(**args)
                finally:
                    join()
