from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def dict_privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `SHOW CREATE` and `EXISTS`
    commands on a dictionary and see the dictionary when they execute `SHOW DICTIONARIES` command
    if and only if they have any privilege on that table granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        dict_name = f"dict_name_{getuid()}"

        Suite(run=check_privilege, flags=TE,
            examples=Examples("privilege on grant_target_name user_name dict_name", [
                tuple(list(row)+[user_name,user_name,dict_name]) for row in check_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def dict_privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `SHOW CREATE` and `EXISTS`
    commands on a dictionary and see the dictionary when they execute `SHOW DICTIONARIES` command
    if and only if they have any privilege on that table granted via role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        dict_name = f"dict_name_{getuid()}"

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=check_privilege, flags=TE,
            examples=Examples("privilege on grant_target_name user_name dict_name", [
                tuple(list(row)+[role_name,user_name,dict_name]) for row in check_privilege.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("SHOW","*.*"),
    ("SHOW DICTIONARIES","dict"),
    ("CREATE DICTIONARY","dict"),
    ("DROP DICTIONARY","dict"),
])
def check_privilege(self, privilege, on, grant_target_name, user_name, dict_name, node=None):
    """Run checks for commands that require SHOW DICTIONARY privilege.
    """

    if node is None:
        node = self.context.node

    on = on.replace("dict", f"{dict_name}")

    Suite(test=show_dict, setup=instrument_clickhouse_server_log)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, dict_name=dict_name)
    Suite(test=exists, setup=instrument_clickhouse_server_log)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, dict_name=dict_name)
    Suite(test=show_create, setup=instrument_clickhouse_server_log)(privilege=privilege, on=on, grant_target_name=grant_target_name, user_name=user_name, dict_name=dict_name)

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowDictionaries_Query("1.0"),
)
def show_dict(self, privilege, on, grant_target_name, user_name, dict_name, node=None):
    """Check that user is only able to see a dictionary in SHOW DICTIONARIES
    when they have a privilege on that dictionary.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    try:
        with Given("I have a dictionary"):
            node.query(f"CREATE DICTIONARY {dict_name}(x Int32, y Int32) PRIMARY KEY x LAYOUT(FLAT()) SOURCE(CLICKHOUSE()) LIFETIME(0)")

        with Scenario("SHOW DICTIONARIES without privilege"):
            with When("I check the user doesn't see the dictionary"):
                output = node.query("SHOW DICTIONARIES", settings = [("user", f"{user_name}")]).output
                assert output == '', error()

        with Scenario("SHOW DICTIONARIES with privilege"):
            with When(f"I grant {privilege} on the dictionary"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I check the user does see a dictionary"):
                node.query("SHOW DICTIONARIES", settings = [("user", f"{user_name}")], message=f"{dict_name}")

        with Scenario("SHOW DICTIONARIES with revoked privilege"):
            with When(f"I grant {privilege} on the dictionary"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the dictionary"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then("I check the user does not see a dictionary"):
                output = node.query("SHOW DICTIONARIES", settings = [("user", f"{user_name}")]).output
                assert output == f'', error()

    finally:
        with Finally("I drop the dictionary"):
            node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ExistsDictionary("1.0"),
)
def exists(self, privilege, on, grant_target_name, user_name, dict_name, node=None):
    """Check that user is able to execute EXISTS on a dictionary if and only if the user has SHOW DICTIONARY privilege
    on that dictionary.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    try:
        with Given("I have a dictionary"):
            node.query(f"CREATE DICTIONARY {dict_name}(x Int32, y Int32) PRIMARY KEY x LAYOUT(FLAT()) SOURCE(CLICKHOUSE()) LIFETIME(0)")

        with Scenario("EXISTS without privilege"):
            with When(f"I check if {dict_name} EXISTS"):
                node.query(f"EXISTS {dict_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        with Scenario("EXISTS with privilege"):
            with When(f"I grant {privilege} on the dictionary"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then(f"I check if {dict_name} EXISTS"):
                node.query(f"EXISTS {dict_name}", settings=[("user",user_name)])

        with Scenario("EXISTS with revoked privilege"):
            with When(f"I grant {privilege} on the dictionary"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the dictionary"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then(f"I check if {dict_name} EXISTS"):
                node.query(f"EXISTS {dict_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

    finally:
        with Finally("I drop the dictionary"):
            node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowCreateDictionary("1.0"),
)
def show_create(self, privilege, on, grant_target_name, user_name, dict_name, node=None):
    """Check that user is able to execute SHOW CREATE on a dictionary if and only if the user has SHOW DICTIONARY privilege
    on that dictionary.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    try:
        with Given("I have a dictionary"):
            node.query(f"CREATE DICTIONARY {dict_name}(x Int32, y Int32) PRIMARY KEY x LAYOUT(FLAT()) SOURCE(CLICKHOUSE()) LIFETIME(0)")

        with Scenario("SHOW CREATE without privilege"):
            with When(f"I attempt to SHOW CREATE {dict_name}"):
                node.query(f"SHOW CREATE DICTIONARY {dict_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        with Scenario("SHOW CREATE with privilege"):
            with When(f"I grant {privilege} on the dictionary"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then(f"I attempt to SHOW CREATE {dict_name}"):
                node.query(f"SHOW CREATE DICTIONARY {dict_name}", settings=[("user",user_name)])

        with Scenario("SHOW CREATE with revoked privilege"):
            with When(f"I grant {privilege} on the dictionary"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And(f"I revoke {privilege} on the dictionary"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with Then(f"I attempt to SHOW CREATE {dict_name}"):
                node.query(f"SHOW CREATE DICTIONARY {dict_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

    finally:
        with Finally("I drop the dictionary"):
            node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

@TestFeature
@Name("show dictionaries")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_ShowDictionaries("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SHOW DICTIONARIES.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=dict_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=dict_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
