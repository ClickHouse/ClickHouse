import os

from contextlib import contextmanager

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@contextmanager
def dict_setup(node, table_name, dict_name, type="UInt64"):
    """Setup and teardown of table and dictionary needed for the tests.
    """

    try:
        with Given("I have a table"):
            node.query(f"CREATE TABLE {table_name} (x UInt64, y UInt64, z {type}) ENGINE = Memory")

        with And("I have a dictionary"):
            node.query(f"CREATE DICTIONARY {dict_name} (x UInt64 HIERARCHICAL IS_OBJECT_ID, y UInt64 HIERARCHICAL, z {type}) PRIMARY KEY x LAYOUT(FLAT()) SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' password '' db 'default' table '{table_name}')) LIFETIME(0)")

        yield

    finally:
        with Finally("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

        with And("I drop the dictionary", flags=TE):
            node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

@TestSuite
def dictGet_granted_directly(self, node=None):
    """Run dictGet checks with privileges granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=dictGet_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in dictGet_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def dictGet_granted_via_role(self, node=None):
    """Run dictGet checks with privileges granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=dictGet_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in dictGet_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("dictGet", "dict"),
    ("dictHas", "dict"),
    ("dictGetHierarchy", "dict"),
    ("dictIsIn", "dict"),
])
@Requirements(
    RQ_SRS_006_RBAC_dictGet_RequiredPrivilege("1.0")
)
def dictGet_check(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is able to execute `dictGet` if and only if they have the necessary privileges.
    """
    if node is None:
        node = self.context.node

    dict_name = f"dict_{getuid()}"
    table_name = f"table_{getuid()}"

    on = on.replace("dict", f"{dict_name}")

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to dictGet without privilege"):
                node.query(f"SELECT dictGet ({dict_name},'y',toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with dict_setup(node, table_name, dict_name):

            with When(f"I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I attempt to dictGet with privilege"):
                node.query(f"SELECT dictGet ({dict_name},'y',toUInt64(1))", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with When("I attempt to dictGet without privilege"):
                node.query(f"SELECT dictGet ({dict_name},'y',toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

@TestSuite
def dictGetOrDefault_granted_directly(self, node=None):
    """Run dictGetOrDefault checks with privileges granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=dictGetOrDefault_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in dictGetOrDefault_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def dictGetOrDefault_granted_via_role(self, node=None):
    """Run dictGetOrDefault checks with privileges granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=dictGetOrDefault_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in dictGetOrDefault_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("dictGet", "dict"),
    ("dictHas", "dict"),
    ("dictGetHierarchy", "dict"),
    ("dictIsIn", "dict"),
])
@Requirements(
    RQ_SRS_006_RBAC_dictGet_OrDefault_RequiredPrivilege("1.0")
)
def dictGetOrDefault_check(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is able to execute `dictGetOrDefault` if and only if they have the necessary privileges.
    """
    if node is None:
        node = self.context.node

    dict_name = f"dict_{getuid()}"
    table_name = f"table_{getuid()}"

    on = on.replace("dict", f"{dict_name}")

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to dictGetOrDefault without privilege"):
                node.query(f"SELECT dictGetOrDefault ({dict_name},'y',toUInt64(1),toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with dict_setup(node, table_name, dict_name):

            with When(f"I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I attempt to dictGetOrDefault with privilege"):
                node.query(f"SELECT dictGetOrDefault ({dict_name},'y',toUInt64(1),toUInt64(1))", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with When("I attempt to dictGetOrDefault without privilege"):
                node.query(f"SELECT dictGetOrDefault ({dict_name},'y',toUInt64(1),toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

@TestSuite
def dictHas_granted_directly(self, node=None):
    """Run dictHas checks with privileges granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=dictHas_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in dictHas_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def dictHas_granted_via_role(self, node=None):
    """Run checks with privileges granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=dictHas_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in dictHas_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("dictGet", "dict"),
    ("dictHas", "dict"),
    ("dictGetHierarchy", "dict"),
    ("dictIsIn", "dict"),
])
@Requirements(
    RQ_SRS_006_RBAC_dictHas_RequiredPrivilege("1.0")
)
def dictHas_check(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is able to execute `dictHas` if and only if they have the necessary privileges.
    """
    if node is None:
        node = self.context.node

    dict_name = f"dict_{getuid()}"
    table_name = f"table_{getuid()}"

    on = on.replace("dict", f"{dict_name}")

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to dictHas without privilege"):
                node.query(f"SELECT dictHas({dict_name},toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I attempt to dictHas with privilege"):
                node.query(f"SELECT dictHas({dict_name},toUInt64(1))", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with When("I attempt to dictHas without privilege"):
                node.query(f"SELECT dictHas({dict_name},toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

@TestSuite
def dictGetHierarchy_granted_directly(self, node=None):
    """Run dictGetHierarchy checks with privileges granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        Suite(run=dictGetHierarchy_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in dictGetHierarchy_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def dictGetHierarchy_granted_via_role(self, node=None):
    """Run checks with privileges granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=dictGetHierarchy_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in dictGetHierarchy_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("dictGet", "dict"),
    ("dictHas", "dict"),
    ("dictGetHierarchy", "dict"),
    ("dictIsIn", "dict"),
])
@Requirements(
    RQ_SRS_006_RBAC_dictGetHierarchy_RequiredPrivilege("1.0")
)
def dictGetHierarchy_check(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is able to execute `dictGetHierarchy` if and only if they have the necessary privileges.
    """
    if node is None:
        node = self.context.node

    dict_name = f"dict_{getuid()}"
    table_name = f"table_{getuid()}"

    on = on.replace("dict", f"{dict_name}")

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to dictGetHierarchy without privilege"):
                node.query(f"SELECT dictGetHierarchy({dict_name},toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I attempt to dictGetHierarchy with privilege"):
                node.query(f"SELECT dictGetHierarchy({dict_name},toUInt64(1))", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with When("I attempt to dictGetHierarchy without privilege"):
                node.query(f"SELECT dictGetHierarchy({dict_name},toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

@TestSuite
def dictIsIn_granted_directly(self, node=None):
    """Run dictIsIn checks with privileges granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        Suite(run=dictIsIn_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in dictIsIn_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
def dictIsIn_granted_via_role(self, node=None):
    """Run checks with privileges granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=dictIsIn_check,
            examples=Examples("privilege on grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in dictIsIn_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("dictGet", "dict"),
    ("dictHas", "dict"),
    ("dictGetHierarchy", "dict"),
    ("dictIsIn", "dict"),
])
@Requirements(
    RQ_SRS_006_RBAC_dictIsIn_RequiredPrivilege("1.0")
)
def dictIsIn_check(self, privilege, on, grant_target_name, user_name, node=None):
    """Check that user is able to execute `dictIsIn` if and only if they have the necessary privileges.
    """
    if node is None:
        node = self.context.node

    dict_name = f"dict_{getuid()}"
    table_name = f"table_{getuid()}"

    on = on.replace("dict", f"{dict_name}")

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to dictIsIn without privilege"):
                node.query(f"SELECT dictIsIn({dict_name},toUInt64(1),toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I attempt to dictIsIn with privilege"):
                node.query(f"SELECT dictIsIn({dict_name},toUInt64(1),toUInt64(1))", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with dict_setup(node, table_name, dict_name):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with When("I attempt to dictIsIn without privilege"):
                node.query(f"SELECT dictIsIn({dict_name},toUInt64(1),toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

@TestSuite
@Examples("type",[
    ("Int8",),
    ("Int16",),
    ("Int32",),
    ("Int64",),
    ("UInt8",),
    ("UInt16",),
    ("UInt32",),
    ("UInt64",),
    ("Float32",),
    ("Float64",),
    ("Date",),
    ("DateTime",),
    ("UUID",),
    ("String",),
])
def dictGetType_granted_directly(self, type, node=None):
    """Run checks on dictGet with a type specified with privileges granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        Suite(run=dictGetType_check,
            examples=Examples("privilege on grant_target_name user_name type", [
                tuple(list(row)+[user_name,user_name,type]) for row in dictGetType_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestSuite
@Examples("type",[
    ("Int8",),
    ("Int16",),
    ("Int32",),
    ("Int64",),
    ("UInt8",),
    ("UInt16",),
    ("UInt32",),
    ("UInt64",),
    ("Float32",),
    ("Float64",),
    ("Date",),
    ("DateTime",),
    ("UUID",),
    ("String",),
])
def dictGetType_granted_via_role(self, type, node=None):
    """Run checks on dictGet with a type specified with privileges granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=dictGetType_check,
            examples=Examples("privilege on grant_target_name user_name type", [
                tuple(list(row)+[role_name,user_name,type]) for row in dictGetType_check.examples
            ], args=Args(name="check privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege on",[
    ("ALL", "*.*"),
    ("dictGet", "dict"),
    ("dictHas", "dict"),
    ("dictGetHierarchy", "dict"),
    ("dictIsIn", "dict"),
])
@Requirements(
    RQ_SRS_006_RBAC_dictGet_Type_RequiredPrivilege("1.0")
)
def dictGetType_check(self, privilege, on, grant_target_name, user_name, type, node=None):
    """Check that user is able to execute `dictGet` if and only if they have the necessary privileges.
    """
    if node is None:
        node = self.context.node

    dict_name = f"dict_{getuid()}"
    table_name = f"table_{getuid()}"

    on = on.replace("dict", f"{dict_name}")

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    with Scenario("user without privilege"):

        with dict_setup(node, table_name, dict_name, type):

            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I attempt to dictGet without privilege"):
                node.query(f"SELECT dictGet{type}({dict_name},'z',toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

    with Scenario("user with privilege"):

        with dict_setup(node, table_name, dict_name, type):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with Then("I attempt to dictGet with privilege"):
                node.query(f"SELECT dictGet{type}({dict_name},'z',toUInt64(1))", settings = [("user", user_name)])

    with Scenario("user with revoked privilege"):

        with dict_setup(node, table_name, dict_name, type):

            with When("I grant privilege"):
                node.query(f"GRANT {privilege} ON {on} TO {grant_target_name}")

            with And("I revoke privilege"):
                node.query(f"REVOKE {privilege} ON {on} FROM {grant_target_name}")

            with When("I attempt to dictGet without privilege"):
                node.query(f"SELECT dictGet{type}({dict_name},'z',toUInt64(1))", settings = [("user", user_name)], exitcode=exitcode, message=message)

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_dictGet_Privilege("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
@Name("dictGet")
def feature(self, node="clickhouse1", stress=None, parallel=None):
    """Check the RBAC functionality of dictGet.
    """
    self.context.node = self.context.cluster.node(node)

    if parallel is not None:
        self.context.parallel = parallel
    if stress is not None:
        self.context.stress = stress

    with Pool(20) as pool:
        tasks = []
        try:

            run_scenario(pool, tasks, Suite(test=dictGet_granted_directly, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictGet_granted_via_role, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictGetOrDefault_granted_directly, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictGetOrDefault_granted_via_role, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictHas_granted_directly, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictHas_granted_via_role, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictGetHierarchy_granted_directly, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictGetHierarchy_granted_via_role, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictIsIn_granted_directly, setup=instrument_clickhouse_server_log))
            run_scenario(pool, tasks, Suite(test=dictIsIn_granted_via_role, setup=instrument_clickhouse_server_log))

            for example in dictGetType_granted_directly.examples:
                type, = example

                with Example(example):
                    run_scenario(pool, tasks, Suite(test=dictGetType_granted_directly, setup=instrument_clickhouse_server_log),{"type" : type})
                    run_scenario(pool, tasks, Suite(test=dictGetType_granted_via_role, setup=instrument_clickhouse_server_log),{"type" : type})

        finally:
            join(tasks)
