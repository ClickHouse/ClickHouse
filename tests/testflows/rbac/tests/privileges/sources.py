from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def file_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `File` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=file,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in file.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def file_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `File` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=file,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in file.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("FILE",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_File("1.0"),
)
def file(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `File` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("File source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the File source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=File()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("File source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the File source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=File()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("File source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the File source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=File()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def url_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `URL` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=url,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in url.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def url_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `URL` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=url,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in url.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("URL",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_URL("1.0"),
)
def url(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `URL` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("URL source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the URL source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=URL()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("URL source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the URL source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=URL()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("URL source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the URL source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=URL()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def remote_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a Remote source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=remote,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in remote.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def remote_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a Remote source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=remote,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in remote.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("REMOTE",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_Remote("1.0"),
)
def remote(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a remote source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("Remote source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the Remote source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE = Distributed()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("Remote source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the Remote source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE = Distributed()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("Remote source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the Remote source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE = Distributed()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def MySQL_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `MySQL` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=MySQL,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in MySQL.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def MySQL_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `MySQL` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=MySQL,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in MySQL.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("MYSQL",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_MySQL("1.0"),
)
def MySQL(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `MySQL` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("MySQL source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the MySQL source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=MySQL()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("MySQL source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the MySQL source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=MySQL()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("MySQL source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the MySQL source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=MySQL()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def ODBC_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `ODBC` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=ODBC,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in ODBC.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def ODBC_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `ODBC` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=ODBC,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in ODBC.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("ODBC",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_ODBC("1.0"),
)
def ODBC(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `ODBC` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("ODBC source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the ODBC source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=ODBC()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("ODBC source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the ODBC source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=ODBC()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("ODBC source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the ODBC source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=ODBC()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def JDBC_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `JDBC` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=JDBC,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in JDBC.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def JDBC_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `JDBC` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=JDBC,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in JDBC.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("JDBC",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_JDBC("1.0"),
)
def JDBC(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `JDBC` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("JDBC source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the JDBC source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=JDBC()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("JDBC source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the JDBC source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=JDBC()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("JDBC source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the JDBC source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=JDBC()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def HDFS_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `HDFS` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=HDFS,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in HDFS.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def HDFS_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `HDFS` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=HDFS,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in HDFS.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("HDFS",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_HDFS("1.0"),
)
def HDFS(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `HDFS` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("HDFS source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the HDFS source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=HDFS()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("HDFS source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the HDFS source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=HDFS()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("HDFS source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the HDFS source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=HDFS()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestSuite
def S3_privileges_granted_directly(self, node=None):
    """Check that a user is able to create a table from a `S3` source with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=S3,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in S3.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def S3_privileges_granted_via_role(self, node=None):
    """Check that a user is able to create a table from a `S3` source with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=S3,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in S3.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("SOURCES",),
    ("S3",),
])
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources_S3("1.0"),
)
def S3(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to to create a table from a `S3` source when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("S3 source without privilege"):
        table_name = f'table_{getuid()}'

        with Given("The user has table privilege"):
            node.query(f"GRANT CREATE TABLE ON {table_name} TO {grant_target_name}")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I check the user can't use the S3 source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=S3()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

    with Scenario("S3 source with privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with Then("I check the user can use the S3 source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=S3()", settings = [("user", f"{user_name}")],
                exitcode=42, message='Exception: Storage')

    with Scenario("S3 source with revoked privilege"):

        with When(f"I grant {privilege}"):
            node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

        with And(f"I revoke {privilege}"):
            node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

        with Then("I check the user cannot use the S3 source"):
            node.query(f"CREATE TABLE {table_name} (x String) ENGINE=S3()", settings=[("user",user_name)],
                exitcode=exitcode, message=message)

@TestFeature
@Name("sources")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Sources("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of SOURCES.
    """
    self.context.node = self.context.cluster.node(node)

    Suite(run=file_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=file_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=url_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=url_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=remote_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=remote_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=MySQL_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=MySQL_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=ODBC_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=ODBC_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=JDBC_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=JDBC_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=HDFS_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=HDFS_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
    Suite(run=S3_privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=S3_privileges_granted_via_role, setup=instrument_clickhouse_server_log)
