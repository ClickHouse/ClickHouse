from testflows.core import *
from testflows.asserts import error

from ldap.authentication.tests.common import add_user_to_ldap, delete_user_from_ldap

@TestScenario
@Name("sanity")
def scenario(self, server="openldap1"):
    """Check that LDAP server is up and running by
    executing ldapsearch, ldapadd, and ldapdelete commands.
    """
    self.context.ldap_node = self.context.cluster.node(server)

    with When("I search LDAP database"):
        r = self.context.ldap_node.command(
            "ldapsearch -x -H ldap://localhost -b \"dc=company,dc=com\" -D \"cn=admin,dc=company,dc=com\" -w admin")
        assert r.exitcode == 0, error()

    with Then("I should find an entry for user1"):
        assert "dn: cn=user1,ou=users,dc=company,dc=com" in r.output, error()

    with When("I add new user to LDAP"):
        user = add_user_to_ldap(cn="myuser", userpassword="myuser")

    with And("I search LDAP database again"):
        r = self.context.ldap_node.command(
            "ldapsearch -x -H ldap://localhost -b \"dc=company,dc=com\" -D \"cn=admin,dc=company,dc=com\" -w admin")
        assert r.exitcode == 0, error()

    with Then("I should find an entry for the new user"):
        assert f"dn: {user['dn']}" in r.output, error()

    with When("I delete user from LDAP"):
        delete_user_from_ldap(user)

    with And("I search LDAP database again"):
        r = self.context.ldap_node.command(
            "ldapsearch -x -H ldap://localhost -b \"dc=company,dc=com\" -D \"cn=admin,dc=company,dc=com\" -w admin")
        assert r.exitcode == 0, error()

    with Then("I should not find an entry for the deleted user"):
        assert f"dn: {user['dn']}" not in r.output, error()
