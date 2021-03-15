from testflows.core import *
from kerberos.tests.common import *
from kerberos.requirements.requirements import *

import time
import datetime
import itertools


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_KerberosNotEnabled("1.0")
)
def kerberos_not_enabled(self):
    """ClickHouse SHALL reject Kerberos authentication if user is properly configured for Kerberos,
    but Kerberos itself is not enabled in config.xml.
    """
    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/config.d/kerberos.xml"

    def modify_file(root):
        return xmltree.fromstring("<yandex></yandex>")

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       output="Kerberos is not enabled")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_MultipleKerberosSections("1.0")
)
def multiple_kerberos(self):
    """ClickHouse SHALL disable Kerberos authentication if more than one kerberos sections specified in config.xml.
    """
    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/config.d/kerberos.xml"

    def modify_file(root):
        second_section = "<kerberos><realm>EXAM.COM</realm></kerberos>"
        root.append(xmltree.fromstring(second_section))
        return root

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       log_error="Multiple kerberos sections are not allowed", healthy_on_restart=False)


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_WrongUserRealm("1.0")
)
def wrong_user_realm(self):
    """ClickHouse SHALL reject Kerberos authentication if user's realm specified in users.xml
    doesn't match the realm of the principal trying to authenticate.
    """

    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/users.d/kerberos-users.xml"

    def modify_file(root):
        krb = root.find('users').find('kerberos_user')
        krb.find('kerberos').find('realm').text = "OTHER.COM"
        return root

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       output="Authentication failed")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_MultipleAuthMethods("1.0")
)
def multiple_auth_methods(self):
    """ClickHouse SHALL reject Kerberos authentication if other
    auth method is specified for user alongside with Kerberos.
    """
    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/users.d/kerberos-users.xml"

    def modify_file(root):
        krb = root.find('users').find('kerberos_user')
        xml_append(krb, 'password', 'qwerty')
        return root

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       log_error="More than one field of", healthy_on_restart=False)


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_PrincipalAndRealmSpecified("1.0")
)
def principal_and_realm_specified(self):
    """ClickHouse SHALL drop an exception if both realm and principal fields are specified in config.xml.
    """
    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/config.d/kerberos.xml"

    def modify_file(root):
        krb = root.find('kerberos')
        xml_append(krb, 'principal', 'HTTP/srv1@EXAMPLE.COM')
        return root

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       log_error="Realm and principal name cannot be specified simultaneously",
                       output="Kerberos is not enabled")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_MultipleRealmSections("1.0")
)
def multiple_realm(self):
    """ClickHouse SHALL throw an exception and disable Kerberos if more than one realm is specified in config.xml.
    """
    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/config.d/kerberos.xml"

    def modify_file(root):
        krb = root.find('kerberos')
        xml_append(krb, 'realm', 'EXAM.COM')
        return root

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       log_error="Multiple realm sections are not allowed")


@TestScenario
@Requirements(
    RQ_SRS_016_Kerberos_Configuration_MultiplePrincipalSections("1.0")
)
def multiple_principal(self):
    """ClickHouse SHALL throw an exception and disable Kerberos if more than one principal is specified in config.xml.
    """
    ch_nodes = self.context.ch_nodes
    config_path = f"kerberos/configs/{ch_nodes[0].name}/config.d/kerberos.xml"

    def modify_file(root):
        krb = root.find('kerberos')
        krb.remove(krb.find('realm'))
        xml_append(krb, 'principal', 'HTTP/s1@EXAMPLE.COM')
        xml_append(krb, 'principal', 'HTTP/s2@EXAMPLE.COM')
        return root

    check_wrong_config(node=ch_nodes[0], client=ch_nodes[2], config_path=config_path, modify_file=modify_file,
                       log_error="Multiple principal sections are not allowed")







@TestFeature
def config(self):
    """Perform ClickHouse Kerberos authentication testing for incorrect configuration files
    """

    self.context.ch_nodes = [self.context.cluster.node(f"clickhouse{i}") for i in range(1, 4)]
    self.context.krb_server = self.context.cluster.node("kerberos")
    self.context.clients = [self.context.cluster.node(f"krb-client{i}") for i in range(1, 6)]

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario, flags=TE)
