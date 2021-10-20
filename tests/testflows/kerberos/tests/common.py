from testflows.core import *
from testflows.asserts import error
from contextlib import contextmanager
import xml.etree.ElementTree as xmltree

import time
import uuid


def getuid():
    return str(uuid.uuid1()).replace('-', '_')


def xml_append(root, tag, text=Null):
    element = xmltree.Element(tag)
    if text:
        element.text = text
    root.append(element)


def xml_write(data, filename):
    strdata = xmltree.tostring(data)
    with open(filename, "wb") as f:
        f.write(strdata)


def xml_parse_file(filename):
    return xmltree.parse(filename).getroot()


def create_default_config(filename):
    contents = ""
    if "kerberos_users.xml" in filename:
        contents = "<yandex><users><kerberos_user><kerberos><realm>EXAMPLE.COM" \
               "</realm></kerberos></kerberos_user></users></yandex>"
    elif "kerberos.xml" in filename:
        contents = "<yandex><kerberos><realm>EXAMPLE.COM</realm></kerberos></yandex>"

    with open(filename, "w") as f:
        f.write(contents)


def test_select_query(node, krb_auth=True, req="SELECT currentUser()"):
    """ Helper forming a HTTP query to ClickHouse server
    """
    if krb_auth:
        return f"echo '{req}' | curl --negotiate -u : 'http://{node.name}:8123/' --data-binary @-"
    else:
        return f"echo '{req}' | curl 'http://{node.name}:8123/' --data-binary @-"


@TestStep(Given)
def kinit_no_keytab(self, node, principal="kerberos_user", lifetime_option="-l 10:00"):
    """ Helper for obtaining Kerberos ticket for client
    """
    try:
        node.cmd("echo pwd | kinit admin/admin")
        node.cmd(f"kadmin -w pwd -q \"add_principal -pw pwd {principal}\"")
        node.cmd(f"echo pwd | kinit {lifetime_option} {principal}")
        yield
    finally:
        node.cmd("kdestroy")


@TestStep(Given)
def create_server_principal(self, node):
    """ Helper for obtaining Kerberos ticket for server
    """
    try:
        node.cmd("echo pwd | kinit admin/admin")
        node.cmd(f"kadmin -w pwd -q \"add_principal -randkey HTTP/docker-compose_{node.name}_1.docker-compose_default\"")
        node.cmd(f"kadmin -w pwd -q \"ktadd -k /etc/krb5.keytab HTTP/docker-compose_{node.name}_1.docker-compose_default\"")
        yield
    finally:
        node.cmd("kdestroy")
        node.cmd("rm /etc/krb5.keytab")


@TestStep(Given)
def save_file_state(self, node, filename):
    """ Save current file and then restore it, restarting the node
    """
    try:
        with When("I save file state"):
            with open(filename, 'r') as f:
                a = f.read()
        yield
    finally:
        with Finally("I restore initial state"):
            with open(filename, 'w') as f:
                f.write(a)
            node.restart()


@TestStep(Given)
def temp_erase(self, node, filename=None):
    """ Temporary erasing config file and restarting the node
    """
    if filename is None:
        filename = f"kerberos/configs/{node.name}/config.d/kerberos.xml"
    with When("I save file state"):
        with open(filename, 'r') as f:
            a = f.read()
    try:
        with Then("I overwrite file to be dummy"):
            with open(filename, 'w') as f:
                f.write("<yandex></yandex>\n")
            node.restart()
            yield
    finally:
        with Finally("I restore initial file state"):
            with open(filename, 'w') as f:
                f.write(a)
            node.restart()


def restart(node, config_path, safe=False, timeout=60):
    """Restart ClickHouse server and wait for config to be reloaded.
    """

    filename = '/etc/clickhouse-server/config.xml' if 'config.d' in config_path else '/etc/clickhouse-server/users.xml'
    with When("I restart ClickHouse server node"):
        with node.cluster.shell(node.name) as bash:
            bash.expect(bash.prompt)

            with By("closing terminal to the node to be restarted"):
                bash.close()

            with And("getting current log size"):
                logsize = \
                    node.command("stat --format=%s /var/log/clickhouse-server/clickhouse-server.log").output.split(" ")[0].strip()

            with And("restarting ClickHouse server"):
                node.restart(safe=safe)

            with Then("tailing the log file from using previous log size as the offset"):
                bash.prompt = bash.__class__.prompt
                bash.open()
                bash.send(f"tail -c +{logsize} -f /var/log/clickhouse-server/clickhouse-server.log")

            with And("waiting for config reload message in the log file"):
                bash.expect(
                    f"ConfigReloader: Loaded config '{filename}', performed update on configuration",
                    timeout=timeout)


@TestStep
def check_wrong_config(self, node, client, config_path, modify_file, log_error="", output="",
                       tail=120, timeout=60, healthy_on_restart=True):
    """Check that ClickHouse errors when trying to load invalid configuration file.
    """
    preprocessed_name = "config.xml" if "config.d" in config_path else "users.xml"

    full_config_path = "/etc/clickhouse-server/config.d/kerberos.xml" if "config.d" in config_path else "/etc/clickhouse-server/users.d/kerberos-users.xml"

    uid = getuid()

    try:
        with Given("I save config file to restore it later"):
            with open(config_path, 'r') as f:
                initial_contents = f.read()

        with And("I prepare the error log by writing empty lines into it"):
            node.command("echo -e \"%s\" > /var/log/clickhouse-server/clickhouse-server.err.log" % ("-\\n" * tail))

        with When("I modify xml file"):
            root = xml_parse_file(config_path)
            root = modify_file(root)
            root.append(xmltree.fromstring(f"<comment>{uid}</comment>"))
            config_contents = xmltree.tostring(root, encoding='utf8', method='xml').decode('utf-8')
            command = f"cat <<HEREDOC > {full_config_path}\n{config_contents}\nHEREDOC"
            node.command(command, steps=False, exitcode=0)
            # time.sleep(1)

        with Then(f"{preprocessed_name} should be updated", description=f"timeout {timeout}"):
            started = time.time()
            command = f"cat /var/lib/clickhouse/preprocessed_configs/{preprocessed_name} | grep {uid} > /dev/null"
            while time.time() - started < timeout:
                exitcode = node.command(command, steps=False).exitcode
                if exitcode == 0:
                    break
                time.sleep(1)
            assert exitcode == 0, error()

        with When("I restart ClickHouse to apply the config changes"):
            if output:
                node.restart(safe=False, wait_healthy=True)
            else:
                node.restart(safe=False, wait_healthy=False)

        if output != "":
            with Then(f"check {output} is in output"):
                time.sleep(5)
                started = time.time()
                while time.time() - started < timeout:
                    kinit_no_keytab(node=client)
                    create_server_principal(node=node)
                    r = client.cmd(test_select_query(node=node), no_checks=True)
                    if output in r.output:
                        assert True, error()
                        break
                    time.sleep(1)
                else:
                    assert False, error()

    finally:
        with Finally("I restore original config"):
            with By("restoring the (correct) config file"):
                with open(config_path, 'w') as f:
                    f.write(initial_contents)
            with And("restarting the node"):
                node.restart(safe=False)

    if log_error != "":
        with Then("error log should contain the expected error message"):
            started = time.time()
            command = f"tail -n {tail} /var/log/clickhouse-server/clickhouse-server.err.log | grep \"{log_error}\""
            while time.time() - started < timeout:
                exitcode = node.command(command, steps=False).exitcode
                if exitcode == 0:
                    break
                time.sleep(1)
            assert exitcode == 0, error()


