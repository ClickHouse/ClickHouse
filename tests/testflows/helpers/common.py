import os
import uuid
import time
import xml.etree.ElementTree as xmltree
from collections import namedtuple

import testflows.settings as settings
from testflows.core import *
from testflows.asserts import error
from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType


def getuid(with_test_name=False):
    if not with_test_name:
        return str(uuid.uuid1()).replace('-', '_')

    if current().subtype == TestSubType.Example:
        testname = f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace('-', '_')


@TestStep(Given)
def instrument_clickhouse_server_log(self, node=None, test=None,
        clickhouse_server_log="/var/log/clickhouse-server/clickhouse-server.log"):
    """Instrument clickhouse-server.log for the current test (default)
    by adding start and end messages that include test name to log
    of the specified node. If we are in the debug mode and the test
    fails then dump the messages from the log for this test.
    """
    if test is None:
    	test = current()

    if node is None:
        node = self.context.node

    with By("getting current log size"):
        cmd =  node.command(f"stat --format=%s {clickhouse_server_log}")
        start_logsize = cmd.output.split(" ")[0].strip()

    try:
        with And("adding test name start message to the clickhouse-server.log"):
            node.command(f"echo -e \"\\n-- start: {test.name} --\\n\" >> {clickhouse_server_log}")
        yield

    finally:
        if test.terminating is True:
            return

        with Finally("adding test name end message to the clickhouse-server.log", flags=TE):
           node.command(f"echo -e \"\\n-- end: {test.name} --\\n\" >> {clickhouse_server_log}")

        with And("getting current log size at the end of the test"):
           cmd =  node.command(f"stat --format=%s {clickhouse_server_log}")
           end_logsize = cmd.output.split(" ")[0].strip()

        with And("checking if test has failing result"):
            if settings.debug and not self.parent.result:
                with Then("dumping clickhouse-server.log for this test"):
                    node.command(f"tail -c +{start_logsize} {clickhouse_server_log}"
                    	f" | head -c {int(end_logsize) - int(start_logsize)}")


xml_with_utf8 = '<?xml version="1.0" encoding="utf-8"?>\n'


def xml_indent(elem, level=0, by="  "):
    i = "\n" + level * by
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + by
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            xml_indent(elem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def xml_append(root, tag, text):
    element = xmltree.Element(tag)
    element.text = text
    root.append(element)
    return element


Config = namedtuple("Config", "content path name uid preprocessed_name")


def create_xml_config_content(entries, config_file, config_d_dir="/etc/clickhouse-server/config.d"):
    """Create XML configuration file from a dictionary.
    """
    uid = getuid()
    path = os.path.join(config_d_dir, config_file)
    name = config_file
    root = xmltree.Element("yandex")
    root.append(xmltree.Comment(text=f"config uid: {uid}"))

    def create_xml_tree(entries, root):
        for k,v in entries.items():
            if type(v) is dict:
                xml_element = xmltree.Element(k)
                create_xml_tree(v, xml_element)
                root.append(xml_element)
            elif type(v) in (list, tuple):
                xml_element = xmltree.Element(k)
                for e in v:
                    create_xml_tree(e, xml_element)
                root.append(xml_element)
            else:
                xml_append(root, k, v)

    create_xml_tree(entries, root)
    xml_indent(root)
    content = xml_with_utf8 + str(xmltree.tostring(root, short_empty_elements=False, encoding="utf-8"), "utf-8")

    return Config(content, path, name, uid, "config.xml")


def add_config(config, timeout=300, restart=False, modify=False, node=None):
    """Add dynamic configuration file to ClickHouse.

    :param config: configuration file description
    :param timeout: timeout, default: 300 sec
    :param restart: restart server, default: False
    :param modify: only modify configuration file, default: False
    """
    if node is None:
        node = current().context.node
    cluster = current().context.cluster

    def check_preprocessed_config_is_updated(after_removal=False):
        """Check that preprocessed config is updated.
        """
        started = time.time()
        command = f"cat /var/lib/clickhouse/preprocessed_configs/{config.preprocessed_name} | grep {config.uid}{' > /dev/null' if not settings.debug else ''}"

        while time.time() - started < timeout:
            exitcode = node.command(command, steps=False).exitcode
            if after_removal:
                if exitcode == 1:
                    break
            else:
                if exitcode == 0:
                    break
            time.sleep(1)

        if settings.debug:
            node.command(f"cat /var/lib/clickhouse/preprocessed_configs/{config.preprocessed_name}")

        if after_removal:
            assert exitcode == 1, error()
        else:
            assert exitcode == 0, error()

    def wait_for_config_to_be_loaded():
        """Wait for config to be loaded.
        """
        if restart:
            with When("I close terminal to the node to be restarted"):
                bash.close()

            with And("I stop ClickHouse to apply the config changes"):
                node.stop(safe=False)

            with And("I get the current log size"):
                cmd = node.cluster.command(None,
                    f"stat --format=%s {cluster.environ['CLICKHOUSE_TESTS_DIR']}/_instances/{node.name}/logs/clickhouse-server.log")
                logsize = cmd.output.split(" ")[0].strip()

            with And("I start ClickHouse back up"):
                node.start()

            with Then("I tail the log file from using previous log size as the offset"):
                bash.prompt = bash.__class__.prompt
                bash.open()
                bash.send(f"tail -c +{logsize} -f /var/log/clickhouse-server/clickhouse-server.log")

        with Then("I wait for config reload message in the log file"):
            if restart:
                bash.expect(
                    f"ConfigReloader: Loaded config '/etc/clickhouse-server/config.xml', performed update on configuration",
                    timeout=timeout)
            else:
                bash.expect(
                    f"ConfigReloader: Loaded config '/etc/clickhouse-server/{config.preprocessed_name}', performed update on configuration",
                    timeout=timeout)

    try:
        with Given(f"{config.name}"):
            if settings.debug:
                with When("I output the content of the config"):
                    debug(config.content)

            with node.cluster.shell(node.name) as bash:
                bash.expect(bash.prompt)
                bash.send("tail -v -n 0 -f /var/log/clickhouse-server/clickhouse-server.log")
                # make sure tail process is launched and started to follow the file
                bash.expect("<==")
                bash.expect("\n")

                with When("I add the config", description=config.path):
                    command = f"cat <<HEREDOC > {config.path}\n{config.content}\nHEREDOC"
                    node.command(command, steps=False, exitcode=0)

                with Then(f"{config.preprocessed_name} should be updated", description=f"timeout {timeout}"):
                    check_preprocessed_config_is_updated()

                with And("I wait for config to be reloaded"):
                    wait_for_config_to_be_loaded()
        yield
    finally:
        if not modify:
            with Finally(f"I remove {config.name}"):
                with node.cluster.shell(node.name) as bash:
                    bash.expect(bash.prompt)
                    bash.send("tail -v -n 0 -f /var/log/clickhouse-server/clickhouse-server.log")
                    # make sure tail process is launched and started to follow the file
                    bash.expect("<==")
                    bash.expect("\n")

                    with By("removing the config file", description=config.path):
                        node.command(f"rm -rf {config.path}", exitcode=0)

                    with Then(f"{config.preprocessed_name} should be updated", description=f"timeout {timeout}"):
                        check_preprocessed_config_is_updated(after_removal=True)

                    with And("I wait for config to be reloaded"):
                        wait_for_config_to_be_loaded()


@TestStep(When)
def copy(self, dest_node, src_path, dest_path, bash=None, binary=False, eof="EOF", src_node=None):
    """Copy file from source to destination node.
    """
    if binary:
        raise NotImplementedError("not yet implemented; need to use base64 encoding")

    bash = self.context.cluster.bash(node=src_node)

    cmd = bash(f"cat {src_path}")

    assert cmd.exitcode == 0, error()
    contents = cmd.output

    dest_node.command(f"cat << {eof} > {dest_path}\n{contents}\n{eof}")
