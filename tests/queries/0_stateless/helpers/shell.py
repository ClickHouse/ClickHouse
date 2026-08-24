import os
import sys
import time

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))

import uexpect


class shell(object):
    def __init__(self, command=None, name="", log=None, prompt="[#\\$] "):
        if command is None:
            command = ["/bin/bash", "--noediting"]
        self.prompt = prompt
        self.client = uexpect.spawn(command)
        self.client.eol("\r")
        self.client.logger(log, prefix=name)
        self.client.timeout(20)
        self.client.expect(prompt, timeout=60)

    def __enter__(self):
        io = self.client.__enter__()
        io.prompt = self.prompt
        return io

    def __exit__(self, type, value, traceback):
        self.client.reader["kill_event"].set()
        # send Ctrl-C
        self.client.send("\x03", eol="")
        time.sleep(0.3)
        self.client.send("exit", eol="\r")
        self.client.send("\x03", eol="")
        return self.client.__exit__(type, value, traceback)
