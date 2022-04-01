import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))

import httpexpect

def client(request, name='', log=None):
    client = httpexpect.spawn({'host':'localhost','port':8123,'timeout':30}, request)
    client.logger(log, prefix=name)
    client.timeout(20)
    return client
