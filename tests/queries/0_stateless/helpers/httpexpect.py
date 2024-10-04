# Copyright (c) 2019 Vitaliy Zakaznikov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import sys
import http.client

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, CURDIR)

import uexpect

from threading import Thread, Event
from queue import Queue, Empty


class IO(uexpect.IO):
    def __init__(self, connection, response, queue, reader):
        self.connection = connection
        self.response = response
        super(IO, self).__init__(None, None, queue, reader)

    def write(self, data):
        raise NotImplementedError

    def close(self, force=True):
        self.reader["kill_event"].set()
        self.connection.close()
        if self._logger:
            self._logger.write("\n")
            self._logger.flush()


def reader(response, queue, kill_event):
    while True:
        try:
            if kill_event.is_set():
                break
            data = response.read(1).decode()
            queue.put(data)
        except Exception as e:
            if kill_event.is_set():
                break
            raise


def spawn(connection, request):
    connection = http.client.HTTPConnection(**connection)
    connection.request(**request)
    response = connection.getresponse()

    queue = Queue()
    reader_kill_event = Event()
    thread = Thread(target=reader, args=(response, queue, reader_kill_event))
    thread.daemon = True
    thread.start()

    return IO(
        connection,
        response,
        queue,
        reader={"thread": thread, "kill_event": reader_kill_event},
    )


if __name__ == "__main__":
    with spawn(
        {"host": "localhost", "port": 8123},
        {"method": "GET", "url": "?query=SELECT%201"},
    ) as client:
        client.logger(sys.stdout)
        client.timeout(2)
        print(client.response.status, client.response.reason)
        client.expect("1\n")
