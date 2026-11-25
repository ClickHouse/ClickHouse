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
import pty
import re
import sys
import time
from queue import Empty, Queue
from subprocess import Popen
from threading import Event, Thread


class TimeoutError(Exception):
    def __init__(self, timeout):
        self.timeout = timeout

    def __str__(self):
        return "Timeout %.3fs" % float(self.timeout)


class ExpectTimeoutError(Exception):
    def __init__(self, pattern, timeout, buffer):
        self.pattern = pattern
        self.timeout = timeout
        self.buffer = buffer

    def __str__(self):
        s = "Timeout %.3fs " % float(self.timeout)
        if self.pattern:
            s += "for %s " % repr(self.pattern.pattern)
        if self.buffer:
            s += "buffer %s" % repr(self.buffer[:])
            # s += ' or \'%s\'' % ','.join(['%x' % ord(c) for c in self.buffer[:]])
        return s


class IO(object):
    class EOF(object):
        pass

    class Timeout(object):
        pass

    EOF = EOF
    TIMEOUT = Timeout

    class Logger(object):
        def __init__(self, logger, prefix=""):
            self._logger = logger
            self._prefix = prefix

        def write(self, data):
            self._logger.write(("\n" + data).replace("\n", "\n" + self._prefix))

        def flush(self):
            self._logger.flush()

    def __init__(self, process, master, queue, reader):
        self.process = process
        self.master = master
        self.queue = queue
        self.buffer = None
        self.before = None
        self.after = None
        self.match = None
        self.pattern = None
        self.reader = reader
        self._timeout = None
        self._logger = None
        self._eol = ""

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def logger(self, logger=None, prefix=""):
        if logger:
            self._logger = self.Logger(logger, prefix=prefix)
        return self._logger

    def timeout(self, timeout=None):
        if timeout:
            self._timeout = timeout
        return self._timeout

    def eol(self, eol=None):
        if eol:
            self._eol = eol
        return self._eol

    def close(self, force=True):
        self.reader["kill_event"].set()
        os.system("pkill -TERM -P %d" % self.process.pid)
        if force:
            self.process.kill()
        else:
            self.process.terminate()
        os.close(self.master)
        if self._logger:
            self._logger.write("\n")
            self._logger.flush()

    def send(self, data, eol=None):
        if eol is None:
            eol = self._eol
        return self.write(data + eol)

    def write(self, data):
        return os.write(self.master, data.encode())

    def expect(self, pattern, timeout=None, escape=False):
        self.match = None
        self.before = None
        self.after = None
        if escape:
            pattern = re.escape(pattern)
        pattern = re.compile(pattern)
        if timeout is None:
            timeout = self._timeout
        timeleft = timeout
        while True:
            start_time = time.time()
            if self.buffer is not None:
                self.match = pattern.search(self.buffer, 0)
                if self.match is not None:
                    self.after = self.buffer[self.match.start() : self.match.end()]
                    self.before = self.buffer[: self.match.start()]
                    self.buffer = self.buffer[self.match.end() :]
                    break
            if timeleft < 0:
                break
            try:
                data = self.read(timeout=timeleft, raise_exception=True)
            except TimeoutError:
                if self._logger:
                    self._logger.write((self.buffer or "") + "\n")
                    self._logger.flush()
                exception = ExpectTimeoutError(pattern, timeout, self.buffer)
                self.buffer = None
                raise exception
            timeleft -= time.time() - start_time
            if data:
                self.buffer = (self.buffer + data) if self.buffer else data
        if self._logger:
            self._logger.write((self.before or "") + (self.after or ""))
            self._logger.flush()
        if self.match is None:
            exception = ExpectTimeoutError(pattern, timeout, self.buffer)
            self.buffer = None
            raise exception
        return self.match

    def read(self, timeout=0, raise_exception=False):
        data = ""
        timeleft = timeout
        try:
            while timeleft >= 0:
                start_time = time.time()
                data += self.queue.get(timeout=timeleft)
                if data:
                    break
                timeleft -= time.time() - start_time
        except Empty:
            if data:
                return data
            if raise_exception:
                raise TimeoutError(timeout)
            pass
        if not data and raise_exception:
            raise TimeoutError(timeout)

        return data


def spawn(command):
    master, slave = pty.openpty()
    process = Popen(
        command,
        preexec_fn=os.setsid,
        stdout=slave,
        stdin=slave,
        stderr=slave,
        bufsize=1,
    )
    os.close(slave)

    queue = Queue()
    reader_kill_event = Event()
    thread = Thread(target=reader, args=(process, master, queue, reader_kill_event))
    thread.daemon = True
    thread.start()

    return IO(
        process,
        master,
        queue,
        reader={"thread": thread, "kill_event": reader_kill_event},
    )


def reader(process, out, queue, kill_event):
    while True:
        try:
            data = os.read(out, 65536).decode(errors="replace")
            queue.put(data)
        except:
            if kill_event.is_set():
                break
            raise
