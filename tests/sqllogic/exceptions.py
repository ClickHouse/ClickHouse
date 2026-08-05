#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class Error(Exception):
    def __init__(
        self,
        message,
        *args,
        file=None,
        name=None,
        pos=None,
        request=None,
        details=None,
        **kwargs,
    ):
        super().__init__(message, *args, **kwargs)
        self._file = file
        self._name = name
        self._pos = pos
        self._request = request
        self._details = details

    @property
    def test_file(self):
        return self._file

    @property
    def test_name(self):
        return self._name

    @property
    def test_pos(self):
        return self._pos

    @property
    def request(self):
        return self._request

    @property
    def message(self):
        return super().__str__()

    @property
    def reason(self):
        details = f"details: {self._details}" if self._details else ""
        return ", ".join((str(x) for x in [super().__str__(), details] if x))

    def set_details(self, file=None, name=None, pos=None, request=None, details=None):
        if file is not None:
            self._file = file
        if name is not None:
            self._name = name
        if pos is not None:
            self._pos = pos
        if pos is not None:
            self._request = request
        if request is not None:
            self._request = request
        if details is not None:
            self._details = details

    def _at_file_and_pos(self):
        if self._file is not None and self._pos is not None:
            return f"at: [{self._file}:{self._pos}]"
        if self._name is not None and self._pos is not None:
            return f"at: [{self._name}:{self._pos}]"
        return None


class ErrorWithParent(Error):
    def __init__(self, message, *args, parent=None, **kwargs):
        super().__init__(message, *args, **kwargs)
        self._parent = parent

    def get_parent(self):
        return self._parent

    @property
    def reason(self):
        exception = f"exception: {self._parent}" if self._parent else ""
        return ", ".join((str(x) for x in [super().reason, exception] if x))


class ProgramError(ErrorWithParent):
    def __str__(self):
        return self.reason


class DataResultDiffer(Error):
    pass


class SchemeResultDiffer(Error):
    pass


class StatementExecutionError(ErrorWithParent):
    pass


class QueryExecutionError(ErrorWithParent):
    pass


class StatementSuccess(Error):
    def __init__(self, *args, **kwargs):
        message = kwargs["success"] if "message" in kwargs else "success"
        super().__init__(message, *args, **kwargs)


class QuerySuccess(Error):
    def __init__(self, *args, **kwargs):
        message = kwargs["success"] if "message" in kwargs else "success"
        super().__init__(message, *args, **kwargs)
