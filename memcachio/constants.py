from __future__ import annotations

import enum

LINE_END = b"\r\n"


class Commands(bytes, enum.Enum):
    GET = b"get"
    GETS = b"gets"
    GAT = b"gat"
    GATS = b"gats"
    SET = b"set"
    CAS = b"cas"
    ADD = b"add"
    REPLACE = b"replace"
    INCR = b"incr"
    DECR = b"decr"
    APPEND = b"append"
    PREPEND = b"prepend"
    TOUCH = b"touch"
    DELETE = b"delete"
    FLUSH_ALL = b"flush_all"
    VERSION = b"version"
    STATS = b"stats"


class Responses(bytes, enum.Enum):
    OK = b"OK"
    STORED = b"STORED"
    EXISTS = b"EXISTS"
    DELETED = b"DELETED"
    TOUCHED = b"TOUCHED"
    VALUE = b"VALUE"
    NOT_FOUND = b"NOT_FOUND"
    NOT_STORED = b"NOT_STORED"
    VERSION = b"VERSION"
    ERROR = b"ERROR"
    SERVER_ERROR = b"SERVER_ERROR"
    CLIENT_ERROR = b"CLIENT_ERROR"
    STAT = b"STAT"
    END = b"END"
