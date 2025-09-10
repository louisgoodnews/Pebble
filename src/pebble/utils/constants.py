"""
Author: Louis Goodnews
Date: 2025-09-05
"""

import os
import re

from pathlib import Path
from typing import Final, List, Literal


__all__: Final[List[str]] = [
    "BOOLEAN",
    "COMBINATOR",
    "COMBINATORS",
    "CWD",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DICTIONARY",
    "FILTER_PATTERN",
    "FLOAT",
    "INTEGER",
    "JSON",
    "LIST",
    "OBJECT_SIZE_LIMIT",
    "OPERATOR",
    "OPERATORS",
    "PATH",
    "QUERY_PATTERN",
    "SCOPE",
    "SCOPES",
    "SET",
    "STRING",
    "TIME",
    "TUPLE",
    "UUID",
]


# Boolean type
BOOLEAN: Final[Literal["boolean"]] = "boolean"

# Valid combinators for a filter/query string
COMBINATOR: Final[Literal["AND", "and", "or", "OR", "&", "|", "&&", "||"]] = "and"

# List of valid combinators for a filter/query string
COMBINATORS: Final[list[Literal["AND", "and", "or", "OR", "&", "|", "&&", "||"]]] = [
    "AND",
    "and",
    "or",
    "OR",
    "&",
    "|",
    "&&",
    "||",
]

# Current working directory
CWD: Final[Path] = Path(os.getcwd())

# Date type
DATE: Final[Literal["date"]] = "date"

# Datetime type
DATETIME: Final[Literal["datetime"]] = "datetime"

# Decimal type
DECIMAL: Final[Literal["decimal"]] = "decimal"

# Dictionary type
DICTIONARY: Final[Literal["dictionary"]] = "dictionary"

# Filter pattern
FILTER_PATTERN: Final[re.Pattern] = re.compile(
    r"""
    ^\s*
    (?P<table>[A-Za-z_]\w*)\.
    (?P<field>\*|[A-Za-z_]\w*)\.
    (?P<scope>\*|ALL|ANY|NONE)\.
    (?P<operator>
        ==|!=|>=|<=|>|<|in|not\s+in|is|is\s+not
    )\.
    (?P<value>
        "(?:\\.|[^"])*"      |   # doppelt-quotiert mit Escapes
        '(?:\\.|[^'])*'      |   # einfach-quotiert mit Escapes
        [^.]+                    # unquotiert: alles bis zum nächsten Punkt (hier letztes Segment)
    )
    \s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Float type
FLOAT: Final[Literal["float"]] = "float"

# Integer type
INTEGER: Final[Literal["integer"]] = "integer"

# JSON type
JSON: Final[Literal["json"]] = "json"

# List type
LIST: Final[Literal["list"]] = "list"

# Object size limit for objects in any PebbleDatabase or PebbleTable
OBJECT_SIZE_LIMIT: Final[int] = 200000

# Valid operator for a filter/query string
OPERATOR: Final[
    Literal[
        "==",
        "!=",
        "<",
        ">",
        "<=",
        ">=",
        "&",
        "|",
        "and",
        "or",
        "in",
        "not in",
        "is",
        "is not",
    ]
] = "=="

# List of valid operators for a filter/query string
OPERATORS: Final[
    List[
        Literal[
            "==",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
            "&",
            "|",
            "and",
            "or",
            "in",
            "not in",
            "is",
            "is not",
            "AND",
            "OR",
        ]
    ]
] = [
    "==",
    "!=",
    "<",
    ">",
    "<=",
    ">=",
    "&",
    "|",
    "and",
    "or",
    "in",
    "not in",
    "is",
    "is not",
    "AND",
    "OR",
]


# Path type
PATH: Final[Literal["path"]] = "path"

# Query pattern
QUERY_PATTERN: Final[re.Pattern] = re.compile(
    flags=re.VERBOSE,
    pattern=r"""
    (?P<table>\w+)\.                  # table name
    (?P<field>\w+)\.                 # field (column) name
    (?P<scope>\*|\w+)\.               # scope (most '*')
    (?P<operator>==|!=|>|<|>=|<=)\.   # operator
    (?P<value>".*?"|\d+|\w+)          # value (string, number, identifier)
    (?:\.(?P<combinator>\&|\|))?      # optional .&. oder .|.
    """,
)

# Valid scope for a filter/query string
SCOPE: Final[Literal["*", "any", "all", "none"]] = "*"

# List of valid scopes for a filter/query string
SCOPES: Final[
    List[
        Literal[
            "*",
            "any",
            "all",
            "none",
        ]
    ]
] = [
    "*",
    "any",
    "all",
    "none",
]

# Set type
SET: Final[Literal["set"]] = "set"

# String type
STRING: Final[Literal["string"]] = "string"

# Time type
TIME: Final[Literal["time"]] = "time"

# Tuple type
TUPLE: Final[Literal["tuple"]] = "tuple"

# UUID type
UUID: Final[Literal["uuid"]] = "uuid"
