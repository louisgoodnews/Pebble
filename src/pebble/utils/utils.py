"""
Author: Louis Goodnews
Date: 2025-09-05
"""

import aiofiles
import asyncio
import json
import os
import re

from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Final, List, Optional, Union
from uuid import UUID

from utils.exceptions import (
    PebbleFileNotCreatedError,
    PebbleFileNotDeletedError,
    PebbleFileNotFoundException,
    PebbleFileReadError,
    PebbleFileWriteError,
)


__all__: Final[List[str]] = [
    "convert_to_path",
    "create_file",
    "cwd",
    "date_to_string",
    "datetime_to_string",
    "decimal_to_string",
    "delete_file",
    "dict_to_json",
    "find_all_patterns",
    "get_uuid",
    "is_date",
    "is_datetime",
    "is_decimal",
    "is_dict",
    "is_list",
    "is_path",
    "is_set",
    "is_stale",
    "is_string_quoted",
    "is_time",
    "is_tuple",
    "is_uuid",
    "json_to_dict",
    "lock",
    "loop",
    "match_pattern",
    "object_to_string",
    "path_exists",
    "path_to_string",
    "PebbleFieldTypes",
    "quote_string",
    "read_file",
    "run_asynchronously",
    "string_to_date",
    "string_to_datetime",
    "string_to_decimal",
    "string_to_dict",
    "string_to_object",
    "string_to_path",
    "string_to_time",
    "string_to_uuid",
    "time_to_string",
    "unquote_string",
    "uuid_to_string",
    "write_file",
]


class PebbleFieldTypes(Enum):
    """
    Enum for Pebble field types.

    Attributes:
        BOOLEAN (Literal["boolean"]): The boolean field type.
        DATE (Literal["date"]): The date field type.
        DATETIME (Literal["datetime"]): The datetime field type.
        DECIMAL (Literal["decimal"]): The decimal field type.
        DICTIONARY (Literal["dictionary"]): The dictionary field type.
        FLOAT (Literal["float"]): The float field type.
        INTEGER (Literal["integer"]): The integer field type.
        LIST (Literal["list"]): The list field type.
        PATH (Literal["path"]): The path field type.
        SET (Literal["set"]): The set field type.
        STRING (Literal["string"]): The string field type.
        TIME (Literal["time"]): The time field type.
        TUPLE (Literal["tuple"]): The tuple field type.
        UUID (Literal["uuid"]): The UUID field type.
    """

    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    DECIMAL = "decimal"
    DICTIONARY = "dictionary"
    FLOAT = "float"
    INTEGER = "integer"
    LIST = "list"
    PATH = "path"
    SET = "set"
    STRING = "string"
    TIME = "time"
    TUPLE = "tuple"
    UUID = "uuid"

    def __str__(self) -> str:
        """
        Return the string representation of the enum.

        Returns:
            str: The string representation of the enum.
        """

        return self.value


# Current working directory
CWD: Optional[Path] = None

# Lock for file operations
LOCK: Optional[asyncio.Lock] = None

# Event loop for file operations
LOOP: Optional[asyncio.AbstractEventLoop] = None


def convert_to_path(
    path: Optional[Union[list[str], Path, set[str], str, tuple[str]]] = None,
) -> Path:
    """
    Convert a path to a Path object.

    Args:
        path (Union[list[str], Path, set[str], str, tuple[str]]): The path to convert.

    Returns:
        Path: The Path object.
    """

    # Check if the path is None
    if path is None:
        # Return the current working directory
        return cwd()

    # Check if the path is a Path object
    if isinstance(
        path,
        Path,
    ):
        # Return the path
        return path

    # Check if the path is a list
    if isinstance(
        path,
        (list, set, tuple),
    ):
        path = Path("/".join(path))
    else:
        # Convert the path to a Path object
        path = Path(path)

    # Return the path
    return path


async def create_file(path: Path) -> bool:
    """
    Create a file at the given path.

    Args:
        path (Path): The path to create the file at.

    Returns:
        bool: True if the file was created, False otherwise.
    """

    try:
        # Check if the file exists
        if not path.exists():
            # Acquire a lock to ensure thread safety
            async with lock():
                # Create the file
                path.touch()

            # Return True if the file was created
            return True

        # Return False if the file already exists
        return False
    except Exception as e:
        # Re-raise the exception with a custom message
        raise PebbleFileNotCreatedError(path=path) from e


def cwd() -> Path:
    """
    Get the current working directory.

    Returns:
        Path: The current working directory.
    """

    # Declare the global variable
    global CWD

    # Check if the current working directory is None
    if CWD is None:
        # Set the current working directory
        CWD = Path(os.getcwd())

    # Return the current working directory
    return CWD


def date_to_string(
    value: date,
    format: Optional[str] = None,
) -> str:
    """
    Convert a date to a string.

    Args:
        value (date): The date to convert.
        format (Optional[str]): The format to use when converting the date.

    Returns:
        str: The string representation of the date.
    """

    # Check if the format is None
    if format is None:
        # Return the ISO format of the date
        return value.isoformat()

    # Return the formatted date
    return value.strftime(format)


def datetime_to_string(
    value: datetime,
    format: Optional[str] = None,
) -> str:
    """
    Convert a datetime to a string.

    Args:
        value (datetime): The datetime to convert.
        format (Optional[str]): The format to use when converting the datetime.

    Returns:
        str: The string representation of the datetime.
    """

    # Check if the format is None
    if format is None:
        # Return the ISO format of the datetime
        return value.isoformat()

    # Return the formatted datetime
    return value.strftime(format)


def decimal_to_string(value: Decimal) -> str:
    """
    Convert a Decimal to a string.

    Args:
        value (Decimal): The Decimal to convert.

    Returns:
        str: The string representation of the Decimal.
    """

    return str(value)


def delete_file(path: Path) -> bool:
    """
    Delete a file at the given path.

    Args:
        path (Path): The path to delete the file at.

    Returns:
        bool: True if the file was deleted, False otherwise.
    """

    try:
        # Check if the file exists
        if path.exists():
            # Delete the file
            path.unlink()

            # Return True if the file was deleted
            return True

        # Return False if the file does not exist
        return False
    except Exception as e:
        # Re-raise the exception with a custom message
        raise PebbleFileNotDeletedError(path=path) from e


def dict_to_json(
    dictionary: dict[str, Any],
    handler: Optional[Callable[[Any], Any]] = None,
) -> str:
    """
    Convert a dictionary to a JSON string.

    Args:
        dictionary (dict[str, Any]): The dictionary to convert.
        handler (Optional[Callable[[Any], Any]]): The handler to use when processing the value.

    Returns:
        str: The JSON string representation of the dictionary.
    """

    def process_value(
        value: Any,
        handler: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        """
        Process a value.

        Args:
            value (Any): The value to process.
            handler (Optional[Callable[[Any], Any]]): The handler to use when processing the value.

        Returns:
            Any: The processed value.
        """

        # Check if the value is None
        if value is None:
            # Return "null" if the value is None
            return "null"

        # Check if the value is a dictionary
        if isinstance(
            value,
            dict,
        ):
            # Process the dictionary
            return {
                k: process_value(
                    handler=handler,
                    value=v,
                )
                for (
                    k,
                    v,
                ) in value.items()
            }
        # Check if the value is a list
        elif isinstance(
            value,
            list,
        ):
            # Process the list
            return [
                process_value(
                    handler=handler,
                    value=item,
                )
                for item in value
            ]
        # Check if the value is a primitive type
        elif isinstance(
            value,
            (
                int,
                float,
                str,
                bool,
            ),
        ):
            # Return the value as is
            return value

        # Call the handler if it is not None
        if handler is not None:
            # Call the handler
            return handler(value)

        if isinstance(
            value,
            date,
        ):
            try:
                # Attempt to convert the value to a date string
                return date_to_string(value=value)
            except ValueError:
                pass

        if isinstance(
            value,
            datetime,
        ):
            try:
                # Attempt to convert the value to a datetime string
                return datetime_to_string(value=value)
            except ValueError:
                pass

        if isinstance(
            value,
            Decimal,
        ):
            try:
                # Attempt to convert the value to a Decimal string
                return decimal_to_string(value=value)
            except ValueError:
                pass

        if isinstance(
            value,
            Path,
        ):
            try:
                # Attempt to convert the value to a Path string
                return path_to_string(value=value)
            except ValueError:
                pass

        if isinstance(
            value,
            time,
        ):
            try:
                # Attempt to convert the value to a time string
                return time_to_string(value=value)
            except ValueError:
                pass

        if isinstance(
            value,
            UUID,
        ):
            try:
                # Attempt to convert the value to a UUID string
                return uuid_to_string(value=value)
            except ValueError:
                pass

        # Return the value as is
        return value

    # Return the JSON string representation of the dictionary
    return json.dumps(
        {
            k: process_value(
                handler=handler,
                value=v,
            )
            for (
                k,
                v,
            ) in dictionary.items()
        }
    )


def find_all_patterns(
    pattern: re.Pattern,
    string: str,
) -> list[dict[str, Any]]:
    """
    Find all patterns in a string.

    Args:
        pattern (re.Pattern): The pattern to find.
        string (str): The string to search.

    Returns:
        list[dict[str, Any]]: The list of patterns found.
    """

    # Return the list of patterns found
    return [match.groupdict() for match in pattern.finditer(string=string)]


def get_uuid(as_string: bool = False) -> Union[UUID, str]:
    """
    Generate a UUID.

    Args:
        as_string (bool): Whether to return the UUID as a string.

    Returns:
        Union[UUID, str]: The generated UUID.
    """

    return uuid.uuid4() if not as_string else uuid.uuid4().hex


def json_to_dict(
    json_string: str,
    handler: Optional[Callable[[Any], Any]] = None,
) -> dict[str, Any]:
    """
    Convert a JSON string to a dictionary.

    Args:
        json_string (str): The JSON string to convert.
        handler (Optional[Callable[[Any], Any]]): The handler to use when processing the value.

    Returns:
        dict[str, Any]: The dictionary representation of the JSON string.
    """

    def process_value(
        value: Any,
        handler: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        """
        Process a value.

        Args:
            handler (Optional[Callable[[Any], Any]]): The handler to use when processing the value.
            value (Any): The value to process.

        Returns:
            Any: The processed value.
        """

        # Check if the value is None
        if not value:
            # Return the value as is
            return value

        # Check if the value is a dictionary
        if isinstance(
            value,
            dict,
        ):
            # Process the dictionary
            return {
                k: process_value(
                    handler=handler,
                    value=v,
                )
                for (
                    k,
                    v,
                ) in value.items()
            }
        # Check if the value is a list
        elif isinstance(
            value,
            list,
        ):
            # Process the list
            return [
                process_value(
                    handler=handler,
                    value=item,
                )
                for item in value
            ]
        # Check if the value is a primitive type
        elif isinstance(
            value,
            (
                int,
                float,
                bool,
            ),
        ):
            # Return the value as is
            return value

        # Call the handler if it is not None
        if handler is not None:
            # Call the handler
            return handler(value)

        if is_date(string=value):
            # Return the value as is
            return string_to_date(value=value)
        elif is_datetime(string=value):
            # Return the value as is
            return string_to_datetime(value=value)
        elif is_decimal(string=value):
            # Return the value as is
            return string_to_decimal(value=value)
        elif is_path(string=value):
            # Return the value as is
            return string_to_path(value=value)
        elif is_time(string=value):
            # Return the value as is
            return string_to_time(value=value)
        elif is_uuid(string=value):
            # Return the value as is
            return string_to_uuid(value=value)

        # Return the value as is
        return value

    # Load the JSON string into a dictionary
    result: dict[str, Any] = json.loads(json_string)

    # Process the dictionary
    for (
        key,
        value,
    ) in result.items():
        # Process the value
        result[key] = process_value(
            handler=handler,
            value=value,
        )

    # Return the processed dictionary
    return result


def is_date(string: str) -> bool:
    """
    Check if a string is a date.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a date, False otherwise.
    """

    try:
        # Attempt to convert the string to a date
        date.fromisoformat(string)

        # Return True if the string is a date
        return True
    except ValueError:
        # Return False if the string is not a date
        return False


def is_datetime(string: str) -> bool:
    """
    Check if a string is a datetime.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a datetime, False otherwise.
    """

    try:
        # Attempt to convert the string to a datetime
        datetime.fromisoformat(string)

        # Return True if the string is a datetime
        return True
    except ValueError:
        # Return False if the string is not a datetime
        return False


def is_decimal(string: str) -> bool:
    """
    Check if a string is a Decimal.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a Decimal, False otherwise.
    """

    try:
        # Attempt to convert the string to a Decimal
        Decimal(string)

        # Return True if the string is a Decimal
        return True
    except InvalidOperation:
        # Return False if the string is not a Decimal
        return False


def is_dict(string: str) -> bool:
    """
    Check if a string is a dictionary.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a dictionary, False otherwise.
    """

    try:
        # Attempt to convert the string to a dictionary
        obj: Any = json.loads(string)

        # Return True if the string is a dictionary
        return isinstance(obj, dict)
    except ValueError:
        # Return False if the string is not a dictionary
        return False


def is_list(string: str) -> bool:
    """
    Check if a string is a list.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a list, False otherwise.
    """

    try:
        # Attempt to convert the string to a list
        obj: Any = json.loads(string)

        # Return True if the string is a list
        return isinstance(obj, list)
    except ValueError:
        # Return False if the string is not a list
        return False


def is_path(string: str) -> bool:
    """
    Check if a string is a Path.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a Path, False otherwise.
    """

    # Attempt to convert the string to a Path
    path: Path = Path(string)

    try:
        # Return True if the string is a Path
        return path.resolve(strict=True).exists()
    except FileNotFoundError:
        # Return False if the string is not a Path
        return False


def is_set(string: str) -> bool:
    """
    Check if a string is a set.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a set, False otherwise.
    """

    try:
        # Attempt to convert the string to a set
        obj: Any = json.loads(string)

        # Return True if the string is a set
        return isinstance(obj, set)
    except ValueError:
        # Return False if the string is not a set
        return False


def is_stale(
    interval: int,
    timestamp: datetime,
) -> bool:
    """
    Check if a timestamp is stale.

    Args:
        interval (int): The interval in seconds.
        timestamp (datetime): The timestamp to check.

    Returns:
        bool: True if the timestamp is stale, False otherwise.
    """

    # Get the current time
    now: datetime = datetime.now()

    # Return True if the timestamp is stale
    return (now - timestamp).total_seconds() > interval


def is_string_quoted(string: str) -> bool:
    """
    Check if a string is quoted.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is quoted, False otherwise.
    """

    # Check if the string is in quotes
    return (
        string.startswith("'")
        and string.endswith("'")
        or string.startswith('"')
        and string.endswith('"')
    )


def is_time(string: str) -> bool:
    """
    Check if a string is a time.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a time, False otherwise.
    """

    try:
        # Attempt to convert the string to a time
        time.fromisoformat(string)

        # Return True if the string is a time
        return True
    except ValueError:
        # Return False if the string is not a time
        return False


def is_tuple(string: str) -> bool:
    """
    Check if a string is a tuple.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a tuple, False otherwise.
    """

    try:
        # Attempt to convert the string to a tuple
        obj: Any = json.loads(string)

        # Return True if the string is a tuple
        return isinstance(obj, tuple)
    except ValueError:
        # Return False if the string is not a tuple
        return False


def is_uuid(string: str) -> bool:
    """
    Check if a string is a UUID.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string is a UUID, False otherwise.
    """

    try:
        # Attempt to convert the string to a UUID
        UUID(string)

        # Return True if the string is a UUID
        return True
    except ValueError:
        # Return False if the string is not a UUID
        return False


def lock() -> asyncio.Lock:
    """
    Get or create a lock.

    Returns:
        asyncio.Lock: The lock.
    """

    # Declare the lock as global
    global LOCK

    # Check if the lock is None
    if not LOCK:
        # Create the lock
        LOCK = asyncio.Lock()

    # Return the lock
    return LOCK


def loop() -> asyncio.AbstractEventLoop:
    """
    Get or create an event loop.

    Returns:
        asyncio.AbstractEventLoop: The event loop.
    """

    # Declare the event loop as global
    global LOOP

    # Check if the event loop is None
    if not LOOP:
        try:
            # Create the event loop
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        except RuntimeError:
            # Create the event loop
            loop = asyncio.new_event_loop()

            # Set the event loop
            asyncio.set_event_loop(loop)

        # Store the event loop
        LOOP = loop

    # Return the event loop
    return LOOP


def match_pattern(
    pattern: re.Pattern,
    string: str,
) -> Optional[dict[str, Any]]:
    """
    Match a string against a pattern.

    Args:
        pattern (re.Pattern): The pattern to match.
        string (str): The string to match.

    Returns:
        Optional[dict[str, Any]]: The match object if the string matches the pattern, None otherwise.
    """

    # Attempt to match the string against the pattern
    match: Optional[re.Match] = pattern.match(string=string)

    # Check if the string does not match the pattern
    if match is None:
        # Return None if the string does not match the pattern
        return None

    # Return the match object
    return match.groupdict()


def object_to_string(
    value: Any,
    handler: Optional[Callable[[Any], str]] = None,
) -> str:
    """
    Convert an object to a string.

    Args:
        handler (Optional[Callable[[Any], str]]): The handler to use when converting the object.
        value (Any): The object to convert.

    Returns:
        str: The string representation of the object.
    """

    # Check if the handler is not None
    if handler is not None:
        # Call the handler
        return handler(value)

    # Return the string representation of the object
    return str(value)


def path_exists(path: Union[list[str], Path, str]) -> bool:
    """
    Check if a path exists.

    Args:
        path (Union[list[str], Path, str]): The path to check.

    Returns:
        bool: True if the path exists, False otherwise.
    """

    # Check if the path is a string or Path object
    if not isinstance(
        path,
        Path,
    ):
        # Convert the string to a Path object
        path = Path(path)

    try:
        # Return True if the path exists
        return path.resolve().exists()
    except FileNotFoundError:
        # Return False if the path does not exist
        return False


def path_to_string(value: Path) -> str:
    """
    Convert a Path to a string.

    Args:
        value (Path): The Path to convert.

    Returns:
        str: The string representation of the Path.
    """

    return value.as_posix()


def quote_string(string: str) -> str:
    """
    Quote a string.

    Args:
        string (str): The string to quote.

    Returns:
        str: The quoted string.
    """

    # Check if the string is already quoted
    if is_string_quoted(string):
        # Return the string as is
        return string

    # Return the quoted string
    return f"'{string}'"


async def read_file(
    path: Union[Path, str],
    encoding: str = "utf-8",
) -> str:
    """
    Read a file and return its contents.

    Args:
        encoding (str): The encoding to use when reading the file.
        path (Union[Path, str]): The path to the file to read.

    Returns:
        str: The contents of the file.
    """

    # Check if the file is a string or Path object
    if not isinstance(
        path,
        Path,
    ):
        # Convert the string to a Path object
        path = Path(path)

    # Check if the file exists
    if not path.exists():
        # Raise a PebbleFileNotFoundException if the file does not exist
        raise PebbleFileNotFoundException(path=path)

    try:
        # Acquire a lock to ensure thread safety
        async with lock():
            # Open the file asynchronously
            async with aiofiles.open(
                path.as_posix(),
                encoding=encoding,
                mode="r",
            ) as file:
                # Read the file contents
                content: str = await file.read()

                # Return the file contents
                return content or ""
    except Exception as e:
        # Re-raise the exception with a custom message
        raise PebbleFileReadError(path=path) from e


def run_asynchronously(
    function: Callable,
    *args,
    **kwargs,
) -> Any:
    """
    Run a function asynchronously.

    Args:
        function (Callable): The function to run.
        args (tuple): The arguments to pass to the function.
        kwargs (dict): The keyword arguments to pass to the function.

    Returns:
        Any: The result of the function.
    """

    if loop().is_running():
        # Run the function asynchronously
        return asyncio.run_coroutine_threadsafe(
            function(
                *args,
                **kwargs,
            ),
            loop(),
        ).result()
    else:
        # Run the function synchronously
        return loop().run_until_complete(
            function(
                *args,
                **kwargs,
            )
        )


def string_to_date(
    value: str,
    format: Optional[str] = None,
) -> date:
    """
    Convert a string to a date.

    Args:
        format (Optional[str]): The format to use when converting the string.
        value (str): The string to convert.

    Returns:
        date: The date representation of the string.
    """

    # Check if the format is None
    if format is None:
        # Return the date from the string
        return date.fromisoformat(value)

    # Return the date from the string
    return date.strptime(value, format)


def string_to_datetime(
    value: str,
    format: Optional[str] = None,
) -> datetime:
    """
    Convert a string to a datetime.

    Args:
        format (Optional[str]): The format to use when converting the string.
        value (str): The string to convert.

    Returns:
        datetime: The datetime representation of the string.
    """

    # Check if the format is None
    if format is None:
        # Return the datetime from the string
        return datetime.fromisoformat(value)

    # Return the datetime from the string
    return datetime.strptime(value, format)


def string_to_decimal(value: str) -> Decimal:
    """
    Convert a string to a Decimal.

    Args:
        value (str): The string to convert.

    Returns:
        Decimal: The Decimal representation of the string.
    """

    try:
        # Return the Decimal from the string
        return Decimal(value)
    except InvalidOperation as e:
        # Raise a ValueError
        raise ValueError(f"Invalid decimal value: {value}") from e


def string_to_dict(value: str) -> dict[str, Any]:
    """
    Convert a string to a dictionary.

    Args:
        value (str): The string to convert.

    Returns:
        dict[str, Any]: The dictionary representation of the string.
    """

    return json.loads(value)


def string_to_object(
    string: str,
    handler: Optional[Callable[[str], Any]] = None,
) -> Any:
    """
    Convert a string to an object.

    Args:
        handler (Optional[Callable[[str], Any]]): The handler to use when converting the string.
        string (str): The string to convert.

    Returns:
        Any: The object representation of the string.
    """

    # Check if the string is not a string
    if not isinstance(
        string,
        str,
    ):
        # Return the string as is
        return string

    # Check if the handler is not None
    if handler is not None:
        # Call the handler
        return handler(string)

    # Check if the string is numeric
    if string.isnumeric():
        # Return the string as an integer
        return int(string)
    # Check if the string is a decimal
    elif string.isdecimal():
        # Return the string as a float
        return float(string)
    # Check if the string is a boolean
    elif string.lower() in ["true", "false"]:
        # Return the string as a boolean
        return string.lower() == "true"
    # Check if the string is None
    elif string.lower() in ["none", "null"]:
        # Return the string as None
        return None
    # Check if the string is a float
    elif string.lower() in ["nan", "inf", "-inf"]:
        # Return the string as a float
        return float(string)
    # Check if the string is a date string
    if is_date(string=string):
        # Return the string as a date
        return string_to_date(value=string)
    # Check if the string is a datetime string
    elif is_datetime(string=string):
        # Return the string as a datetime
        return string_to_datetime(value=string)
    # Check if the string is a decimal string
    elif is_decimal(string=string):
        # Return the string as a Decimal
        return string_to_decimal(value=string)
    # Check if the string is a path string
    elif is_path(string=string):
        # Return the string as a Path
        return string_to_path(value=string)
    # Check if the string is a time string
    elif is_time(string=string):
        # Return the string as a time
        return string_to_time(value=string)
    # Check if the string is a UUID string
    elif is_uuid(string=string):
        # Return the string as a UUID
        return string_to_uuid(value=string)

    # Return the value as is
    return string


def string_to_time(
    value: str,
    format: Optional[str] = None,
) -> time:
    """
    Convert a string to a time.

    Args:
        format (Optional[str]): The format to use when converting the string.
        value (str): The string to convert.

    Returns:
        time: The time representation of the string.
    """

    # Check if the format is None
    if format is None:
        # Return the time from the string
        return time.fromisoformat(value)

    # Return the time from the string
    return time.strptime(value, format)


def string_to_path(value: str) -> Path:
    """
    Convert a string to a Path.

    Args:
        value (str): The string to convert.

    Returns:
        Path: The Path representation of the string.
    """

    try:
        # Return the Path representation of the string
        return Path(value).resolve(strict=True)
    except FileNotFoundError:
        pass


def string_to_uuid(value: str) -> UUID:
    """
    Convert a string to a UUID.

    Args:
        value (str): The string to convert.

    Returns:
        UUID: The UUID representation of the string.
    """

    # Return the UUID from the string
    return UUID(value)


def time_to_string(
    value: time,
    format: Optional[str] = None,
) -> str:
    """
    Convert a time to a string.

    Args:
        format (Optional[str]): The format to use when converting the time.
        value (time): The time to convert.

    Returns:
        str: The string representation of the time.
    """

    # Check if the format is None
    if format is None:
        # Return the ISO format of the time
        return value.isoformat()

    # Return the formatted time
    return value.strftime(format)


def unquote_string(string: str) -> str:
    """
    Remove the quotes from a string.

    Args:
        string (str): The string to remove the quotes from.

    Returns:
        str: The string with the quotes removed.
    """

    # Check if the string is not a string
    if not isinstance(
        string,
        str,
    ):
        # Return the string as is
        return string

    # Check if the string is in quotes
    if is_string_quoted(string):
        # Remove the quotes from the string
        return string[1:-1]

    # Return the string as is
    return string


def uuid_to_string(value: UUID) -> str:
    """
    Convert a UUID to a string.

    Args:
        value (UUID): The UUID to convert.

    Returns:
        str: The string representation of the UUID.
    """

    return str(value)


async def write_file(
    data: str,
    path: Union[Path, str],
) -> bool:
    """
    Write data to a file.

    Args:
        data (str): The data to write to the file.
        path (Union[Path, str]): The path to the file to write to.

    Returns:
        bool: True if the file was written, False otherwise.
    """

    try:
        # Acquire a lock to ensure thread safety
        async with lock():
            # Open the file asynchronously
            async with aiofiles.open(
                path.as_posix(),
                encoding="utf-8",
                mode="w",
            ) as file:
                # Write the data to the file
                await file.write(data)

                # Return True if the file was written
                return True
    except Exception as e:
        # Re-raise the exception with a custom message
        raise PebbleFileWriteError(path=path) from e
