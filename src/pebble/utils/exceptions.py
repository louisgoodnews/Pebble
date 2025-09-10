"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from pathlib import Path
from typing import Any, Final, List

from utils.constants import OBJECT_SIZE_LIMIT


__all__: Final[List[str]] = [
    "PebbleError",
    "PebbleFieldValidationError",
    "PebbleFileNotCreatedError",
    "PebbleFileNoDeletedError",
    "PebbleFileNotFoundException",
    "PebbleFileReadError",
    "PebbleFileWriteError",
    "PebbleFilterStringFormatError",
    "PebbleFilterStringOperatorError",
    "PebbleFilterStringScopeError",
    "PebbleQueryStringFormatError",
    "PebbleQueryStringOperatorError",
    "PebbleQueryStringScopeError",
    "PebbleRecordImmutabilityViolationError",
    "PebbleRecordMergeError",
    "PebbleSizeExceededError",
    "PebbleTableAlreadyRegisteredError",
    "PebbleTableAlreadyExistsError",
    "PebbleTableNotFoundError",
]


class PebbleError(Exception):
    """
    Custom exception class for any semi arbitrary exception thrown by the pebble library.
    """


class PebbleFieldValidationError(PebbleError):
    """
    Custom exception class for when a field is not valid.
    """


class PebbleFileNotCreatedError(PebbleError):
    """
    Custom exception class for when a file is not created.
    """

    def __init__(
        self,
        path: Path,
    ) -> None:
        """
        Initialize the exception with the path of the file that was not created.

        Args:
            path: The path of the file that was not created.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"File at path '{path}' was not created.")


class PebbleFileNoDeletedError(PebbleError):
    """
    Custom exception class for when a file is not deleted.
    """

    def __init__(
        self,
        path: Path,
    ) -> None:
        """
        Initialize the exception with the path of the file that was not deleted.

        Args:
            path: The path of the file that was not deleted.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"File at path '{path}' was not deleted.")


class PebbleFileNotFoundException(PebbleError):
    """
    Custom exception class for when a file is not found.
    """

    def __init__(
        self,
        path: Path,
    ) -> None:
        """
        Initialize the exception with the path of the file that was not found.

        Args:
            path: The path of the file that was not found.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"File at path '{path}' does not exist.")


class PebbleFileReadError(PebbleError):
    """
    Custom exception class for when a file is not read.
    """

    def __init__(
        self,
        path: Path,
    ) -> None:
        """
        Initialize the exception with the path of the file that was not read.

        Args:
            path: The path of the file that was not read.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"File at path '{path}' could not be read.")


class PebbleFileWriteError(PebbleError):
    """
    Custom exception class for when a file is not written.
    """

    def __init__(
        self,
        path: Path,
    ) -> None:
        """
        Initialize the exception with the path of the file that was not written.

        Args:
            path: The path of the file that was not written.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"File at path '{path}' could not be written.")


class PebbleFilterStringOperatorError(PebbleError):
    """
    Custom exception class for when a filter string operator is not valid.
    """

    def __init__(
        self,
        operator: str,
    ) -> None:
        """
        Initialize the exception with the operator that is not valid.

        Args:
            operator: The operator that is not valid.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"Filter string operator '{operator}' is not valid. Must be ['==', '!=', '<', '>', '<=', '>=', 'in', 'not in', 'is', 'is not']"
        )


class PebbleFilterStringScopeError(PebbleError):
    """
    Custom exception class for when a filter string scope is not valid.
    """

    def __init__(
        self,
        scope: str,
    ) -> None:
        """
        Initialize the exception with the scope that is not valid.

        Args:
            scope: The scope that is not valid.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"Filter string scope '{scope}' is not valid. Must be ['*', 'any', 'all' or 'none']"
        )


class PebbleFilterStringFormatError(PebbleError):
    """
    Custom exception class for when a filter string is not in the correct format.
    """

    def __init__(
        self,
        string: str,
    ) -> None:
        """
        Initialize the exception with the string that is not in the correct format.

        Args:
            string: The string that is not in the correct format.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"Filter string '{string}' is not in the correct format.")


class PebbleQueryStringOperatorError(PebbleError):
    """
    Custom exception class for when a query string operator is not valid.
    """

    def __init__(
        self,
        operator: str,
    ) -> None:
        """
        Initialize the exception with the operator that is not valid.

        Args:
            operator: The operator that is not valid.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"Query string operator '{operator}' is not valid. Must be ['==', '!=', '<', '>', '<=', '>=', 'in', 'not in', 'is', 'is not']"
        )


class PebbleQueryStringScopeError(PebbleError):
    """
    Custom exception class for when a query string scope is not valid.
    """

    def __init__(
        self,
        scope: str,
    ) -> None:
        """
        Initialize the exception with the scope that is not valid.

        Args:
            scope: The scope that is not valid.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"Query string scope '{scope}' is not valid. Must be ['*', 'any', 'all' or 'none']"
        )


class PebbleQueryStringFormatError(PebbleError):
    """
    Custom exception class for when a query string is not in the correct format.
    """

    def __init__(
        self,
        string: str,
    ) -> None:
        """
        Initialize the exception with the string that is not in the correct format.

        Args:
            string: The string that is not in the correct format.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"Query string '{string}' is not in the correct format.")


class PebbleRecordImmutabilityViolationError(PebbleError):
    """
    Custom exception class for when a record is immutable.
    """

    def __init__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Initialize the exception with the record that is immutable.

        Args:
            key: The key that is immutable.
            value: The value that is immutable.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"Cannot update '{key}' with value '{value}'. Object of class 'PebbleRecord' is immutable."
        )


class PebbleRecordMergeError(PebbleError):
    """
    Custom exception class for when a record is merged.
    """

    def __init__(
        self,
        record1: "PebbleRecord",
        record2: "PebbleRecord",
    ) -> None:
        """
        Initialize the exception with the records that are merged.

        Args:
            record1: The first record that is merged.
            record2: The second record that is merged.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"Failed to merge records {record1} and {record2}.")


class PebbleSizeExceededError(PebbleError):
    """
    Custom exception class for when a size is exceeded.
    """

    def __init__(
        self,
        name: str,
    ) -> None:
        """
        Initialize the exception with the size that is exceeded.

        Args:
            name: The name of the object that is exceeded.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"The pebble {name} exceeds the maxmum size of {OBJECT_SIZE_LIMIT} entries. Please consider splitting the {name} pebble into multiple pebbles."
        )


class PebbleTableAlreadyExistsError(PebbleError):
    """
    Custom exception class for when a table already exists.
    """

    def __init__(
        self,
        table: str,
    ) -> None:
        """
        Initialize the exception with the name of the table that already exists.

        Args:
            table: The name of the table that already exists.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"Table {table} already exists.")


class PebbleTableAlreadyRegisteredError(PebbleError):
    """
    Custom exception class for when a table is already registered.
    """

    def __init__(
        self,
        table: str,
    ) -> None:
        """
        Initialize the exception with the name of the table that is already registered.

        Args:
            table: The name of the table that is already registered.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(
            f"Table {table} is already registered. Cannot register a table with the same name."
        )


class PebbleTableNotFoundError(PebbleError):
    """
    Custom exception class for when a table is not found.
    """

    def __init__(
        self,
        table: str,
    ) -> None:
        """
        Initialize the exception with the name of the table that was not found.

        Args:
            table: The name of the table that was not found.

        Returns:
            None
        """

        # Call the parent class constructor
        super().__init__(f"Table {table} does not exist.")
