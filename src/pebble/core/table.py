"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from pathlib import Path
from typing import Any, Final, List, Optional, Union

from core.cache import PebbleCache
from core.exceptions import PebbleSizeExceededError
from core.tools import PebbleToolBuilder

from utils.constants import OBJECT_SIZE_LIMIT
from utils.utils import (
    convert_to_path,
    dict_to_json,
    json_to_dict,
    read_file,
    run_asynchronously,
    write_file,
)

__all__: Final[List[str]] = [
    "PebbleTable",
    "PebbleTableFactory",
    "PebbleTableBuilder",
]


class PebbleTable:
    """
    A table in a database.
    """

    def __init__(
        self,
        name: str,
        constraints: Optional[dict[str, Any]] = None,
        definition: Optional[dict[str, Any]] = None,
        entries: Optional[dict[str, Any]] = None,
        fields: Optional[dict[str, Any]] = None,
        flush_interval: int = 300,
        identifier: Optional[str] = None,
        indexes: Optional[list[str]] = None,
        primary_key: Optional[str] = None,
        references: Optional[dict[str, Any]] = None,
        required: Optional[list[str]] = None,
        unique: Optional[list[str]] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Initialize a new PebbleTable object.

        Args:
            constraints (Optional[dict[str, Any]]): The dictionary to store the constraints of the table.
            definition (Optional[dict[str, Any]]): The dictionary to store the definition of the table.
            entries (Optional[dict[str, Any]]): The dictionary to store the table data in.
            fields (Optional[dict[str, Any]]): The dictionary to store the fields of the table.
            flush_interval (int): The flush interval in seconds.
            identifier (Optional[str]): The identifier of the table.
            indexes (Optional[list[str]]): The list to store the indexes of the table.
            name (str): The name of the table.
            path (Optional[Union[Path, str]]): The path to the table file.
            primary_key (Optional[str]): The name of the primary key of the table.
            references (Optional[dict[str, Any]]): The dictionary to store the references of the table.
            required (Optional[list[str]]): The list of required fields.
            unique (Optional[list[str]]): The list of unique fields.

        Returns:
            None
        """

        # Initialize the cache as an instance variable
        self._cache: Final[PebbleCache] = PebbleCache()

        # Check if the constraints is None
        if constraints is None:
            # Initialize an empty constraints
            constraints = {}

        # Store the passed constraints in an instance variable
        self._constraints: dict[str, Any] = constraints

        # Check if the definition is None
        if definition is None:
            # Initialize an empty definition
            definition = {
                "constraints": {},
                "fields": {},
                "indexes": [],
                "primary_key": "",
                "references": {},
                "required": [],
                "unique": [],
            }

        # Store the passed definition in an instance variable
        self._definition: dict[str, Any] = definition

        # Initialize the dirty flag as an instance variable
        self._dirty: bool = False

        # Store the passed engine in an instance variable
        self._engine: Optional[PebbleFilterEngine] = None

        # Check if the entries is None
        if entries is None:
            # Initialize an empty entries
            entries = {
                "total": 0,
                "values": {},
            }

        # Store the passed entries in an instance variable
        self._entries: dict[str, Any] = entries

        # Check if the fields is None
        if fields is None:
            # Initialize an empty fields
            fields = {}

        # Store the passed fields in an instance variable
        self._fields: dict[str, Any] = fields

        # Store the passed flush interval in an instance variable
        self._flush_interval: Final[int] = flush_interval

        # Check if the identifier is None
        if identifier is None:
            # Initialize an empty identifier
            identifier = uuid.uuid4().hex

        # Store the passed identifier in an instance variable
        self._identifier: Final[str] = identifier

        # Check if the indexes is None
        if indexes is None:
            # Initialize an empty indexes
            indexes = []

        # Store the passed indexes in an instance variable
        self._indexes: list[str] = indexes

        # Initialize the last flushed at as an instance variable
        self._last_flushed_at: Optional[datetime] = None

        # Store the passed name string in an instance variable
        self._name: str = name

        # Check if the path is a string
        if path is None or not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = convert_to_path(path=path)

        # Store the passed path in an instance variable
        self._path: Path = path

        # Check if the primary_key is None
        if primary_key is None:
            # Initialize an empty primary_key
            primary_key = ""

        # Store the passed primary_key in an instance variable
        self._primary_key: str = primary_key

        # Check if the references is None
        if references is None:
            # Initialize an empty references
            references = {}

        # Store the passed references in an instance variable
        self._references: dict[str, Any] = references

        # Check if the required is None
        if required is None:
            # Initialize an empty required
            required = []

        # Store the passed required in an instance variable
        self._required: list[str] = required

        # Check if the unique is None
        if unique is None:
            # Initialize an empty unique
            unique = []

        # Store the passed unique in an instance variable
        self._unique: list[str] = unique

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the table contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the table contains the key, False otherwise.
        """

        # Initialize an empty list to store the values
        values: list[Any] = []

        # Check if the key is in the dictionary
        values.extend([values for values in self.values.values()])

        # Return True if the key is in the dictionary
        return key in values

    def __eq__(
        self,
        other: object,
    ) -> bool:
        """
        Check if the table is equal to another table.

        Args:
            other: The other table to compare with.

        Returns:
            True if the tables are equal, False otherwise.
        """

        # Check if the other object is a PebbleTable object
        if not isinstance(
            other,
            PebbleTable,
        ):
            # Return False if the other object is not a PebbleTable object
            return False

        # Return True if the tables are equal
        return self._identifier == other._identifier

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get an item from the table itself or its data.

        This method is used to access the table object's data.
        It can also be used to access the table object's instance variables.

        Args:
            key: The key of the item.

        Returns:
            The item.
        """

        # Return the value from the dictionary
        return self._entries.get(key)

    def __len__(self) -> int:
        """
        Get the length of the table.

        Returns:
            The length of the table.
        """

        # Return the length of the dictionary
        return len(self._entries)

    def __repr__(self) -> str:
        """
        Get a string representation of the table.

        Returns:
            A string representation of the table.
        """

        # Return the string representation of the table
        return f"<{self.__class__.__name__}(cache=[{self._cache}], definition=[{self._definition}], entries=[{self._entries['total']} {'entry' if self._entries['total'] == 1 else 'entries'}], identifier=[{self._identifier}], name=[{self._name}], path=[{self._path}])>"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set an item in the table itself or its data.

        This method is used to set the table object's data.
        It can also be used to set the table object's instance variables.

        Args:
            key: The key of the item.
            value: The value of the item.

        Returns:
            None
        """

        # Check if the key is the identifier
        if key == "identifier":
            # Raise an AttributeError if the identifier is set
            raise AttributeError(
                f"Cannot set 'identifier' of a {self.__class__.__name__} object. As this attribute is immutable."
            )

        # Set the value in the dictionary
        self._entries[key] = value

    def __str__(self) -> str:
        """
        Get a string representation of the table.

        Returns:
            A string representation of the table.
        """

        # Return the string representation of the table
        return str(self._entries)

    @property
    def cache(self) -> PebbleCache:
        """
        Get the cache of the table.

        Returns:
            PebbleCache: The cache of the table.
        """

        return self._cache

    @property
    def constraints(self) -> dict[str, Any]:
        """
        Get the constraints of the table.

        Returns:
            dict[str, Any]: The constraints of the table.
        """

        return dict(self._constraints)

    @property
    def definition(self) -> dict[str, Any]:
        """
        Get the definition of the table.

        Returns:
            dict[str, Any]: The definition of the table.
        """

        return {
            "constraints": self._constraints,
            "fields": self._fields,
            "indexes": self._indexes,
            "primary_key": self._primary_key,
            "references": self._references,
            "required": self._required,
            "unique": self._unique,
        }

    @property
    def dirty(self) -> bool:
        """
        Get the dirty flag of the table.

        Returns:
            bool: The dirty flag of the table.
        """

        return self._dirty

    @dirty.setter
    def dirty(
        self,
        value: bool,
    ) -> None:
        """
        Set the dirty flag of the table.

        Args:
            value: The dirty flag of the table.

        Returns:
            None
        """

        self._dirty = value

    @property
    def entries(self) -> dict[str, Any]:
        """
        Get the entries of the table.

        Returns:
            dict[str, Any]: The entries of the table.
        """

        # Return the entries of the table
        return dict(self._entries)

    @property
    def fields(self) -> dict[str, Any]:
        """
        Get the fields of the table.

        Returns:
            dict[str, Any]: The fields of the table.
        """

        # Return the fields of the table
        return dict(self._fields)

    @property
    def flush_interval(self) -> int:
        """
        Get the flush interval of the table.

        Returns:
            int: The flush interval of the table.
        """

        return self._flush_interval

    @property
    def identifier(self) -> str:
        """
        Get the identifier of the table.

        Returns:
            str: The identifier of the table.
        """

        return self._identifier

    @property
    def indexes(self) -> list[str]:
        """
        Get the indexes of the table.

        Returns:
            list[str]: The indexes of the table.
        """

        # Return the indexes of the table
        return list(self._indexes)

    @property
    def last_flushed_at(self) -> Optional[datetime]:
        """
        Get the last flushed at of the table.

        Returns:
            Optional[datetime]: The last flushed at of the table.
        """

        return self._last_flushed_at

    @last_flushed_at.setter
    def last_flushed_at(
        self,
        value: Optional[datetime],
    ) -> None:
        """
        Set the last flushed at of the table.

        Args:
            value: The last flushed at of the table.

        Returns:
            None
        """

        self._last_flushed_at = value

    @property
    def name(self) -> str:
        """
        Get the name of the table.

        Returns:
            str: The name of the table.
        """

        return self._name

    @name.setter
    def name(
        self,
        value: str,
    ) -> None:
        """
        Set the name of the table.

        Args:
            value: The name of the table.

        Returns:
            None
        """

        # Update the instance variable with the passed value
        self._name = value

    @property
    def path(self) -> Path:
        """
        Get the path of the table.

        Returns:
            Path: The path of the table.
        """

        return self._path

    @path.setter
    def path(
        self,
        value: Union[Path, str],
    ) -> None:
        """
        Set the path of the table.

        Args:
            value: The path of the table.

        Returns:
            None
        """

        # Check if the value is a string
        if not isinstance(
            value,
            Path,
        ):
            # Convert the string to a Path object
            value = Path(value)

        # Update the instance variable with the passed value
        self._path = value

    @property
    def primary_key(self) -> str:
        """
        Get the primary key of the table.

        Returns:
            str: The primary key of the table.
        """

        return self._primary_key

    @property
    def references(self) -> dict[str, Any]:
        """
        Get the references of the table.

        Returns:
            dict[str, Any]: The references of the table.
        """

        # Return the references of the table
        return dict(self._references)

    @property
    def required(self) -> list[str]:
        """
        Get the required fields of the table.

        Returns:
            list[str]: The required fields of the table.
        """

        # Return the required fields of the table
        return list(self._required)

    @property
    def total(self) -> int:
        """
        Get the total number of entries in the table.

        Returns:
            int: The total number of entries in the table.
        """

        # Check if the total is in the dictionary
        if "total" not in self._entries:
            # Initialize the total
            self._entries["total"] = 0

        # Return the total number of entries
        return self._entries["total"]

    @total.setter
    def total(
        self,
        value: int,
    ) -> None:
        """
        Set the total number of entries in the table.

        Args:
            value: The total number of entries in the table.

        Returns:
            None
        """

        # Check if the total is in the dictionary
        if "total" not in self._entries:
            # Initialize the total
            self._entries["total"] = 0

        # Update the total count in the entries dictionary
        self._entries["total"] = value

    @property
    def unique(self) -> list[str]:
        """
        Get the unique constraints of the table.

        Returns:
            list[str]: The unique constraints of the table.
        """

        # Return the unique constraints of the table
        return list(self._unique)

    @property
    def values(self) -> dict[str, Any]:
        """
        Get the values of the table.

        Returns:
            dict[str, Any]: The values of the table.
        """

        if "values" not in self._entries:
            # Initialize the values
            self._entries["values"] = {}

        # Return the values of the table
        return self._entries["values"].copy()

    def all(
        self,
        format: Literal[
            "dict",
            "list",
            "set",
            "tuple",
            "json",
        ] = "dict",
    ) -> Union[dict[str, Any], list[dict[str, Any]], set[dict[str, Any]], tuple[dict[str, Any]]]:
        """
        Get all the data in the table.

        This method will return all the data in the table.
        It can do so in different formats.
        Supported formats are:
            - dict
            - list
            - set
            - tuple
            - json

        Args:
            format (Literal["dict", "list", "set", "tuple", "json"]): The format of the data.

        Returns:
            Union[dict[str, Any], list[dict[str, Any]], set[dict[str, Any]], tuple[dict[str, Any]]]: The data in the table.
        """

        # Check if the format is supported
        if format not in {
            "dict",
            "list",
            "set",
            "tuple",
            "json",
        }:
            # Raise a ValueError
            raise ValueError(f"Unsupported format: {format}")

        # Get a copy of the table's entries
        result: dict[str, Any] = dict(self.values)

        # Check if the format is dict
        if format == "dict":
            # Return the result
            return result
        # Check if the format is list
        elif format == "list":
            # Return a list of the table's entries
            return list(result.values())
        # Check if the format is set
        elif format == "set":
            # Return a set of the table's entries
            return set(result.values())
        # Check if the format is tuple
        elif format == "tuple":
            # Return a tuple of the table's entries
            return tuple(result.values())
        # Check if the format is json
        elif format == "json":
            # Return the result as a json string
            return dict_to_json(result)

    def bulk_remove(
        self,
        identifiers: list[int],
    ) -> bool:
        """
        Delete multiple entries from the table.

        Args:
            identifiers (list[int]): The identifiers of the entries to delete.

        Returns:
            bool: True if all entries were deleted, False otherwise.
        """

        # Initialize an empty list of identifiers
        result: list[bool] = []

        # Iterate over the identifiers
        for identifier in identifiers:
            # Delete the entry
            result.append(self.remove(identifier=identifier))

        # Return the identifiers of the entries
        return all(result)

    def bulk_set(
        self,
        entries: list[dict[str, Any]],
        identifiers: Optional[list[int]] = None,
    ) -> list[int]:
        """
        Set multiple entries in the table.

        Args:
            entries (list[dict[str, Any]]): The entries to set.
            identifiers (list[int]): The identifiers of the entries.

        Returns:
            list[int]: The identifiers of the entries.
        """

        # Initialize an empty list of identifiers
        result: list[int] = []

        # Check if the identifiers are None
        if identifiers is None:
            # Set the identifiers to the length of the entries
            identifiers = [None for _ in range(len(entries))]

        # Iterate over the entries
        for (
            entry,
            identifier,
        ) in zip(
            *[
                entries,
                identifiers,
            ]
        ):
            # Set the entry
            result.append(
                self.set(
                    entry=entry,
                    identifier=identifier,
                )
            )

        # Return the identifiers of the entries
        return result

    def check_for_size(self) -> bool:
        """
        Check if the table exceeds the maximum size.

        Returns:
            bool: True if the table exceeds the maximum size, False otherwise.

        Raises:
            PebbleSizeExceededError: If the table exceeds the maximum size.
        """

        # Check if the table exceeds the maximum size
        if self.get_size_of() > OBJECT_SIZE_LIMIT:
            # Raise a PebbleSizeExceededError
            raise PebbleSizeExceededError(name=self._name)

        # Return False if the table does not exceed the maximum size
        return False

    def commit(self) -> None:
        """
        Commit the table to a file.

        This method will commit the table to a file.

        Returns:
            None
        """

        # Check if the file exists
        if not self.path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=self.path,
            )

        # Write the dictionary to the file
        run_asynchronously(
            function=write_file,
            path=self.path,
            data=dict_to_json(dictionary=self.to_dict()),
        )

        # Mark the table as clean
        self.mark_as_clean()

        # Update the last flushed at
        self._last_flushed_at = datetime.now()

    def configure(
        self,
        path: str,
        value: Any,
    ) -> None:
        """
        Configure the table.

        This method will configure the table.

        Args:
            path: The path to the value.
            value: The value to set.

        Returns:
            None
        """

        # Check if the path does not start with "constraints" or "definition"
        if not path.startswith("constraints") and not path.startswith("definition"):
            # Raise a ValueError
            raise ValueError(
                "Cannot configure non-constraints or non-definition values with 'configure' method. PebbleTable object is otherwise immutable."
            )

        # Create a builder
        builder: PebbleToolBuilder = PebbleToolBuilder(dictionary=self.to_dict())

        # Set the value
        builder.set(path=path, value=value)

        # Update the table object
        self.__dict__.update(builder.build().to_dict())

        # Mark the table as dirty
        self.mark_as_dirty()

    def delete(self) -> None:
        """
        Delete the table file.

        Returns:
            None
        """

        # Check if the file exists
        if not self._path.exists():
            # Return if the file does not exist
            return

        # Delete the table file
        delete_file(path=self.path)

        # Delete the table object
        del self

    def empty(self) -> bool:
        """
        Check if the table is empty.

        This method will check if the table is empty.
        It is a convenience wrapper for the 'has_entries' method.

        Returns:
            bool: True if the table is empty, False otherwise.
        """

        return not self.has_entries()

    def engine(self) -> PebbleFilterEngine:
        """
        Get the filter engine.

        Returns:
            The filter engine.
        """

        # Check if the engine is None
        if self._engine is None:
            # Initialize the engine with this table
            self._engine = PebbleFilterEngine(table=self)

        # Return the engine
        return self._engine

    def filter(self) -> dict[str, Any]:
        """
        Filter the table.

        Returns:
            The filtered table.

        Raises:
            PebbleFilterEngineNotInitializedError: If the filter engine is not initialized.
        """

        # Check if the engine is None
        if self._engine is None:
            # Raise a PebbleFilterEngineNotInitializedError if the engine is None
            raise PebbleFilterEngineNotInitializedError("The filter engine is not initialized.")

        # Return the filtered table
        return self._engine.filter()

    @classmethod
    def from_file(
        cls,
        path: Union[Path, str],
    ) -> "PebbleTable":
        """
        Create a new PebbleTable object from a file.

        Args:
            path: The path to the table file.

        Returns:
            A new PebbleTable object.
        """

        # Check if the path is a string
        if not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path)

        # Check if the file exists
        if not path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=path,
            )

        json: dict[str, Any] = json_to_dict(
            run_asynchronously(
                function=read_file,
                path=path,
            )
        )

        # Return the table
        return PebbleTable(
            definition=json.get("definition", {}),
            entries=json.get("entries", {}),
            identifier=json.get("identifier", ""),
            name=json.get("name", ""),
            path=json.get("path", ""),
        )

    @classmethod
    def from_dict(
        cls,
        dictionary: dict[str, Any],
        name: str,
    ) -> "PebbleTable":
        """
        Create a new PebbleTable object from a dictionary.

        Args:
            dictionary: The dictionary to create the table from.
            name: The name of the table.

        Returns:
            A new PebbleTable object.
        """

        # Return the table
        return PebbleTable(
            definition=dictionary,
            name=name,
        )

    def get_by_id(
        self,
        identifier: int,
    ) -> Any:
        """
        Get an item from the table by its identifier.

        Args:
            identifier: The identifier of the item.

        Returns:
            The item.
        """

        # Return the item
        return self.values.get(
            str(identifier),
            None,
        )

    def get_by_ids(
        self,
        identifiers: list[int],
    ) -> dict[int, Any]:
        """
        Get items from the table by their identifiers.

        Args:
            identifiers: The identifiers of the items.

        Returns:
            The items.
        """

        # Return the items
        return {
            identifier: self.values.get(
                str(identifier),
                None,
            )
            for identifier in identifiers
        }

    def get_size_of(self) -> int:
        """
        Get the size of the table.

        Returns:
            int: The size of the table.
        """

        # Return the size of the table
        return self.total

    def has_definition(self) -> bool:
        """
        Check if the table has a definition.

        Returns:
            bool: True if the table has a definition, False otherwise.
        """

        return len(self.definition) != 0

    def has_entries(self) -> bool:
        """
        Check if the table has entries.

        Returns:
            bool: True if the table has entries, False otherwise.
        """

        return self.total != 0

    def has_primary_key(self) -> bool:
        """
        Check if the table has a primary key.

        Returns:
            bool: True if the table has a primary key, False otherwise.
        """

        return len(self.primary_key) != 0

    def has_references(self) -> bool:
        """
        Check if the table has references.

        Returns:
            bool: True if the table has references, False otherwise.
        """

        return len(self.references) != 0

    def has_requireds(self) -> bool:
        """
        Check if the table has required fields.

        Returns:
            bool: True if the table has required fields, False otherwise.
        """

        return len(self.required) != 0

    def has_uniques(self) -> bool:
        """
        Check if the table has unique fields.

        Returns:
            bool: True if the table has unique fields, False otherwise.
        """

        return len(self.unique) != 0

    def is_empty(self) -> bool:
        """
        Check if the table is empty.

        Returns:
            bool: True if the table is empty, False otherwise.
        """

        return self.total == 0

    def mark_as_clean(self) -> None:
        """
        Mark the table as clean.

        Returns:
            None
        """

        # Set the dirty flag to False
        self._dirty = False

    def mark_as_dirty(self) -> None:
        """
        Mark the table as dirty.

        Returns:
            None
        """

        # Set the dirty flag to True
        self._dirty = True

    def remove(
        self,
        identifier: Union[int, str],
    ) -> bool:
        """
        Delete an entry from the table.

        Args:
            identifier (Union[int, str]): The identifier of the entry to delete.

        Returns:
            bool: True if the entry was deleted, False otherwise.
        """

        # Check if the identifier is an integer
        if not isinstance(
            identifier,
            int,
        ):
            # Convert the identifier to a string
            identifier = str(identifier)

        # Check if the identifier exists
        if identifier not in self.values:
            # Return False if the identifier does not exist
            return False

        # Delete the identifier
        self.values.pop(identifier)

        # Mark the table as dirty
        self.mark_as_dirty()

        # Return True if the identifier was deleted
        return True

    def set(
        self,
        entry: dict[str, Any],
        identifier: Optional[str] = None,
    ) -> int:
        """
        Set an item in the table.

        Args:
            entry (dict[str, Any]): The entry to set.
            identifier (Optional[str]): The identifier of the entry.

        Returns:
            int: The identifier of the entry.
        """

        # Check if the total is in the dictionary
        if "total" not in self.entries:
            # Set the total to 0
            self.total = 0

        # Check if the values is in the dictionary
        if "values" not in self.entries:
            # Set the values to an empty dictionary
            self.entries["values"] = {}

        # Check if the identifier is None
        if identifier is None:
            # Generate a new identifier
            identifier = str(len(self.entries["values"]))

            # Increment the total
            self.total = int(identifier) + 1

        # Set the entry in the dictionary
        self.entries["values"][identifier] = entry

        # Mark the table as dirty
        self.mark_as_dirty()

        # Return the identifier of the entry
        return identifier

    def set_filter(
        self,
        string: str,
        flag: Literal[
            "ALL",
            "ANY",
            "NONE",
        ] = "ALL",
    ) -> Self:
        """
        Set a filter for the table.

        Args:
            flag (Literal["ALL", "ANY", "NONE"]): The flag to use for the filter.
            string (str): The string to filter the table by.

        Returns:
            Self: The table.

        Raises:
            PebbleFilterEngineNotInitializedError: If the filter engine is not initialized.
        """

        # Check if the engine is None
        if not self._engine:
            # Initialize the engine
            raise PebbleFilterEngineNotInitializedError("The filter engine is not initialized.")

        # Set the filter
        self._engine.set_filter(
            filter=PebbleFilterString(
                flag=flag,
                string=string,
            )
        )

        # Return the table
        return self

    def set_filters(
        self,
        strings: list[str],
        flag: Literal[
            "ALL",
            "ANY",
            "NONE",
        ] = "ALL",
    ) -> Self:
        """
        Set filters for the table.

        Args:
            flag (Literal["ALL", "ANY", "NONE"]): The flag to use for the filters.
            strings (list[str]): The strings to filter the table by.

        Returns:
            Self: The table.
        """

        # Check if the engine is None
        if not self._engine:
            # Initialize the engine
            raise PebbleFilterEngineNotInitializedError("The filter engine is not initialized.")

        # Set the filters
        self._engine.set_filters(
            filters=[
                PebbleFilterString(
                    flag=flag,
                    string=string,
                )
                for string in strings
            ]
        )

        # Return the table
        return self

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the table to a dictionary.

        Returns:
            dict[str, Any]: The table as a dictionary.
        """

        # Return the table as a dictionary
        return {
            "definition": {
                "constraints": self._constraints,
                "fields": self._fields,
                "indexes": self._indexes,
                "primary_key": self._primary_key,
                "references": self._references,
                "unique": self._unique,
            },
            "entries": {
                "total": self.total,
                "values": self.values,
            },
            "identifier": self._identifier,
            "name": self._name,
            "path": self._path,
        }

    def to_json(self) -> str:
        """
        Convert the table to a JSON string.

        Returns:
            str: The table as a JSON string.
        """

        # Return the table as a JSON string
        return dict_to_json(dictionary=self.to_dict())

    def to_str(self) -> str:
        """
        Convert the table to a string.

        Returns:
            str: The table as a string.
        """

        # Return the table as a string
        return str(self.entries)


class PebbleTableFactory:
    """
    A factory class for creating new PebbleTable objects.
    """

    @classmethod
    def create(
        cls,
        name: str,
        definition: Optional[dict[str, Any]] = None,
        entries: Optional[dict[str, Any]] = None,
        identifier: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> PebbleTable:
        """
        Create a new PebbleTable object.

        Args:
            name: The name of the table.
            definition: The dictionary to store the definition of the table.
            entries: The dictionary to store the table data in.
            identifier: The identifier of the table.
            path: The path to the table file.

        Returns:
            A new PebbleTable object.
        """

        # Return the table
        return PebbleTable(
            name=name,
            definition=definition,
            entries=entries,
            identifier=identifier,
            path=path,
        )


class PebbleTableBuilder:
    """
    Build a new PebbleTable object.
    """

    def __init__(self) -> None:
        """
        Initialize a new PebbleTableBuilder object.

        Returns:
            None
        """

        # Store the configuration in an instance variable
        self._configuration: dict[str, Any] = {
            "definition": {
                "constraints": {},
                "fields": {},
                "indexes": [],
                "primary_key": "",
                "references": {},
                "unique": [],
            },
            "entries": {
                "total": 0,
                "values": {},
            },
            "identifier": "",
            "name": "",
            "path": "",
        }

    @property
    def configuration(self) -> dict[str, Any]:
        """
        Get the configuration.

        Returns:
            dict[str, Any]: The configuration.
        """

        return dict(self._configuration)

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the configuration contains a key.

        Args:
            key (str): The key to check for.

        Returns:
            bool: True if the configuration contains the key, False otherwise.
        """

        # Return the presence of the key in the configuration
        return key in self._configuration

    def __eq__(
        self,
        other: "PebbleTableBuilder",
    ) -> bool:
        """
        Check if the builder is equal to another builder.

        Args:
            other (PebbleTableBuilder): The other builder to compare to.

        Returns:
            bool: True if the builders are equal, False otherwise.
        """

        # Check if the other object is a PebbleTableBuilder
        if not isinstance(
            other,
            PebbleTableBuilder,
        ):
            # Return False
            return False

        # Return the equality of the builders
        return self._configuration == other._configuration

    def __repr__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            str: The string representation of the builder.
        """

        # Return the string representation of the builder
        return f"PebbleTableBuilder(configuration={self._configuration})"

    def __str__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            str: The string representation of the builder.
        """

        # Return the string representation of the builder
        return str(self._configuration)

    def build(self) -> PebbleTable:
        """
        Build the table.

        Returns:
            PebbleTable: The table.
        """

        # Create and return the table
        return PebbleTableFactory.create(**self._configuration)

    def with_constraint(
        self,
        value: PebbleConstraint,
    ) -> Self:
        """
        Set the constraint.

        Args:
            value (PebbleConstraint): The constraint to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"constraints": {}}

        # Set the constraint
        self._configuration["definition"]["constraints"][value.name] = value.to_dict()

        # Return the builder
        return self

    def with_definition(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the definition.

        Args:
            value (dict[str, Any]): The definition to set.

        Returns:
            Self: The builder.
        """

        # Set the definition
        self._configuration["definition"] = value

        # Return the builder
        return self

    def with_dictionary(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the dictionary.

        Args:
            value (dict[str, Any]): The dictionary to set.

        Returns:
            Self: The builder.
        """

        # Set the dictionary
        self._configuration["dictionary"] = value

        # Return the builder
        return self

    def with_field(
        self,
        value: PebbleField,
    ) -> Self:
        """
        Set the field.

        Args:
            value (PebbleField): The field to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"fields": {}}

        # Set the field
        self._configuration["definition"]["fields"][value.name] = value.to_dict()

        # Return the builder
        return self

    def with_identifier(
        self,
        value: Optional[str] = None,
    ) -> Self:
        """
        Set the identifier.

        Args:
            value (str): The identifier to set.

        Returns:
            Self: The builder.
        """

        # Generate a random UUID if the value is None
        if value is None:
            # Generate a random UUID
            value = uuid.uuid4().hex

        # Check if the value is a valid UUID
        if not is_uuid(string=value):
            # Raise a ValueError
            raise ValueError("Invalid UUID identifier")

        # Set the identifier
        self._configuration["identifier"] = value

        # Return the builder
        return self

    def with_index(
        self,
        value: str,
    ) -> Self:
        """
        Set the index.

        Args:
            value (str): The index to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"indexes": []}

        # Set the index
        self._configuration["definition"]["indexes"].append(value)

        # Return the builder
        return self

    def with_indexes(
        self,
        value: list[str],
    ) -> Self:
        """
        Set the indexes.

        Args:
            value (list[str]): The indexes to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"indexes": []}

        # Set the indexes
        for index in value:
            # Set the index
            self._configuration["definition"]["indexes"].append(index)

        # Return the builder
        return self

    def with_name(
        self,
        value: str,
    ) -> Self:
        """
        Set the name.

        Args:
            value (str): The name to set.

        Returns:
            Self: The builder.
        """

        # Set the name
        self._configuration["name"] = value

        # Return the builder
        return self

    def with_path(
        self,
        value: Union[Path, str],
    ) -> Self:
        """
        Set the path.

        Args:
            value (Union[Path, str]): The path to set.

        Returns:
            Self: The builder.
        """

        # Check if the value is a Path object
        if not isinstance(
            value,
            Path,
        ):
            # Convert the string to a Path object
            value = Path(value)

        # Set the path
        self._configuration["path"] = value

        # Return the builder
        return self

    def with_primary_key(
        self,
        value: str,
    ) -> Self:
        """
        Set the primary key.

        Args:
            value (str): The primary key to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"primary_key": ""}

        # Set the primary key
        self._configuration["definition"]["primary_key"] = value

        # Return the builder
        return self

    def with_reference(
        self,
        value: str,
    ) -> Self:
        """
        Set the reference.

        Args:
            value (str): The reference to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"references": {}}

        # Generate an identifier for the reference
        identifier: str = str(len(self._configuration["definition"]["references"]))

        # Set the reference
        self._configuration["definition"]["references"][identifier] = value

        # Return the builder
        return self

    def with_references(
        self,
        value: list[str],
    ) -> Self:
        """
        Set the references.

        Args:
            value (list[str]): The references to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"references": {}}

        # Set the references
        for reference in value:
            # Check if the reference already exists
            if reference in self._configuration["definition"]["references"].values():
                # Raise a ValueError
                raise ValueError(f"Reference '{reference}' already exists")

            # Generate an identifier for the reference
            identifier: str = str(len(self._configuration["definition"]["references"]))

            # Set the reference
            self._configuration["definition"]["references"][identifier] = reference

        # Return the builder
        return self

    def with_unique_key(
        self,
        value: str,
    ) -> Self:
        """
        Set the unique key.

        Args:
            value (str): The unique key to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"unique": []}

        # Set the unique key
        self._configuration["definition"]["unique"].append(value)

        # Return the builder
        return self

    def with_unique_keys(
        self,
        value: list[str],
    ) -> Self:
        """
        Set the unique keys.

        Args:
            value (list[str]): The unique keys to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"unique": []}

        # Set the unique keys
        for key in value:
            # Check if the key already exists
            if key in self._configuration["definition"]["unique"]:
                # Raise a ValueError
                raise ValueError(f"Unique key '{key}' already exists")

            # Set the unique key
            self._configuration["definition"]["unique"].append(key)

        # Return the builder
        return self
