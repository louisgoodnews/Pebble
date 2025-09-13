"""
Author: Louis Goodnews
Date: 2025-09-05
"""

import uuid

from datetime import datetime
from pathlib import Path
from typing import Any, Final, List, Optional, Union

from core.cache import PebbleCache
from core.exceptions import (
    PebbleQueryEngineNotInitializedError,
    PebbleTableAlreadyRegisteredError,
    PebbleTableNotFoundError,
    PebbleSizeExceededError,
)
from core.queries import PebbleQueryEngine
from core.table import PebbleTable, PebbleTableBuilder

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
    "PebbleDatabase",
    "PebbleDatabaseFactory",
    "PebbleDatabaseBuilder",
]


class PebbleDatabase:
    """
    A class to represent a database.
    """

    def __init__(
        self,
        name: str,
        flush_interval: int = 300,
        identifier: Optional[str] = None,
        tables: Optional[dict[str, Any]] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Initialize a new PebbleDatabase object.

        Args:
            flush_interval (int): The flush interval in seconds.
            identifier (str): The identifier of the database.
            name (str): The name of the database.
            path (Union[Path, str]): The path to the table file.
            tables (dict[str, Any]): The dictionary to store the table data in.

        Returns:
            None
        """

        # Initialize the cache as an instance variable
        self._cache: Final[PebbleCache] = PebbleCache()

        # Initialize the dirty flag as an instance variable
        self._dirty: bool = False

        # Initialize a new PebbleQueryEngine object
        self._engine: Optional[PebbleQueryEngine] = None

        # Store the passed flush interval in an instance variable
        self._flush_interval: Final[int] = flush_interval

        # Check if the identifier is None
        if identifier is None:
            # Initialize an empty identifier
            identifier = uuid.uuid4().hex

        # Store the passed identifier in an instance variable
        self._identifier: Final[str] = identifier

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

        # Check if the tables is None
        if tables is None:
            # Initialize an empty dictionary
            tables = {
                "total": 0,
                "values": {},
            }

        # Store the passed dictionary in an instance variable
        self._tables: dict[str, Any] = tables

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the database contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the database contains the key, False otherwise.
        """

        return key in [value["name"] for value in self._tables["values"].values()]

    def __eq__(
        self,
        other: object,
    ) -> bool:
        """
        Check if the database is equal to another database.

        Args:
            other: The other database to compare with.

        Returns:
            True if the databases are equal, False otherwise.
        """

        # Check if the other object is a PebbleDatabase object
        if not isinstance(
            other,
            PebbleDatabase,
        ):
            # Return False if the other object is not a PebbleDatabase object
            return False

        # Return True if the databases are equal
        return self._identifier == other._identifier

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get an item from the database itself or its data.

        This method is used to access the database object's data.
        It can also be used to access the database object's instance variables.

        Args:
            key: The key of the item.

        Returns:
            The item.
        """

        # Return the value from the dictionary
        return self._tables["values"][key]

    def __repr__(self) -> str:
        """
        Get a string representation of the database.

        Returns:
            A string representation of the database.
        """

        return f"<{self.__class__.__name__}(tables=[{self._tables['total']} {'table' if self._tables['total'] == 1 else 'tables'}], identifier=[{self._identifier}], name=[{self._name}], path=[{self._path}])>"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set an item in the database itself or its data.

        This method is used to set the database object's data.
        It can also be used to set the database object's instance variables.

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
        self._tables[key] = value

    def __str__(self) -> str:
        """
        Get a string representation of the database.

        Returns:
            A string representation of the database.
        """

        return str(self._tables)

    @property
    def cache(self) -> PebbleCache:
        """
        Get the cache of the database.

        Returns:
            PebbleCache: The cache of the database.
        """

        return self._cache

    @property
    def dirty(self) -> bool:
        """
        Get the dirty flag of the database.

        Returns:
            bool: The dirty flag of the database.
        """

        return self._dirty

    @dirty.setter
    def dirty(
        self,
        value: bool,
    ) -> None:
        """
        Set the dirty flag of the database.

        Args:
            value: The dirty flag of the database.

        Returns:
            None
        """

        self._dirty = value

    @property
    def flush_interval(self) -> int:
        """
        Get the flush interval of the database.

        Returns:
            int: The flush interval of the database.
        """

        return self._flush_interval

    @property
    def identifier(self) -> str:
        """
        Get the identifier of the database.

        Returns:
            str: The identifier of the database.
        """

        return self._identifier

    @property
    def last_flushed_at(self) -> Optional[datetime]:
        """
        Get the last flushed at of the database.

        Returns:
            Optional[datetime]: The last flushed at of the database.
        """

        return self._last_flushed_at

    @last_flushed_at.setter
    def last_flushed_at(
        self,
        value: Optional[datetime],
    ) -> None:
        """
        Set the last flushed at of the database.

        Args:
            value: The last flushed at of the database.

        Returns:
            None
        """

        self._last_flushed_at = value

    @property
    def name(self) -> str:
        """
        Get the name of the database.

        Returns:
            str: The name of the database.
        """

        return self._name

    @name.setter
    def name(
        self,
        value: str,
    ) -> None:
        """
        Set the name of the database.

        Args:
            value: The name of the database.

        Returns:
            None
        """

        # Update the instance variable with the passed value
        self._name = value

    @property
    def path(self) -> Path:
        """
        Get the path of the database.

        Returns:
            Path: The path of the database.
        """

        return self._path

    @path.setter
    def path(
        self,
        value: Union[Path, str],
    ) -> None:
        """
        Set the path of the database.

        Args:
            value: The path of the database.

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
    def tables(self) -> dict[str, Any]:
        """
        Get the tables of the database.

        Returns:
            dict[str, Any]: The tables of the database.
        """

        return dict(self._tables)

    @property
    def total(self) -> int:
        """
        Get the total number of tables in the database.

        Returns:
            int: The total number of tables in the database.
        """

        # Check if the total is in the dictionary
        if "total" not in self._tables:
            # Initialize the total
            self._tables["total"] = 0

        # Return the total
        return self._tables["total"]

    @total.setter
    def total(
        self,
        value: int,
    ) -> None:
        """
        Set the total number of tables in the database.

        Args:
            value: The total number of tables in the database.

        Returns:
            None
        """

        # Check if the total is in the dictionary
        if "total" not in self._tables:
            # Initialize the total
            self._tables["total"] = 0

        # Update the instance variable with the passed value
        self._tables["total"] = value

    @property
    def values(self) -> dict[str, Any]:
        """
        Get the values of the database.

        This method returns a copy of the values.

        Returns:
            dict[str, Any]: The values of the database.
        """

        # Check if the values is in the dictionary
        if "values" not in self._tables:
            # Initialize the values
            self._tables["values"] = {}

        # Return the values
        return self._tables["values"].copy()

    def add_table(
        self,
        table: PebbleTable,
    ) -> None:
        """
        Add a table to the database.

        Args:
            table: The table to add.

        Returns:
            None

        Raises:
            PebbleTableAlreadyRegisteredError: If the table is already registered.
        """

        # Check if the table is already registered
        if table.name in self.values:
            # Raise a KeyError if the table is already registered
            raise PebbleTableAlreadyRegisteredError(table=table.name)

        # Increment the total
        self.total += 1

        # Get the identifier
        identifier: str = str(len(self.values))

        # Add the table to the dictionary
        self._tables["values"][identifier] = {
            "name": table.name,
            "path": table.path,
        }

        # Mark the database as dirty
        self.mark_as_dirty()

    def all(self) -> dict[str, Any]:
        """
        Get all the data in the database.

        Returns:
            The data in the database.
        """

        return self.values

    def check_for_size(self) -> bool:
        """
        Check if the database exceeds the maximum size.

        Returns:
            bool: True if the database exceeds the maximum size, False otherwise.

        Raises:
            PebbleSizeExceededError: If the database exceeds the maximum size.
        """

        # Check if the database exceeds the maximum size
        if self.get_size_of() > OBJECT_SIZE_LIMIT:
            # Raise a PebbleSizeExceededError
            raise PebbleSizeExceededError(name=self._name)

        # Return False if the database does not exceed the maximum size
        return False

    def commit(self) -> None:
        """
        Commit the database to a file.

        This method will commit the database to a file.

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

        # Mark the database as clean
        self.mark_as_clean()

        # Update the last flushed at
        self._last_flushed_at = datetime.now()

    def configure(
        self,
        path: str,
        value: Any,
    ) -> None:
        """
        Configure the database.

        This method will configure the database.

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
                "Cannot configure non-constraints or non-definition values with 'configure' method. PebbleDatabase object is otherwise immutable."
            )

        # Create a builder
        builder: PebbleToolBuilder = PebbleToolBuilder(dictionary=self.to_dict())

        # Set the value
        builder.set(path=path, value=value)

        # Update the table object
        self.__dict__.update(builder.build().to_dict())

        # Mark the database as dirty
        self.mark_as_dirty()

    def delete(self) -> None:
        """
        Delete the database file and all its tables.

        This method will delete the database file and all its tables.

        Returns:
            None
        """

        # Iterate over the dictionary
        for value in self.values.values():
            # Delete the table
            PebbleTable.from_file(path=value["path"]).delete()

        # Check if the file exists
        if not self.path.exists():
            # Return if the file does not exist
            return

        # Delete the database file
        delete_file(path=self.path)

        # Delete the database object
        del self

    def engine(self) -> Self:
        """
        Return the engine.

        Returns:
            Self: The Database object.
        """

        # Check if the engine is None
        if self._engine is None:
            # Initialize a new PebbleQueryEngine object
            self._engine = PebbleQueryEngine(data=self.values)

        # Return the database object
        return self

    @classmethod
    def from_file(
        cls,
        path: Union[Path, str],
    ) -> "PebbleDatabase":
        """
        Create a new PebbleDatabase object from a file.

        Args:
            path: The path to the database file.

        Returns:
            A new PebbleDatabase object.
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

        # Return the database
        return PebbleDatabase(
            identifier=json.get("identifier", ""),
            name=json.get("name", ""),
            path=json.get("path", ""),
            tables=json,
        )

    @classmethod
    def from_dict(
        cls,
        dictionary: dict[str, Any],
        name: str,
    ) -> "PebbleDatabase":
        """
        Create a new PebbleDatabase object from a dictionary.

        Args:
            dictionary: The dictionary to create the database from.
            name: The name of the database.

        Returns:
            A new PebbleDatabase object.
        """

        # Return the database
        return PebbleDatabase(
            name=name,
            tables=dictionary,
        )

    def get_size_of(self) -> int:
        """
        Get the size of the database.

        Returns:
            int: The size of the database.
        """

        # Return the size of the database
        return self._total

    def is_empty(self) -> bool:
        """
        Check if the database is empty.

        Returns:
            bool: True if the database is empty, False otherwise.
        """

        return self._total == 0

    def mark_as_clean(self) -> None:
        """
        Mark the database as clean.

        Returns:
            None
        """

        # Set the dirty flag to False
        self._dirty = False

    def mark_as_dirty(self) -> None:
        """
        Mark the database as dirty.

        Returns:
            None
        """

        # Set the dirty flag to True
        self._dirty = True

    def query(self) -> dict[str, Any]:
        """
        Return the query.

        Returns:
            dict[str, Any]: The query.

        Raises:
            PebbleQueryEngineNotInitializedError: If the query engine is not initialized.
        """

        # Check if the engine is None
        if not self._engine:
            # Raise a PebbleQueryEngineNotInitializedError if the engine is None
            raise PebbleQueryEngineNotInitializedError("The query engine is not initialized.")

        # Return the query
        return self._engine.query()

    def remove_table(
        self,
        table: PebbleTable,
    ) -> None:
        """
        Remove a table from the database.

        Args:
            table: The table to remove.

        Returns:
            None

        Raises:
            PebbleTableNotFoundError: If the table is not found.
        """

        # Check if the table is in the dictionary
        if table.name not in self.values:
            # Raise a KeyError if the table is not found
            raise PebbleTableNotFoundError(table=table.name)

        # Iterate over the dictionary
        for (
            key,
            value,
        ) in self.values.items():
            # Check if the table is in the dictionary
            if value["name"] != table.name:
                # Skip the table
                continue

            # Remove the table from the dictionary
            self._tables["values"].pop(key)

            # Decrement the total
            self.total -= 1

            # Break the loop
            break

        # Mark the database as dirty
        self.mark_as_dirty()

    def set_query(
        self,
        string: str,
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
        flag: Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"] = "CASE_INSENSITIVE",
    ) -> Self:
        """
        Set the query.

        Args:
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.
            string (str): The string to set the query from.

        Returns:
            Self: The database object.
        """

        # Check if the engine is None
        if self._engine is None:
            # Raise a PebbleQueryEngineNotInitializedError if the engine is None
            raise PebbleQueryEngineNotInitializedError("The query engine is not initialized.")

        # Set the query
        self._engine.set_query(
            flag=flag,
            scope=scope,
            string=string,
        )

        # Return the database object
        return self

    def set_queries(
        self,
        strings: list[str],
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
        flag: Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"] = "CASE_INSENSITIVE",
    ) -> Self:
        """
        Set the queries.

        Args:
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.
            strings (list[str]): The strings to set the queries from.

        Returns:
            Self: The database object.
        """

        # Check if the engine is None
        if self._engine is None:
            # Raise a PebbleQueryEngineNotInitializedError if the engine is None
            raise PebbleQueryEngineNotInitializedError("The query engine is not initialized.")

        # Set the queries
        self._engine.set_queries(
            flag=flag,
            scope=scope,
            strings=strings,
        )

        # Return the database object
        return self

    def table(
        self,
        name: str,
        path: Optional[Path] = None,
    ) -> PebbleTable:
        """
        Get or create a table.

        Args:
            name (str): The name of the table.
            path (Optional[Path], optional): The path to the table. Defaults to None.

        Returns:
            PebbleTable: The table.
        """

        # Check if the table exists
        if name in self._tables:
            # Return the table
            return self._tables[name]

        # Create the table builder
        builder: PebbleTableBuilder = PebbleTableBuilder()

        # Set the identifier
        builder.with_identifier()

        # Set the name
        builder.with_name(value=name)

        # Check if the path is None
        if not path:
            # Configure the path
            path = Path(
                ".",
                f"{name}.json",
            )

        # Set the path
        builder.with_path(value=path)

        # Build the table
        table: PebbleTable = builder.build()

        # Add the table to the database
        self.add_table(table=table)

        # Mark the database as dirty
        self.mark_as_dirty()

        # Return the table
        return table

    def tables(
        self,
        names: bool = False,
    ) -> List[PebbleTable]:
        """
        Get all the tables in the database.

        Args:
            names (bool, optional): Whether to return the names of the tables.

        Returns:
            List[PebbleTable]: The tables in the database.
        """

        # Initialize the tables list
        tables: List[PebbleTable] = []

        # Iterate over the dictionary
        for table in self.values.values():
            # Check if the names flag is set
            if names:
                # Append the name of the table
                tables.append(table["name"])

                # Continue to the next table
                continue

            # Create the table
            tables.append(PebbleTable.from_file(path=table["path"]))

        # Return the tables
        return tables

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the database to a dictionary.

        Returns:
            The dictionary representation of the database.
        """

        return {
            "identifier": self._identifier,
            "name": self._name,
            "path": self._path,
            "tables": self._tables,
        }

    def to_str(self) -> str:
        """
        Convert the database to a string.

        Returns:
            The string representation of the database.
        """

        return dict_to_json(dictionary=self.to_dict())


class PebbleDatabaseFactory:
    """
    A factory class for creating new databases.
    """

    @staticmethod
    def create(
        name: str,
        identifier: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
        tables: Optional[dict[str, Any]] = None,
    ) -> PebbleDatabase:
        """
        Create a new database.

        Args:
            tables (dict[str, Any]): The tables to create the database from.
            identifier (str): The identifier of the database.
            name (str): The name of the database.
            path (Union[Path, str]): The path to the database file.

        Returns:
            PebbleDatabase: The new database.
        """

        # Return the database
        return PebbleDatabase(
            identifier=identifier,
            name=name,
            path=path,
            tables=tables,
        )


class PebbleDatabaseBuilder:
    """
    A builder class for creating new databases.
    """

    def __init__(self) -> None:
        """
        Initialize a new PebbleDatabaseBuilder object.

        Returns:
            None
        """

        # Initialize the configuration
        self._configuration: dict[str, Any] = {
            "identifier": "",
            "name": "",
            "path": "",
            "tables": {
                "total": 0,
                "values": {},
            },
        }

    @property
    def configuration(self) -> dict[str, Any]:
        """
        Get the configuration.

        This method returns a copy of the configuration.

        Returns:
            The configuration.
        """

        # Return the configuration
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
        other: "PebbleDatabaseBuilder",
    ) -> bool:
        """
        Check if the builder is equal to another builder.

        Args:
            other (PebbleDatabaseBuilder): The other builder to compare to.

        Returns:
            bool: True if the builders are equal, False otherwise.
        """

        # Check if the other object is a PebbleDatabaseBuilder
        if not isinstance(
            other,
            PebbleDatabaseBuilder,
        ):
            # Return False
            return False

        # Return the equality of the builders
        return self._configuration == other._configuration

    def __repr__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            The string representation of the builder.
        """

        # Return the string representation of the builder
        return f"PebbleDatabaseBuilder(configuration={self._configuration})"

    def __str__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            The string representation of the builder.
        """

        # Return the string representation of the builder
        return str(self._configuration)

    def build(self) -> PebbleDatabase:
        """
        Build the database.

        This method returns a new PebbleDatabase object.

        Returns:
            The database.
        """

        # Return the database
        return PebbleDatabaseFactory.create(**self._configuration)

    def with_dictionary(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the dictionary of the database.

        Args:
            value (dict[str, Any]): The dictionary of the database.

        Returns:
            Self: The builder.
        """

        # Set the dictionary
        self._configuration["dictionary"] = value

        # Return the builder
        return self

    def with_identifier(
        self,
        value: Optional[str] = None,
    ) -> Self:
        """
        Set the identifier of the database.

        Args:
            value (str): The identifier of the database.

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

    def with_name(
        self,
        value: str,
    ) -> Self:
        """
        Set the name of the database.

        Args:
            value (str): The name of the database.

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
        Set the path of the database.

        Args:
            value (Union[Path, str]): The path of the database.

        Returns:
            Self: The builder.
        """

        # Check if the value is a Path object
        if not isinstance(value, Path):
            # Convert the string to a Path object
            value = Path(value)

        # Set the path
        self._configuration["path"] = value

        # Return the builder
        return self

    def with_table(
        self,
        value: PebbleTable,
    ) -> Self:
        """
        Set the table of the database.

        Args:
            value (PebbleTable): The table of the database.

        Returns:
            Self: The builder.
        """

        # Check if the tables are not in the configuration
        if "tables" not in self._configuration:
            # Initialize the tables nested dictionary
            self._configuration["tables"] = {"total": 0, "values": {}}

        # Generate an identifier for the table
        identifier: str = str(len(self._configuration["tables"]["values"]))

        # Set the table
        self._configuration["tables"]["values"][identifier] = value

        # Increment the total number of tables
        self._configuration["tables"]["total"] += 1

        # Return the builder
        return self

    def with_tables(
        self,
        value: list[PebbleTable],
    ) -> Self:
        """
        Set the tables of the database.

        Args:
            value (list[PebbleTable]): The tables of the database.

        Returns:
            Self: The builder.
        """

        # Iterate over the tables
        for table in value:
            # Add the table
            self.with_table(value=table)

        # Return the builder
        return self
