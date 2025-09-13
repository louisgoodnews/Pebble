"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from pathlib import Path
from typing import (
    Final,
    List,
    Optional,
    Union,
)
from utils.utils import (
    convert_to_path,
    cwd,
    path_exists,
)

from core.database import PebbleDatabase, PebbleDatabaseBuilder
from core.table import PebbleTable, PebbleTableBuilder


__all__: Final[List[str]] = ["Pebble"]


class Pebble:
    """
    The main class for the Pebble library.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Initialize a new Pebble object.

        Args:
            name (str, optional): The name of the database.
            path (Union[Path, str], optional): The path to the database file.

        Returns:
            None
        """

        # Initialize the databases dictionary
        self._databases: dict[str, PebbleDatabase] = {}

        # Initialize the tables dictionary
        self._tables: dict[str, PebbleTable] = {}

    def empty(
        self,
        database: bool = False,
        tables: bool = False,
    ) -> bool:
        """
        Check if the Pebble object is empty.

        This method will check if the Pebble object is empty, i.e. if the databases and tables dictionaries are empty.

        Args:
            database (bool, optional): Whether to check the databases.
            tables (bool, optional): Whether to check the tables.

        Returns:
            bool: True if the Pebble object is empty, False otherwise.
        """

        # Initialize the result list
        result: List[bool] = []

        # Check if the databases should be checked
        if database:
            # Check if the databases dictionary is empty
            result.append(len(self._databases) == 0)

        # Check if the tables should be checked
        if tables:
            # Check if the tables dictionary is empty
            result.append(len(self._tables) == 0)

        # Return True if all the conditions are met
        return all(result)

    def database(
        self,
        name: str,
        path: Optional[Path] = None,
    ) -> PebbleDatabase:
        """
        Get or create a database.

        Args:
            name (str): The name of the database.

        Returns:
            PebbleDatabase: The database.
        """

        # Check if the database exists
        if name in self._databases:
            # Return the database
            return self._databases[name]

        # Create the database builder
        builder: PebbleDatabaseBuilder = PebbleDatabaseBuilder()

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

        # Build the database
        database: PebbleDatabase = builder.build()

        # Add the database to the Pebble object
        self._databases[name] = database

        # Return the database
        return database

    @classmethod
    def get_database_builder() -> PebbleDatabaseBuilder:
        """
        Get a new database builder.

        Returns:
            PebbleDatabaseBuilder: The database builder.
        """

        # Return a new database builder
        return PebbleDatabaseBuilder()

    @classmethod
    def get_table_builder() -> PebbleTableBuilder:
        """
        Get a new table builder.

        Returns:
            PebbleTableBuilder: The table builder.
        """

        # Return a new table builder
        return PebbleTableBuilder()

    def get_or_create_database(
        self,
        name: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> PebbleDatabase:
        """
        Get or create a database.

        Args:
            name (str, optional): The name of the database.
            path (Union[Path, str], optional): The path to the database file.

        Returns:
            PebbleDatabase: The database.
        """

        # Check if the database exists
        if name in self._databases:
            # Return the database
            return self._databases[name]

        # Check if the path is None
        if path is None:
            # Configure the path
            path = Path(
                cwd(),
                f"{name}.json",
            )

        # Check if the path is a string
        if not isinstance(
            path,
            Path,
        ):
            # Convert the path to a Path object
            path = convert_to_path(path=path)

        # Check if the path exists
        if path_exists(path=path):
            # Create the database
            database = PebbleDatabase.from_file(path=path)
        else:
            # Create the database
            database = PebbleDatabase(
                name=name,
                path=path,
            )

        # Add the database to the dictionary
        self._databases[name] = database

        # Return the database
        return database

    def get_or_create_table(
        self,
        name: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> PebbleTable:
        """
        Get or create a table.

        Args:
            name (str, optional): The name of the table.
            path (Union[Path, str], optional): The path to the table file.

        Returns:
            PebbleTable: The table.
        """

        # Check if the path is None
        if path is None or not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = convert_to_path(path=path)

        # Check if the table exists
        if name in self._tables:
            # Return the table
            return self._tables[name]

        # Create the table
        table = PebbleTable.from_file(path=path)

        # Add the table to the dictionary
        self._tables[name] = table

        # Return the table
        return table

    def table(
        self,
        name: str,
        path: Optional[Path] = None,
    ) -> PebbleTable:
        """
        Get or create a table.

        Args:
            name (str): The name of the table.

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

        # Add the table to the dictionary
        self._tables[name] = table

        # Return the table
        return table
