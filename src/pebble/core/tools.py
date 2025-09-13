"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from typing import Final, List


__all__: Final[List[str]] = [
    "PebbleTool",
    "PebbleToolBuilder",
]


class PebbleTool:
    """
    A collection of tools for working with Pebble objects.
    """

    @staticmethod
    def from_json(
        file_or_str: Union[bytes, Path, str],
        from_file: bool = True,
    ) -> dict[str, Any]:
        """
        Convert a JSON string to a dictionary.

        Args:
            file_or_str: The JSON string or file to convert.
            from_file: Whether the JSON string is a file.

        Returns:
            The dictionary representation of the JSON string.
        """

        # Check if the JSON string is a file
        if from_file:
            # Check if the file is a Path object
            if not isinstance(
                file_or_str,
                Path,
            ):
                # Convert the string to a Path object
                file_or_str = Path(file_or_str)

            # Read the file
            content: str = run_asynchronously(
                function=read_file,
                path=file_or_str,
            )

            # Convert the JSON string to a dictionary
            return json_to_dict(json_string=content)
        else:
            # Convert the JSON string to a dictionary
            return json_to_dict(json_string=file_or_str)

    @staticmethod
    def immutable(dictionary: dict[str, Any]) -> PebbleRecord:
        """
        Convert a dictionary to an immutable PebbleRecord.

        Args:
            dictionary: The dictionary to convert.

        Returns:
            The immutable PebbleRecord.
        """
        return PebbleRecord(dictionary=dictionary)

    @staticmethod
    def mutable(dictionary: PebbleRecord) -> dict[str, Any]:
        """
        Convert a PebbleRecord to a mutable dictionary.

        This method is a wrapper for the PebbleRecord.to_dict() method.
        Might as well just use the to_dict() method directly.

        Args:
            dictionary: The PebbleRecord to convert.

        Returns:
            The mutable dictionary.
        """

        # Return the dictionary
        return dictionary.to_dict()

    @staticmethod
    def merge(
        source: dict[str, Any],
        target: dict[str, Any],
        conflict_resolver: Callable[[Any, Any], Any] = lambda x, y: y,
    ) -> dict[str, Any]:
        """
        Merge two dictionaries into one.

        This method converts the dictionaries to PebbleRecords and then merges them.

        Args:
            source: The source dictionary.
            target: The target dictionary.
            conflict_resolver: The conflict resolver.

        Returns:
            The merged dictionary.
        """

        # Return the merged dictionary
        return PebbleTool.immutable(dictionary=source).merge(
            conflict_resolver=conflict_resolver,
            other=PebbleTool.immutable(dictionary=target),
        )

    @staticmethod
    def to_json(
        dictionary: dict[str, Any],
        path: Optional[Union[Path, str]] = None,
    ) -> str:
        """
        Convert a dictionary to a JSON string.

        Args:
            dictionary (dict[str, Any]): The dictionary to convert.
            path (Optional[Union[Path, str]]): The path to the file to write the JSON string to.

        Returns:
            str: The path to the file.
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

        # Write the dictionary to the file
        result: bool = run_asynchronously(
            data=dict_to_json(dictionary=dictionary),
            function=write_file,
            path=path,
        )

        # Check if the file was written
        if not result:
            # Raise a PebbleFileWriteError
            raise PebbleFileWriteError(path=path)

        # Return the file path
        return path.resolve().as_posix()

    @staticmethod
    def to_list(dictionary: dict) -> list:
        """
        Convert a dictionary to a list.

        Args:
            dictionary: The dictionary to convert.

        Returns:
            The list representation of the dictionary.
        """

        # Return the list
        return list(dictionary.values())

    @staticmethod
    def traverse(
        dictionary: Union[dict, PebbleDatabase, PebbleRecord, PebbleTable],
        path: str = "",
    ) -> Iterable[tuple[str, Any]]:
        """
        Traverses a dictionary and returns a list of (key, value) pairs.

        Args:
            dictionary: The dictionary to traverse.
            path: The path to the current key. It has to have a format like "key1.key2.key3".

        Returns:
            A list of (key, value) pairs.
        """

        # Check if the dictionary is a PebbleDatabase, PebbleRecord, or PebbleTable
        if isinstance(
            dictionary,
            (
                PebbleDatabase,
                PebbleRecord,
                PebbleTable,
            ),
        ):
            # Convert the PebbleDatabase, PebbleRecord, or PebbleTable to a dictionary
            dictionary = dictionary.to_dict()

        # Iterate over the dictionary
        for (
            key,
            value,
        ) in dictionary.items():
            # Get the current path
            current_path: str = f"{path}.{key}" if path else key

            # Check if the value is a dictionary
            if isinstance(
                value,
                dict,
            ):
                # Recursively traverse the dictionary
                yield from PebbleTool.traverse(value, current_path)
            else:
                # Yield the key and value
                yield (current_path, value)


class PebbleToolBuilder:
    """
    A class that provides a builder pattern for dictionaries.
    """

    def __init__(
        self,
        dictionary: dict[str, Any],
    ) -> None:
        """
        Initialize a new PebbleToolBuilder instance.

        Args:
            dictionary: The dictionary to build.
        """

        # Store the passed dictionary in an instance variable
        self._dictionary: PebbleRecord = PebbleRecord.from_dict(dictionary=dictionary)

        # Initialize the result to None
        self._result: Optional[Union[dict[str, Any], PebbleRecord]] = None

    @property
    def dictionary(self) -> dict[str, Any]:
        """
        Get the dictionary of this PebbleToolBuilder instance.

        Returns:
            dict[str, Any]: The dictionary of this PebbleToolBuilder instance.
        """

        return self._dictionary.to_dict()

    def __repr__(self) -> str:
        """
        Get a string representation of the PebbleToolBuilder instance.

        Returns:
            str: The string representation of the PebbleToolBuilder instance.
        """

        return f"<{self.__class__.__name__}(dictionary={self._dictionary})>"

    def build(self) -> Union[dict[str, Any], PebbleRecord]:
        """
        Build the dictionary.

        Returns:
            Union[dict[str, Any], PebbleRecord]: The built dictionary.
        """

        # Return the result
        return self._result

    def delete(
        self,
        path: str,
    ) -> Self:
        """
        Delete a key from the dictionary.

        Args:
            path (str): The path to the key.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            self._dictionary = self._dictionary.to_dict()

        # Split the path into parts
        parts: list[str] = path.split(".")

        # Set the current dictionary to the dictionary
        current: dict[str, Any] = self._dictionary

        # Iterate over the parts of the path
        for part in parts:
            # Check if the part is not in the current dictionary or if it is not a dictionary
            if part not in current or not isinstance(current[part], dict):
                # Create the dictionary
                current[part] = {}

            # Set the current dictionary to the part
            current = current[part]

        # Delete the key
        current.pop(parts[-1])

        # Set the dictionary to the dictionary
        self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def filter(
        self,
        string: str,
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> Self:
        """
        Filter the dictionary.

        Args:
            scope (Literal["ALL", "ANY", "NONE"]): The scope of the filter. Defaults to "ALL".
            string (str): The filter string.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            dictionary: dict[str, Any] = self._dictionary.to_dict()
        else:
            # Set the dictionary to the passed dictionary
            dictionary = self._dictionary

        # Initialize the filter engine
        engine: PebbleFilterEngine = PebbleFilterEngine(table=dictionary)

        # Set the filter
        engine.set_filter(
            filter=PebbleFilterString(string=string),
            scope=scope,
        )

        # Set the dictionary to the filtered dictionary
        self._dictionary = PebbleRecord.from_dict(dictionary=engine.filter())

        # Return the PebbleToolBuilder
        return self

    def merge(
        self,
        other: PebbleRecord,
    ) -> Self:
        """
        Merge another dictionary into this dictionary.

        Args:
            other (PebbleRecord): The dictionary to merge.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        if not isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a PebbleRecord
            self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Merge the dictionaries
        self._dictionary = self._dictionary.merge(other=other)

        # Return the PebbleToolBuilder
        return self

    def query(
        self,
        string: str,
    ) -> Self:
        """
        Query the dictionary.

        Args:
            string (str): The query string.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        raise NotImplementedError(f"{self.__class__.__name__}.query() is not implemented yet.")

    def set(
        self,
        path: str,
        value: Any,
    ) -> Self:
        """
        Set a value in the dictionary.

        Args:
            path (str): The path to the value.
            value (Any): The value to set.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            self._dictionary = self._dictionary.to_dict()

        # Split the path into parts
        parts: list[str] = path.split(".")

        # Set the current dictionary to the dictionary
        current: dict[str, Any] = self._dictionary

        # Iterate over the parts of the path
        for part in parts:
            # Check if the part is not in the current dictionary or if it is not a dictionary
            if part not in current or not isinstance(current[part], dict):
                # Create the dictionary
                current[part] = {}

            # Set the current dictionary to the part
            current = current[part]

        # Set the value
        current[parts[-1]] = value

        # Set the dictionary to the dictionary
        self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def to_immutable(self) -> Self:
        """
        Convert the dictionary to an immutable PebbleRecord.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is not a PebbleRecord (i.e. its a dict)
        if not isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a PebbleRecord
            self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def to_json(self) -> Self:
        """
        Convert the dictionary to a JSON string.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            self._dictionary = self._dictionary.to_dict()

        # Convert the dictionary to a JSON string
        self._result = dict_to_json(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def traverse(self) -> List[tuple[str, Any]]:
        """
        Traverses the dictionary and returns a list of (key, value) pairs.

        Returns:
            List[tuple[str, Any]]: A list of (key, value) pairs.
        """

        # Return the list of (key, value) pairs
        return PebbleTool.traverse(dictionary=self._dictionary)
