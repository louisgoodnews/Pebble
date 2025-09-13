"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from typing import Any, Final, Iterator, List, Literal


__all__: Final[List[str]] = [
    "PebbleFilterString",
    "PebbleFilterEngine",
]


class PebbleFilterString:
    """
    A class to represent a filter string.
    """

    def __init__(
        self,
        string: str,
        flag: Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"] = "CASE_INSENSITIVE",
    ) -> None:
        """
        Initialize a new PebbleFilterString object.

        Args:
            string: The string to store in the filter.

        Returns:
            None
        """

        # Store the field of the filter in an instance variable
        self._field: str = ""

        # Store the flag of the filter in an instance variable
        self._flag: Final[Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]] = flag

        # Store the parsed state of the filter in an instance variable
        self._parsed: bool = False

        # Store the operator of the filter in an instance variable
        self._operator: str = ""

        # Store the scope of the filter in an instance variable
        self._scope: str = ""

        # Store the passed string in an instance variable
        self._string: Final[str] = string

        # Store the value of the filter in an instance variable
        self._value: Any = ""

        # Parse the filter string
        self.parse()

    def __eq__(
        self,
        other: "PebbleFilterString",
    ) -> bool:
        """
        Check if the filter is equal to another object.

        Args:
            other: The object to compare to.

        Returns:
            True if the filter is equal to the other object, False otherwise.
        """

        # Check if the other object is a PebbleFilterString
        if not isinstance(
            other,
            PebbleFilterString,
        ):
            # Return False if the other object is not a PebbleFilterString
            return False

        # Return True if the strings are equal
        return self._string == other.string

    def __iter__(self) -> Iterator[str]:
        """
        Return an iterator over the string.

        Returns:
            Iterator[str]: An iterator over the string.
        """

        return iter(self._string)

    def __len__(self) -> int:
        """
        Return the length of the string.

        Returns:
            int: The length of the string.
        """

        return len(self._string)

    def __repr__(self) -> str:
        """
        Get a string representation of the filter.

        Returns:
            A string representation of the filter.
        """

        return f"<{self.__class__.__name__}(field={self._field!r}, flag={self._flag!r}, operator={self._operator!r}, scope={self._scope!r}, value={self._value!r})>"

    def __str__(self) -> str:
        """
        Get the string of the filter.

        Returns:
            The string of the filter.
        """

        return self._string

    @property
    def field(self) -> str:
        """
        Get the field of the filter.

        Returns:
            The field of the filter.
        """

        return self._field

    @property
    def flag(self) -> Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]:
        """
        Get the flag of the filter.

        Returns:
            The flag of the filter.
        """

        return self._flag

    @property
    def operator(self) -> str:
        """
        Get the operator of the filter.

        Returns:
            The operator of the filter.
        """

        return self._operator

    @property
    def parsed(self) -> bool:
        """
        Get the parsed state of the filter.

        Returns:
            The parsed state of the filter.
        """

        return self._parsed

    @property
    def scope(self) -> str:
        """
        Get the scope of the filter.

        Returns:
            The scope of the filter.
        """

        return self._scope

    @property
    def string(self) -> str:
        """
        Get the string of the filter.

        Returns:
            The string of the filter.
        """

        return self._string

    @property
    def value(self) -> Any:
        """
        Get the value of the filter.

        Returns:
            The value of the filter.
        """

        return self._value

    def evaluate(
        self,
        entry: dict[str, Any],
    ) -> bool:
        """
        Evaluate the filter against a single entry (row).

        Args:
            entry: The table entry (dict) to test.

        Returns:
            True if the filter matches the entry, False otherwise.
        """

        # Field not present → no match
        if self._field not in entry:
            # Return False if the field is not present
            return False

        # Get the value of the field
        entry_value: Optional[Any] = entry.get(
            self._field,
            None,
        )

        # Check if the field is present
        if entry_value is None:
            # Return False if the field is not present
            return False

        # Get the operator
        operator: Literal["==", "!=", "<", ">", "<=", ">=", "in", "not in", "is", "is not"] = (
            self._operator.lower()
        )

        # Get the value
        value: Any = self._value

        # Convert to lowercase if the flag is CASE_INSENSITIVE and the values are strings
        if isinstance(entry_value, str) and isinstance(value, str):
            # Convert to lowercase if the flag is CASE_INSENSITIVE
            if self._flag == "CASE_INSENSITIVE":
                # Convert entry_value to lowercase if it is a string
                entry_value = entry_value.lower()

                # Convert value to lowercase if it is a string
                value = value.lower()

        # Comparison logic
        if operator in {"==", "is"}:
            # Return True if the values are equal
            return entry_value == value
        elif operator in {"!=", "is not"}:
            # Return True if the values are not equal
            return entry_value != value
        elif operator == "<":
            # Return True if the entry value is less than the value
            return entry_value < value
        elif operator == ">":
            # Return True if the entry value is greater than the value
            return entry_value > value
        elif operator == "<=":
            # Return True if the entry value is less than or equal to the value
            return entry_value <= value
        elif operator == ">=":
            # Return True if the entry value is greater than or equal to the value
            return entry_value >= value
        elif operator == "in":
            try:
                # Check if the entry value is a string and the value is a string
                if isinstance(entry_value, str) and isinstance(value, str):
                    # Return True if the value is in the entry value
                    return value in entry_value
                # Check if the entry value is a list, set, tuple or dict
                if isinstance(entry_value, (list, set, tuple, dict)):
                    # Return True if the value is in the entry value
                    return value in entry_value
                return False
            except TypeError:
                return False
        elif operator == "not in":
            try:
                # Check if the entry value is a string and the value is a string
                if isinstance(entry_value, str) and isinstance(value, str):
                    # Return True if the value is not in the entry value
                    return value not in entry_value
                # Check if the entry value is a list, set, tuple or dict
                if isinstance(entry_value, (list, set, tuple, dict)):
                    # Return True if the value is not in the entry value
                    return value not in entry_value
                return True
            except TypeError:
                return True

        # Unknown operator
        raise ValueError(f"Unsupported operator: {self._operator}")

    def parse(self) -> None:
        """
        Parse the filter string.

        Returns:
            None
        """

        # Check if the filter has already been parsed
        if self._parsed:
            # Return if the filter has already been parsed
            return

        # Parse the filter string
        parsed: Optional[dict[str, Any]] = match_pattern(
            pattern=FILTER_PATTERN,
            string=self._string,
        )

        # Check if the string is in the correct format
        if parsed is None:
            # Raise a PebbleFilterStringFormatError if the string is not in the correct format
            raise PebbleFilterStringFormatError(string=self._string)

        # Set the attributes of the filter
        for (
            key,
            value,
        ) in parsed.items():
            # Set the attribute of the filter
            setattr(
                self,
                f"_{key}",
                string_to_object(string=value),
            )

        # Remove the quotes if the value is a string
        if self._value is not None and isinstance(
            self._value,
            str,
        ):
            # Remove the quotes from the value
            self._value = unquote_string(string=self._value)

        # Set the parsed state of the filter to True
        self._parsed = True

    def parts(self) -> list[str]:
        """
        Get the parts of the filter string.

        Returns:
            The parts of the filter string.
        """

        return [
            self._table,
            self._field,
            self._scope,
            self._operator,
            self._value,
        ]

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the filter to a dictionary.

        Returns:
            The filter as a dictionary.
        """

        return {
            "field": self._field,
            "operator": self._operator,
            "scope": self._scope,
            "string": self._string,
            "table": self._table,
            "value": {
                "type": type(self._value).__name__,
                "value": self._value,
            },
        }

    def to_list(self) -> list[str]:
        """
        Convert the filter to a list.

        This method is a wrapper for the parts() method.

        Returns:
            The filter as a list.
        """

        return self.parts()

    def to_str(self) -> str:
        """
        Convert the filter to a string.

        Returns:
            The filter as a string.
        """

        return self._string

    def to_tuple(self) -> tuple[str, str, str, str, str]:
        """
        Convert the filter to a tuple.

        Returns:
            The filter as a tuple.
        """

        return tuple(self.parts())


class PebbleFilterEngine:
    """
    A filter engine for filtering tables.
    """

    def __init__(
        self,
        table: Union[dict[str, Any], "PebbleTable"],
    ) -> None:
        """
        Initialize a new PebbleFilterEngine object.

        Args:
            table (Union[dict[str, Any], PebbleTable]): The dictionary or table to filter.

        Returns:
            None
        """

        # Initialize an empty dictionary of filters
        self._filters: dict[
            str,
            dict[
                str,
                Union[
                    Literal["ALL", "ANY", "NONE"],
                    Literal["AND", "OR"],
                    PebbleFilterString,
                ],
            ],
        ] = {}

        # Store the passed table in an instance variable
        self._table: Union[dict[str, Any], PebbleTable] = table

    @property
    def filters(
        self,
    ) -> list[
        dict[
            str,
            Union[
                Literal["ALL", "ANY", "NONE"],
                Literal["AND", "OR"],
                PebbleFilterString,
            ],
        ],
    ]:
        """
        Get the filters of the engine.

        Returns:
            list[dict[str, Union[Literal["ALL", "ANY", "NONE"], Literal["AND", "OR"], PebbleFilterString]]]: The filters of the engine.
        """

        return list(self._filters.values())

    @property
    def table(self) -> Union[dict[str, Any], "PebbleTable"]:
        """
        Get the table of the engine.

        Returns:
            Union[dict[str, Any], "PebbleTable"]: The table of the engine.
        """

        return self._table

    @table.setter
    def table(
        self,
        value: Union[dict[str, Any], "PebbleTable"],
    ) -> None:
        """
        Set the table of the engine.

        Args:
            value (Union[dict[str, Any], PebbleTable]): The table to set.

        Returns:
            None
        """

        # Check if the value is a PebbleTable
        if isinstance(
            value,
            PebbleTable,
        ):
            # Set the table to the entries of the table
            value = PebbleTable.entries

        # Update the instance variable with the passed value
        self._table = value

    def __repr__(self) -> str:
        """
        Get a string representation of the engine.

        Returns:
            A string representation of the engine.
        """

        return f"<{self.__class__.__name__}(filters={self._filters!r}, table={self._table!r})>"

    def __str__(self) -> str:
        """
        Get a string representation of the engine.

        Returns:
            A string representation of the engine.
        """

        return self.__repr__()

    def filter(self) -> dict[str, Any]:
        """
        Filter the table.

        Returns:
            The filtered table.
        """

        table: list[dict[str, Any]] = []

        # Check if the table is a PebbleTable
        if isinstance(
            self._table,
            PebbleTable,
        ):
            # Get the table entries
            table = self._table.all(format="list")
        else:
            # Set the table to the passed table
            table = PebbleTool.to_list(dictionary=self._table)

        # Initialize an empty dictionary
        result: dict[str, Any] = {
            "filter": f"{'.'.join([f'{str(filter.get('filter', ''))}.{filter.get('operator', '')}.{filter.get('scope', '')}' for filter in self.filters])}",
            "total": 0,
            "values": [],
        }

        # Iterate over the table entries
        for entry in table:

            # Initialize an empty list of matches
            matches: list[bool] = []

            # Initialize the operator
            outer_operator: Literal["AND", "OR", ""] = ""

            # Initialize the scope
            outer_scope: Literal["ALL", "ANY", "NONE", ""] = ""

            # Iterate over the filters
            for clause in self.filters:
                # Get the filter string of the clause
                filter_string: PebbleFilterString = clause["filter"]

                # Get the scope of the clause
                outer_scope = clause.get("scope", "ALL")

                # Get the (merge) operator of the clause
                outer_operator = clause.get("operator", "AND")

                # Initialize a match variable
                match: bool = filter_string.evaluate(entry=entry)

                # Invert the match if the scope is NONE
                if outer_scope == "NONE":
                    # Invert the match
                    match = not match

                # Append the match to the list
                matches.append(match)

            # Check if the entry matches any of the filters
            if matches:
                # Check if the entry matches all of the filters
                if outer_operator == "AND" and all(matches):
                    # Add the entry to the result
                    result["values"].append(entry)

                    # Increment the total
                    result["total"] += 1
                # Check if the entry matches any of the filters
                elif outer_operator == "OR" and any(matches):
                    # Add the entry to the result
                    result["values"].append(entry)

                    # Increment the total
                    result["total"] += 1

        # Return the result
        return result

    def set_filter(
        self,
        filter: PebbleFilterString,
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> "PebbleFilterEngine":
        """
        Set the filter of the engine.

        Args:
            filter: The filter to set.
            scope: The scope to use when combining the filters.

        Returns:
            The engine.
        """

        # Add the filter to the dictionary
        self._filters[filter.to_str()] = {
            "filter": filter,
            "scope": scope,
        }

        # Return the engine
        return self

    def set_filters(
        self,
        filters: list[PebbleFilterString],
        operator: Literal["AND", "OR"] = "AND",
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> "PebbleFilterEngine":
        """
        Set the filters of the engine.

        Args:
            filters (list[PebbleFilterString]): The filters to set.
            operator (Literal["AND", "OR"]): The operator to use when combining the filters.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use when combining the filters.

        Returns:
            PebbleFilterEngine: The engine.
        """

        # Iterate over the filters
        for filter in filters:
            # Add the filters to the dictionary
            self._filters[filter.to_str()] = {
                "filter": filter,
                "operator": operator,
                "scope": scope,
            }

        # Return the engine
        return self
