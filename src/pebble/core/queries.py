"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from typing import Any, Final, List, Literal, Optional

from core.filters import PebbleFilterEngine, PebbleFilterString


__all__: Final[List[str]] = []


class PebbleQueryString:
    """
    A class to represent a query string.
    """

    def __init__(
        self,
        string: str,
        flag: Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"] = "CASE_INSENSITIVE",
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> None:
        """
        Initialize a new PebbleQueryString object.

        Args:
            flag (Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.
            string (str): The string to store.

        Returns:
            None
        """

        # Initialize the engine as an instance variable
        self._engine: PebbleFilterEngine = PebbleFilterEngine(table={})

        # Initialize the filters dictionary as an instance variable
        self._filters: dict[str, list[PebbleFilterString]] = {}

        # Initialize the flag as an instance variable
        self._flag: Final[Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]] = flag

        # Initialize the parsed state as an instance variable
        self._parsed: bool = False

        # Initialize the scope as an instance variable
        self._scope: Final[Literal["ALL", "ANY", "NONE"]] = scope

        # Store the passed string str as an instance variable
        self._string: Final[str] = string

        # Initialize the queries list as an instance variable
        self._sub_queries: list[tuple[str, Optional[str]]] = []

        # Initialize the tables list as an instance variable
        self._tables: list[str] = []

        # Parse the string
        self.parse()

    def __eq__(
        self,
        other: "PebbleQueryString",
    ) -> bool:
        """
        Check if the string is equal to the other string.

        Args:
            other (PebbleQueryString): The other string to compare.

        Returns:
            bool: True if the string is equal to the other string, False otherwise.
        """

        return self._string == other._string

    def __hash__(self) -> int:
        """
        Return the hash of the string.

        Returns:
            int: The hash of the string.
        """

        return hash(self._string)

    def __repr__(self) -> str:
        """
        Return the string representation of the string.

        Returns:
            str: The string representation of the string.
        """

        return f"<{self.__class__.__name__}(string={self._string!r})>"

    def __str__(self) -> str:
        """
        Return the string representation of the string.

        Returns:
            str: The string representation of the string.
        """

        return self._string

    @property
    def filters(self) -> dict[str, list[PebbleFilterString]]:
        """
        Return the filters.

        Returns:
            dict[str, list[PebbleFilterString]]: The filters.
        """

        return dict(self._filters)

    @property
    def flag(self) -> Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]:
        """
        Return the flag.

        Returns:
            Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]: The flag.
        """

        return self._flag

    @property
    def scope(self) -> Literal["ALL", "ANY", "NONE"]:
        """
        Return the scope.

        Returns:
            Literal["ALL", "ANY", "NONE"]: The scope.
        """

        return self._scope

    @property
    def string(self) -> str:
        """
        Return the string.

        Returns:
            str: The string.
        """

        return self._string

    @property
    def sub_queries(self) -> list[tuple[str, Optional[str]]]:
        """
        Return the sub-queries.

        Returns:
            list[tuple[str, Optional[str]]]: The sub-queries.
        """

        return list(self._sub_queries)

    @property
    def tables(self) -> list[str]:
        """
        Return the tables.

        Returns:
            list[str]: The tables.
        """

        return list(self._tables)

    def __contains__(
        self,
        item: str,
    ) -> bool:
        """
        Check if the string contains the item.

        Args:
            item (str): The item to check.

        Returns:
            bool: True if the string contains the item, False otherwise.
        """

        return item in self._string

    def evaluate(
        self,
        table: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Evaluate the string.

        Args:
            table (dict[str, Any]): The table to evaluate.

        Returns:
            dict[str, Any]: The result of the evaluation.
        """

        # Initialize an empty dictionary
        result: dict[str, Any] = {
            "filter": ".".join(
                f"{f.field}.{f.operator}.{f.scope}"
                for filters in self._filters.values()
                for f in filters
            ),
            "total": 0,
            "values": [],
        }

        # Set the table of the engine
        self._engine.table = table

        # Iterate over the filters
        for filter_list in self._filters.values():
            # Set the filters of the engine
            self._engine.set_filters(filters=filter_list)

        # Filter the table and set the values
        result["values"] = self._engine.filter()

        # Set the total
        result["total"] = len(result["values"])

        # Return the result
        return result

    def parse(self) -> None:
        """
        Parse the string.

        This method attempts to split the string into queries based on the combinators.
        If the string is not split, it is considered as a single query.

        Returns:
            None
        """

        # Check if the string has already been parsed
        if self._parsed:
            # Return if the string has already been parsed
            return

        # Parse the string into queries
        self.parse_queries()

        # Iterate over the queries
        for string in self._queries:
            # Parse the string into filters
            self.parse_filters(string=string)

        # Set the parsed state of the string to True
        self._parsed = True

    def parse_filters(
        self,
        string: str,
    ) -> None:
        """
        Parse the string into filters.

        Args:
            string (str): The string to parse.

        Returns:
            None
        """

        try:
            # Initialize the filter string
            filter_string: PebbleFilterString = PebbleFilterString(
                flag=self._flag,
                string=string,
            )

            # Check if the table is not in the filters
            if filter_string.table not in self._filters:
                # Initialize the table filters
                self._filters[filter_string.table] = []

            # Set the filter
            self._filters[filter_string.table].append(filter_string)
        except PebbleFilterStringFormatError:
            # Skip if the filter string is not in the correct format
            pass

    def parse_queries(self) -> None:
        """
        Parse the string into queries.

        This method attempts to split the string into queries based on the combinators.
        If the string is not split, it is considered as a single query.

        Returns:
            None
        """

        # Check if the string has already been parsed
        if self._parsed:
            # Return if the string has already been parsed
            return

        # Find all patterns in the string
        parsed: Optional[list[dict[str, Optional[str]]]] = find_all_patterns(
            pattern=QUERY_PATTERN,
            string=self._string,
        )

        # Check if the string is in the correct format
        if parsed is None:
            # Raise a PebbleQueryStringFormatError if the string is not in the correct format
            raise PebbleQueryStringFormatError(string=self._string)

        # Add the tables to the tables list
        self._tables.extend(set([query.get("table", "") for query in parsed]))

        # Set the queries
        self._sub_queries.extend(parsed)

    def parts(self) -> list[list[str]]:
        """
        Return the parts of the string.

        Returns:
            list[list[str]]: The parts of the string.
        """

        return [filter.parts() for filter in self._filters.values()]

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """
        Return the dictionary representation of the string.

        Returns:
            dict[str, dict[str, Any]]: The dictionary representation of the string.
        """

        return {
            filter.table: filter.to_dict()
            for filter in [filter for filter in self._filters.values()]
        }

    def to_list(self) -> list[list[str]]:
        """
        Return the list representation of the string.

        Returns:
            list[list[str]]: The list representation of the string.
        """

        return [filter.to_list() for filter in [filter for filter in self._filters.values()]]

    def to_str(self) -> str:
        """
        Return the string representation of the string.

        Returns:
            str: The string representation of the string.
        """

        return self._string

    def to_tuple(self) -> tuple[tuple[str]]:
        """
        Return the tuple representation of the string.

        Returns:
            tuple[tuple[str]]: The tuple representation of the string.
        """

        return tuple(filter.to_tuple() for filter in [filter for filter in self._filters.values()])


class PebbleQueryEngine:
    """
    A class to represent a query engine.
    """

    def __init__(
        self,
        data: dict[str, Any],
    ) -> None:
        """
        Initialize a new PebbleQueryEngine object.

        Args:
            data (dict[str, Any]): The data to query.

        Returns:
            None
        """

        # Store the passed data as an instance variable
        self._data: dict[str, Any] = data

        # Initialize the filters list as an instance variable
        self._filters: list[PebbleQueryString] = []

    @property
    def data(self) -> dict[str, Any]:
        """
        Return the data.

        Returns:
            dict[str, Any]: The data.
        """

        return self._data

    @property
    def filters(self) -> list[PebbleQueryString]:
        """
        Return the filters.

        Returns:
            list[PebbleQueryString]: The filters.
        """

        return self._filters

    def query(self) -> dict[str, Any]:
        """
        Return the query results.

        Returns:
            dict[str, Any]: The query results.
        """

        # Initialize an empty list
        results: dict[str, Any] = {
            "query": "",
            "total": 0,
            "values": {},
        }

        # Iterate over the filters
        for (
            index,
            filter,
        ) in enumerate(iterable=self._filters):
            # Set the query
            results["query"] += f"{filter.to_str()}" if index == 0 else f" {filter.to_str()}"

            # Evaluate the filter
            results["values"][str(index)] = filter.evaluate(table=self._data)

        # Return the results
        return results

    def set_query(
        self,
        string: str,
        flag: Literal[
            "CASE_INSENSITIVE",
            "CASE_SENSITIVE",
        ] = "CASE_INSENSITIVE",
        scope: Literal[
            "ALL",
            "ANY",
            "NONE",
        ] = "ALL",
    ) -> "PebbleQueryEngine":
        """
        Set the query.

        Args:
            string (str): The string to set the query from.
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.

        Returns:
            PebbleQueryEngine: The query engine object.
        """

        # Initialize a new PebbleQueryString object
        query_string: PebbleQueryString = PebbleQueryString(
            string=string,
            flag=flag,
            scope=scope,
        )

        # Add the query string to the filters list
        self._filters.append(query_string)

        # Return the query engine object
        return self

    def set_queries(
        self,
        strings: list[str],
        flag: Literal[
            "CASE_INSENSITIVE",
            "CASE_SENSITIVE",
        ] = "CASE_INSENSITIVE",
        scope: Literal[
            "ALL",
            "ANY",
            "NONE",
        ] = "ALL",
    ) -> "PebbleQueryEngine":
        """
        Set the queries.

        Args:
            strings (list[str]): The strings to set the queries from.
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.

        Returns:
            PebbleQueryEngine: The query engine object.
        """

        # Iterate over the strings
        for string in strings:
            # Initialize a new PebbleQueryString object
            query_string: PebbleQueryString = PebbleQueryString(
                string=string,
                flag=flag,
                scope=scope,
            )

            # Add the query string to the filters list
            self._filters.append(query_string)

        # Return the query engine object
        return self
