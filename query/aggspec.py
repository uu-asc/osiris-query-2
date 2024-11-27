"""SQL aggregation specification builder with an immutable fluent interface.

This module provides a flexible builder pattern for creating SQL aggregation specifications, particularly useful for building dynamic SQL queries with COUNT, SUM, AVG, and other aggregate functions. The specifications can handle both direct column aggregations and conditional aggregations using CASE statements.

Key Features:
- Immutable builder pattern with method chaining
- Support for all major SQL aggregation functions
- Flexible ordering of builder methods
- Built-in validation
- Support for complex CASE conditions

Basic Usage:
    >>> spec = count.column("user_id").name("total_users").build()
    >>> spec = count.case("status = 'active'").name("active_users").build()
    >>> spec = sum.column("amount").name("total_amount").build()
    >>> spec = avg.column("score").distinct(True).name("avg_score").build()

Complex Conditions:
    >>> spec = (
    ...     count
    ...     .case(["status = 'active'", "type = 'premium'"])
    ...     .name("premium_active")
    ...     .build()
    ... )
    >>> spec = (
    ...     sum
    ...     .case("status = 'active'")
    ...     .then("amount * weight")
    ...     .name("weighted_total")
    ...     .build()
    ... )

Pre-configured Builders:
    The module provides pre-configured builders for common aggregation functions:
    - count: COUNT aggregation
    - sum: SUM aggregation
    - avg: AVG aggregation
    - max: MAX aggregation
    - min: MIN aggregation
    - listagg: LISTAGG aggregation

Each builder method can act as both a getter and setter:
    >>> agg = count.name("total")
    >>> agg.name()  # Returns "total"

Validation is performed at build time or can be done explicitly:
    >>> agg = count.case("status = 'active'")
    >>> try:
    ...     agg.validate()
    ... except ValidationError as e:
    ...     print(f"Invalid spec: {e}")
"""

from copy import deepcopy
from enum import Enum
from functools import wraps
from typing import Any, Callable, Self


class AggFunc(str, Enum):
    SUM = 'SUM'
    COUNT = 'COUNT'
    AVG = 'AVG'
    MAX = 'MAX'
    MIN = 'MIN'
    LISTAGG = 'LISTAGG'


class ValidationError(ValueError):
    """Custom exception for aggregation specification validation errors"""
    pass


class AggregationBuilder:
    """
    Builder for SQL aggregation specifications with immutable chain operations.

    This class implements a flexible builder pattern where methods can be chained in any order, with validation performed at build time. Each modification returns a new instance. All setter methods can also act as getters when called without arguments.

    Parameters:
    - aggfunc (AggFunc): The aggregation function to use (COUNT, SUM, AVG, etc.)

    Examples:
    - Basic usage with flexible ordering:
        >>> spec = (
        ...     AggregationBuilder(AggFunc.COUNT)
        ...     .case("type = 'premium'")
        ...     .name("premium_users")
        ...     .build()
        ... )

    - Complex case conditions:
        >>> spec = (
        ...     sum
        ...     .case(["status = 'active'", "type = 'premium'"])
        ...     .then("amount")
        ...     .name("premium_revenue")
        ...     .build()
        ... )

    - Using as getter:
        >>> agg = sum.name("total").column("amount")
        >>> agg.name()  # Returns "total"
        >>> agg.validate()  # Optional early validation
    """
    def __init__(self, aggfunc: AggFunc):
        self._spec = {
            'aggfunc': aggfunc.value,
            'distinct': False
        }

    def name(self, value: str | None = None) -> str | Self:
        """Get or set the name for the aggregation column"""
        if value is None:
            return self._spec.get('name')

        new = self.copy()
        new._spec['name'] = value
        return new

    def column(self, value: str | None = None) -> str | Self:
        """Get or set the column to aggregate"""
        if value is None:
            return self._spec.get('column')

        new = self.copy()
        new._spec['column'] = value
        # If no name is set, use column name as default
        if 'name' not in new._spec:
            new._spec['name'] = value
        return new

    def case(
        self,
        condition: str | list[str] | None = None,
        operator: str = 'AND'
    ) -> str | Self:
        """Get or set a CASE condition

        Parameters:
        - condition (str | list[str] | None): Single SQL condition or list of conditions.
          If None, returns current case value.
        - operator (str): The operator to join multiple conditions ('AND' or 'OR').
          Only used when condition is a list.

        Returns:
        - str | Self: Current case value if condition is None, else new AggregationBuilder

        Examples:
            >>> agg.case("status = 'active'").name("active_count")
            >>> agg.name("premium_total").case(["status = 'active'", "type = 'premium'"])
            >>> agg.case(["type = 'trial'", "type = 'premium'"], operator='OR')
        """
        if condition is None:
            return self._spec.get('case')

        new = self.copy()

        # Handle list of conditions
        if isinstance(condition, list):
            joined_condition = f" {operator} ".join(f"({i})" for i in condition)
            new._spec['case'] = f"WHEN {joined_condition}"
        else:
            new._spec['case'] = f"WHEN {condition}"

        # Add THEN clause
        if 'then' not in new._spec:
            new._spec['case'] += " THEN 1"
        else:
            new._spec['case'] += f" THEN {new._spec['then']}"

        return new

    def then(self, value: str|int|None = None) -> str|int|Self:
        """Get or set the THEN value for CASE statement"""
        if value is None:
            return self._spec.get('then')

        new = self.copy()
        new._spec['then'] = value
        if 'case' in new._spec:
            # Update existing case statement with new then value
            new._spec['case'] = f"{new._spec['case'].split(' THEN')[0]} THEN {value}"
        return new

    def distinct(self, value: bool|None = None) -> bool|Self:
        """Get or set whether to use DISTINCT"""
        if value is None:
            return self._spec.get('distinct', False)

        new = self.copy()
        new._spec['distinct'] = value
        return new

    def copy(self) -> Self:
        """Create a copy of the current builder"""
        new = AggregationBuilder(AggFunc(self._spec['aggfunc']))
        new._spec = deepcopy(self._spec)
        return new

    def validate(self) -> None:
        """
        Validate the current specification without building it.

        Raises:
        - ValidationError: If the specification is invalid

        This method can be called explicitly to check validity before building:
        >>> agg = count.case("type = 'premium'")
        >>> try:
        ...     agg.validate()
        ... except ValidationError as e:
        ...     print(f"Invalid spec: {e}")
        """
        errors = []

        # Check that either column or case is specified
        if 'column' not in self._spec and 'case' not in self._spec:
            errors.append("Must specify either column or case")

        # Check that column and case aren't both specified
        if 'column' in self._spec and 'case' in self._spec:
            errors.append("Cannot specify both column and case")

        # Check that case has a name
        if 'case' in self._spec and 'name' not in self._spec:
            errors.append("Name must be specified when using case condition")

        # Check for invalid operator combinations
        if ('then' in self._spec and 'case' not in self._spec):
            errors.append("THEN clause specified without CASE condition")

        if errors:
            raise ValidationError("\n".join(errors))

    def build(self) -> dict:
        """
        Build and validate the specification dictionary.

        Returns:
        - dict: The complete and validated specification

        Raises:
        - ValidationError: If the specification is invalid

        Examples:
            >>> spec = count.name("active").case("status = 'active'").build()
            >>> spec = sum.column("amount").distinct(True).build()
        """
        self.validate()
        # Create a copy and remove 'then' since it's already part of 'case'
        spec = dict(self._spec)
        if 'then' in spec:
            del spec['then']
        return spec


# Pre-configured builders for common aggregation functions
count = AggregationBuilder(AggFunc.COUNT)
sum = AggregationBuilder(AggFunc.SUM)
avg = AggregationBuilder(AggFunc.AVG)
max = AggregationBuilder(AggFunc.MAX)
min = AggregationBuilder(AggFunc.MIN)
listagg = AggregationBuilder(AggFunc.LISTAGG)


def build_value_specs(func: Callable) -> Callable:
    """Decorator that builds AggregationBuilder instances in the 'values' kwarg.

    If 'values' contains an AggregationBuilder or a list with AggregationBuilders,
    they will be built into dictionaries before being passed to the decorated function.

    Parameters:
    - func (Callable): The function to decorate

    Returns:
    - Callable: The decorated function
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if 'values' not in kwargs:
            return func(*args, **kwargs)

        values = kwargs['values']

        # Handle single AggregationBuilder
        if isinstance(values, AggregationBuilder):
            kwargs['values'] = values.build()
        # Handle list of values
        elif isinstance(values, list):
            is_builder = lambda i: isinstance(i, AggregationBuilder)
            kwargs['values'] = [
                val.build() if is_builder(val) else val
                for val in values
            ]

        return func(*args, **kwargs)

    return wrapper
