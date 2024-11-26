"""Functions for creating SQL aggregation specifications.

This module provides a simple interface for creating SQL aggregation specifications, particularly useful for building dynamic SQL queries with COUNT, SUM, and AVG functions. The specifications can handle both direct column aggregations and conditional aggregations using CASE statements.

Basic usage:
    >>> count('total_users', column='user_id')
    >>> count('active_users', condition="status = 'active'")
    >>> sum('total_amount', column='amount')
    >>> avg('score', condition="is_valid = 1", then='score * weight')

Each function returns a dictionary specification that can be used to generate SQL queries.
The specifications support:
- Simple column aggregations
- Conditional aggregations with CASE statements
- Custom THEN values/expressions
- DISTINCT aggregations
- Combined conditions using AND/OR

Example of combining conditions:
    >>> spec = count('premium_active',
    ...     condition=combine_conditions(
    ...         "status = 'active'",
    ...         "type = 'premium'"
    ...     ))
"""

from enum import Enum


class AggFunc(str, Enum):
    SUM = 'SUM'
    COUNT = 'COUNT'
    AVG = 'AVG'
    MAX = 'MAX'
    MIN = 'MIN'


def create_spec(
    name: str,
    column: str | None = None,
    condition: str | None = None,
    then: str | int = 1,
    distinct: bool = False,
    aggfunc: AggFunc = AggFunc.COUNT
) -> dict:
    """Create a base specification dictionary for SQL aggregations.

    Parameters:
    - name (str): Name of the resulting column.
    - column (str | None): Column to aggregate. Mutually exclusive with condition.
    - condition (str | None): SQL condition for CASE statement. Mutually exclusive with column.
    - then (str | int): Value or expression to use after THEN in CASE statement.
    - distinct (bool): Whether to use DISTINCT in aggregation.
    - aggfunc (AggFunc): Aggregation function to use.

    Returns:
    - dict: Specification dictionary for SQL generation.

    Raises:
    - ValueError: If both column and condition are specified or neither is specified.
    """
    if column is not None and condition is not None:
        raise ValueError("Cannot specify both column and condition")
    if column is None and condition is None:
        raise ValueError("Must specify either column or condition")

    result = {
        'name': name,
        'column': column,
        'distinct': distinct,
        'aggfunc': aggfunc.value
    }

    if condition:
        result['case'] = f"WHEN {condition} THEN {then}"

    return result


def count(
    name: str,
    column: str | None = None,
    condition: str | None = None,
    then: str | int = 1,
    distinct: bool = False
) -> dict:
    """Create a COUNT specification.

    Parameters:
    - name (str): Name of the resulting column.
    - column (str | None): Column to count. Mutually exclusive with condition.
    - condition (str | None): SQL condition for CASE statement. Mutually exclusive with column.
    - then (str | int): Value or expression to count when condition is met.
    - distinct (bool): Whether to use DISTINCT in the COUNT.

    Returns:
    - dict: Specification dictionary for COUNT query.

    Examples:
        >>> count('total_users', column='user_id')
        >>> count('new_users', condition="status = 'new'")
        >>> count('weighted_users', condition="status = 'premium'", then='weight')
        >>> count('unique_logins', column='login_id', distinct=True)
    """
    return create_spec(
        name=name,
        column=column,
        condition=condition,
        then=then,
        distinct=distinct,
        aggfunc=AggFunc.COUNT
    )


def sum(
    name: str,
    column: str | None = None,
    condition: str | None = None,
    then: str | int = 1,
    distinct: bool = False
) -> dict:
    """Create a SUM specification.

    Parameters:
    - name (str): Name of the resulting column.
    - column (str | None): Column to sum. Mutually exclusive with condition.
    - condition (str | None): SQL condition for CASE statement. Mutually exclusive with column.
    - then (str | int): Value or expression to sum when condition is met.
    - distinct (bool): Whether to use DISTINCT in the SUM.

    Returns:
    - dict: Specification dictionary for SUM query.

    Examples:
        >>> sum('total_amount', column='amount')
        >>> sum('premium_amount', condition="type = 'premium'", then='amount')
        >>> sum('weighted_total', condition="is_valid = 1", then='amount * weight')
    """
    return create_spec(
        name=name,
        column=column,
        condition=condition,
        then=then,
        distinct=distinct,
        aggfunc=AggFunc.SUM
    )


def avg(
    name: str,
    column: str | None = None,
    condition: str | None = None,
    then: str | int = 1,
    distinct: bool = False
) -> dict:
    """Create an AVG specification.

    Parameters:
    - name (str): Name of the resulting column.
    - column (str | None): Column to average. Mutually exclusive with condition.
    - condition (str | None): SQL condition for CASE statement. Mutually exclusive with column.
    - then (str | int): Value or expression to average when condition is met.
    - distinct (bool): Whether to use DISTINCT in the AVG.

    Returns:
    - dict: Specification dictionary for AVG query.

    Examples:
        >>> avg('avg_amount', column='amount')
        >>> avg('premium_avg', condition="type = 'premium'", then='amount')
        >>> avg('weighted_avg', condition="is_valid = 1", then='amount * weight')
    """
    return create_spec(
        name=name,
        column=column,
        condition=condition,
        then=then,
        distinct=distinct,
        aggfunc=AggFunc.AVG
    )


def combine_conditions(*conditions: str, operator: str = 'AND') -> str:
    """Combine multiple SQL conditions with a specified operator.

    Parameters:
    - *conditions (str): Variable number of SQL condition strings.
    - operator (str): The operator to join conditions with ('AND' or 'OR').

    Returns:
    - str: Combined SQL condition string.

    Examples:
        >>> combine_conditions("status = 'active'", "type = 'premium'")
        "status = 'active' AND type = 'premium'"
        >>> combine_conditions("type = 'trial'", "type = 'premium'", operator='OR')
        "type = 'trial' OR type = 'premium'"
    """
    return f" {operator} ".join(f"({cond})" for cond in conditions)
