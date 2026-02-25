"""
Printer compatibility condition evaluator.

Evaluates slicer compatible_printers_condition expressions against
printer configuration data, supporting comparison operators, regex,
logical operators, and nested parentheses.
"""

import re
from typing import Any, Optional


def evaluate_printer_condition(
    condition: str,
    printer_data: dict[str, Any],
    slicer: str = "bambustudio",
    defaults: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Evaluate a slicer compatible_printers_condition expression.

    Supports: ==, !=, >=, <=, =~ (regex), !~ (not regex),
    and/or/&&/||, nested parentheses, array indexing like nozzle_diameter[0].

    Args:
        condition: The condition expression string.
        printer_data: Printer configuration data to evaluate against.
        slicer: Slicer type string (affects value parsing).
        defaults: Optional default printer configuration to use as base.

    Returns:
        True if the condition is satisfied, False otherwise.
    """
    if not condition or not condition.strip():
        return False

    # Merge defaults with provided printer data
    merged_data = {}
    if defaults:
        merged_data.update(defaults)
    merged_data.update(printer_data)

    var_re = re.compile(r"(?P<key>\w+)(?:\[(?P<index>[0-9]+)])?")

    def get_value_from_config(key: str):
        match = var_re.fullmatch(key)
        if not match:
            return None

        groups = match.groupdict()
        if groups["key"] in merged_data:
            value = merged_data[groups["key"]]
        elif key == "num_extruders":
            extruders_count = get_value_from_config("extruders_count")
            if extruders_count is not None:
                return extruders_count
            nozzle_diameters = get_value_from_config("nozzle_diameter")
            if nozzle_diameters is not None:
                return len(re.split("[;,]", nozzle_diameters))
            return None
        else:
            return None

        if groups["index"] is None:
            if slicer != "prusaslicer" and isinstance(value, list) and len(value) == 1:
                value = value[0]
        else:
            if slicer == "prusaslicer":
                value = re.split("[;,]", value)
            value = value[int(groups["index"])]

        return value

    return _evaluate_expression(condition, get_value_from_config, slicer)


def _evaluate_expression(
    condition: str,
    get_value: callable,
    slicer: str,
) -> bool:
    """Evaluate a condition expression, handling parentheses recursively."""

    # Handle parentheses by evaluating inner expressions first
    if "(" in condition:
        if condition.count("(") != condition.count(")"):
            raise ValueError("Unmatched parentheses in condition")

        while True:
            result = _find_first_parenthesis_set(condition)
            if result is None:
                break
            start_idx, end_idx = result
            inner = condition[start_idx + 1 : end_idx]
            inner_result = _evaluate_expression(inner, get_value, slicer)
            condition = (
                condition[:start_idx]
                + str(inner_result)
                + condition[end_idx + 1 :]
            )
    elif ")" in condition:
        raise ValueError("Unmatched parentheses in condition")

    # Split on logical operators
    parts = [x for x in re.split(r" and | && | or | \|\|", condition)]

    # Extract operators in order
    operators = []
    for word in condition.split():
        w = word.strip()
        if w in ("and", "&&"):
            operators.append("and")
        elif w in ("or", "||"):
            operators.append("or")

    all_and = "or" not in operators
    all_or = "and" not in operators

    prev_ret = None
    for idx, part in enumerate(parts):
        part = part.strip()

        is_not = False
        if part.startswith("! "):
            is_not = True
            part = part[2:]

        ret = _evaluate_single_condition(part, get_value)

        if is_not:
            ret = not ret

        # Short-circuit
        if all_and and ret is False:
            return False
        elif all_or and ret is True:
            return True

        if prev_ret is None:
            prev_ret = ret
            continue

        op = operators[idx - 1]
        if op == "and":
            prev_ret = prev_ret and ret
        elif op == "or":
            prev_ret = prev_ret or ret

    return prev_ret


_OPERATORS = ["=~", "!~", "==", "!=", ">=", "<=", ">", "<"]


def _evaluate_single_condition(condition: str, get_value: callable) -> bool:
    """Evaluate a single condition (no logical operators)."""
    condition = condition.strip()

    if condition.lower() == "true":
        return True
    if condition.lower() == "false":
        return False

    # Find which operator is used
    operator = None
    for op in _OPERATORS:
        if op in condition:
            operator = op
            break

    if operator is None:
        # No operator — check if config value is truthy
        config_value = get_value(condition)
        return config_value == "1" or config_value == "true"

    operands = condition.split(operator)
    if len(operands) != 2:
        return False

    config_key, check_value = operands
    config_value = get_value(config_key.strip())

    if config_value is None:
        return False

    def remove_quotes(s: str) -> str:
        s = s.strip()
        if s.startswith('"') and s.endswith('"'):
            return s[1:-1]
        return s

    if operator == "=~":
        pattern = check_value.strip().strip("/")
        return re.match(pattern, str(config_value), re.DOTALL) is not None
    elif operator == "!~":
        pattern = check_value.strip().strip("/")
        return re.match(pattern, str(config_value), re.DOTALL) is None
    elif operator == "==":
        return str(config_value) == remove_quotes(check_value)
    elif operator == "!=":
        return str(config_value) != remove_quotes(check_value)
    elif operator in (">=", "<=", ">", "<"):
        try:
            lhs = float(config_value)
            rhs = float(check_value.strip())
        except (ValueError, TypeError):
            return False
        if operator == ">=":
            return lhs >= rhs
        elif operator == "<=":
            return lhs <= rhs
        elif operator == ">":
            return lhs > rhs
        elif operator == "<":
            return lhs < rhs

    return False


def _find_first_parenthesis_set(string: str) -> Optional[tuple[int, int]]:
    """Return indices of the first balanced parenthesis pair, skipping regex patterns."""
    depth = 0
    is_in_regex = False
    start_idx = 0

    for idx in range(len(string)):
        char = string[idx]

        if char == "/":
            if is_in_regex:
                is_in_regex = False
            else:
                test_slice = string[idx - 2 : idx] if idx >= 2 else ""
                if test_slice in ("=~", "!~"):
                    is_in_regex = True

        if is_in_regex:
            continue

        if char == "(":
            if depth == 0:
                start_idx = idx
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return start_idx, idx

    return None
