"""Cura 5 profile graph normalisation.

Cura resources are not standalone profiles.  A usable setting stack is built
from inherited machine and extruder definitions, a hardware variant, an XML
material and one or more quality/intent instances.  This module keeps that
composition metadata separate from the flat settings consumed by CuraEngine.

The normalised single-material contract is:

* ``ParsedProfile.settings``: concrete Cura setting key/value pairs only;
* ``ParsedProfile.context``: upstream IDs and compatibility information;
* ``ParsedProfile.setting_scopes``: setting key -> ``global``/``extruder.N``.

Expressions are evaluated by a deliberately small AST interpreter.  Unknown
or unsupported expressions fall back to the setting's concrete default and
are never copied into engine settings as executable strings.
"""

from __future__ import annotations

import ast
import configparser
import copy
import json
import logging
import math
import operator
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .base import BaseParser
from ..models import ParsedProfile, ProfileType, SlicerType

logger = logging.getLogger(__name__)


def _resource_context(resource_version: str | None) -> dict[str, str]:
    """Attach source provenance only when the caller knows the snapshot."""
    return {"resource_version": resource_version} if resource_version else {}


# Names used by Cura's XmlMaterialProfile plug-in.  Keep this aligned with
# ``XmlMaterialProfile.__material_settings_setting_map`` in the pinned Cura
# source rather than forwarding human-readable XML keys to CuraEngine.
MATERIAL_SETTING_MAP = {
    "print temperature": "default_material_print_temperature",
    "heated bed temperature": "default_material_bed_temperature",
    "standby temperature": "material_standby_temperature",
    "processing temperature graph": "material_flow_temp_graph",
    "print cooling": "cool_fan_speed",
    "retraction amount": "retraction_amount",
    "retraction speed": "retraction_speed",
    "adhesion tendency": "material_adhesion_tendency",
    "surface energy": "material_surface_energy",
    "build volume temperature": "build_volume_temperature",
    "anti ooze retract position": "material_anti_ooze_retracted_position",
    "anti ooze retract speed": "material_anti_ooze_retraction_speed",
    "break preparation position": "material_break_preparation_retracted_position",
    "break preparation speed": "material_break_preparation_speed",
    "break preparation temperature": "material_break_preparation_temperature",
    "break position": "material_break_retracted_position",
    "flush purge speed": "material_flush_purge_speed",
    "flush purge length": "material_flush_purge_length",
    "end of filament purge speed": "material_end_of_filament_purge_speed",
    "end of filament purge length": "material_end_of_filament_purge_length",
    "maximum park duration": "material_maximum_park_duration",
    "no load move factor": "material_no_load_move_factor",
    "break speed": "material_break_speed",
    "break temperature": "material_break_temperature",
}

_MATH_FUNCTIONS = {
    "atan",
    "ceil",
    "degrees",
    "floor",
    "radians",
    "sqrt",
    "tan",
}

CURA_MATERIAL_RECOMPUTE_PLAN = "_cura_material_recompute_plan"
_HIGH_FLOW_VARIANT_RE = re.compile(
    r"(?:^|[^a-z0-9])(?:super[\s_-]*volcano|volcano|cht|high[\s_-]*flow|hf)(?=$|[^a-z0-9]|\d)",
    re.IGNORECASE,
)


class _UnresolvedExpression(ValueError):
    """An expression depends on a setting that is not concrete yet."""


class _UnsafeExpression(ValueError):
    """An expression uses syntax outside Cura's supported safe subset."""


class _SafeExpressionEvaluator:
    """Evaluate Cura setting expressions without using ``eval``.

    Cura's shipped definitions primarily use arithmetic, comparisons and a
    small group of aggregate helpers.  Multi-extruder helpers intentionally
    collapse to extruder zero for the currently supported golden path.
    """

    _BINARY_OPERATORS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
    }
    _UNARY_OPERATORS = {
        ast.UAdd: operator.pos,
        ast.USub: operator.neg,
        ast.Not: operator.not_,
    }
    _COMPARE_OPERATORS = {
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.In: lambda left, right: left in right,
        ast.NotIn: lambda left, right: left not in right,
        ast.Is: operator.is_,
        ast.IsNot: operator.is_not,
    }

    def __init__(self, values: Mapping[str, Any]):
        self.values = values

    def evaluate(self, expression: str) -> Any:
        try:
            tree = ast.parse(expression, mode="eval")
        except (SyntaxError, ValueError) as exc:
            raise _UnresolvedExpression(expression) from exc
        if sum(1 for _ in ast.walk(tree)) > 512:
            raise _UnsafeExpression("expression is too large")
        return self._visit(tree.body)

    def _visit(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in self.values:
                return self.values[node.id]
            functions = {
                "abs": abs,
                "any": any,
                "bool": bool,
                "float": float,
                "int": int,
                "len": len,
                "map": lambda function, values: list(map(function, values)),
                "max": max,
                "min": min,
                "round": round,
                "str": str,
                "sum": sum,
            }
            if node.id in functions:
                return functions[node.id]
            raise _UnresolvedExpression(node.id)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            values = [self._visit(item) for item in node.elts]
            if isinstance(node, ast.Tuple):
                return tuple(values)
            if isinstance(node, ast.Set):
                return set(values)
            return values
        if isinstance(node, ast.Dict):
            return {
                self._visit(key): self._visit(value)
                for key, value in zip(node.keys, node.values)
                if key is not None
            }
        if isinstance(node, ast.BinOp):
            function = self._BINARY_OPERATORS.get(type(node.op))
            if function is None:
                raise _UnsafeExpression(type(node.op).__name__)
            left = self._visit(node.left)
            right = self._visit(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 16:
                raise _UnsafeExpression("power is outside the safe range")
            return function(left, right)
        if isinstance(node, ast.UnaryOp):
            function = self._UNARY_OPERATORS.get(type(node.op))
            if function is None:
                raise _UnsafeExpression(type(node.op).__name__)
            return function(self._visit(node.operand))
        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                result: Any = True
                for value in node.values:
                    result = self._visit(value)
                    if not result:
                        return result
                return result
            if isinstance(node.op, ast.Or):
                result = False
                for value in node.values:
                    result = self._visit(value)
                    if result:
                        return result
                return result
            raise _UnsafeExpression(type(node.op).__name__)
        if isinstance(node, ast.Compare):
            left = self._visit(node.left)
            for operation, comparator in zip(node.ops, node.comparators):
                right = self._visit(comparator)
                function = self._COMPARE_OPERATORS.get(type(operation))
                if function is None or not function(left, right):
                    return False
                left = right
            return True
        if isinstance(node, ast.IfExp):
            branch = node.body if self._visit(node.test) else node.orelse
            return self._visit(branch)
        if isinstance(node, ast.Subscript):
            return self._visit(node.value)[self._visit(node.slice)]
        if isinstance(node, ast.Slice):
            return slice(
                self._visit(node.lower) if node.lower else None,
                self._visit(node.upper) if node.upper else None,
                self._visit(node.step) if node.step else None,
            )
        if isinstance(node, ast.Attribute):
            if (
                isinstance(node.value, ast.Name)
                and node.value.id == "math"
                and node.attr in _MATH_FUNCTIONS
            ):
                return getattr(math, node.attr)
            value = self._visit(node.value)
            if node.attr == "index" and isinstance(value, (list, tuple, str)):
                return value.index
            raise _UnsafeExpression(node.attr)
        if isinstance(node, ast.Call):
            return self._call(node)
        raise _UnsafeExpression(type(node).__name__)

    def _call(self, node: ast.Call) -> Any:
        if isinstance(node.func, ast.Name):
            name = node.func.id
            if name in {
                "extruderValue",
                "extruderValueFromContainer",
                "extruderValues",
                "resolveOrValue",
                "anyExtruderWithMaterial",
                "defaultExtruderPosition",
            }:
                args = [self._visit(arg) for arg in node.args]
                return self._cura_helper(name, args)

        function = self._visit(node.func)
        if function not in {
            abs,
            any,
            bool,
            float,
            int,
            len,
            max,
            min,
            round,
            str,
            sum,
        } and not (
            callable(function)
            and (
                getattr(function, "__module__", "") == "math"
                or getattr(function, "__name__", "") == "<lambda>"
                or getattr(function, "__name__", "") == "index"
            )
        ):
            raise _UnsafeExpression("function is not allowed")
        args = [self._visit(arg) for arg in node.args]
        kwargs = {keyword.arg: self._visit(keyword.value) for keyword in node.keywords}
        try:
            return function(*args, **kwargs)
        except (ArithmeticError, TypeError, ValueError, IndexError) as exc:
            raise _UnresolvedExpression(str(exc)) from exc

    def _cura_helper(self, name: str, args: list[Any]) -> Any:
        if name == "defaultExtruderPosition":
            return 0
        if not args:
            raise _UnresolvedExpression(name)
        if name in {"resolveOrValue", "extruderValues"}:
            key = args[0]
        elif len(args) >= 2:
            key = args[1]
        else:
            raise _UnresolvedExpression(name)
        if not isinstance(key, str) or key not in self.values:
            raise _UnresolvedExpression(str(key))
        value = self.values[key]
        if name == "extruderValues":
            return [value]
        if name == "anyExtruderWithMaterial":
            return bool(value)
        return value


def _resource_id(path: Path) -> str:
    name = path.name
    for suffix in (".def.json", ".inst.cfg", ".xml.fdm_material", ".fdm_material"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return path.stem


_RESOURCE_MEDIA_TYPES = {
    ".3mf": "model/3mf",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".obj": "model/obj",
    ".png": "image/png",
    ".stl": "model/stl",
    ".svg": "image/svg+xml",
}


def _asset_descriptor(reference: Any) -> dict[str, Any] | None:
    """Describe a source asset without coupling consumers to file names."""
    if not isinstance(reference, str) or not reference:
        return None
    suffix = Path(reference).suffix.lower()
    descriptor: dict[str, Any] = {
        "ref": reference,
        "format": suffix.lstrip("."),
    }
    media_type = _RESOURCE_MEDIA_TYPES.get(suffix)
    if media_type:
        descriptor["media_type"] = media_type
    return descriptor


def _bed_assets(
    metadata: Mapping[str, Any], machine_values: Mapping[str, Any]
) -> dict[str, Any]:
    """Normalise Cura platform metadata into generic typed bed assets."""
    model = _asset_descriptor(metadata.get("platform"))
    texture = _asset_descriptor(metadata.get("platform_texture"))
    if model is None and texture is None:
        return {}

    assets: dict[str, Any] = {
        "coordinate_space": "machine_build_volume",
        # Cura platform assets are authored around their local origin.  That
        # origin maps to the build-volume centre; their bounds may include a
        # frame and must not be used to recenter the asset.
        "origin": "build_volume_center",
    }
    width = machine_values.get("machine_width")
    depth = machine_values.get("machine_depth")
    if width is not None and depth is not None:
        assets["build_volume"] = {"width": width, "depth": depth}

    if model is not None:
        offset = metadata.get("platform_offset")
        if isinstance(offset, (list, tuple)) and len(offset) >= 3:
            model["transform"] = {
                "translation": list(offset[:3]),
                "unit": "mm",
            }
        assets["model"] = model
    if texture is not None:
        assets["texture"] = texture
    return assets


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _scene_context(machine_values: Mapping[str, Any]) -> dict[str, Any] | None:
    """Describe the machine-space scene transform without engine-side guesses.

    SimplyPrint exports STL vertices in the selected machine's native build
    volume coordinates. Cura's mesh loader consumes XY coordinates about the
    build-volume centre, while native machine coordinates may start at the
    lower-left. The affine terms keep user-editable mesh positions and build
    dimensions live instead of freezing their imported values into an offset.
    """

    center_is_zero = machine_values.get("machine_center_is_zero")
    centered = center_is_zero in (True, 1, "1", "true", "True")
    width = _finite_number(machine_values.get("machine_width"))
    depth = _finite_number(machine_values.get("machine_depth"))
    if width is None or width <= 0 or depth is None or depth <= 0:
        return None

    origin_setting = (
        "machine_center_is_zero" if "machine_center_is_zero" in machine_values else None
    )

    def axis_terms(
        position_key: str, dimension_key: str | None = None
    ) -> dict[str, Any]:
        terms: list[dict[str, Any]] = []
        if _finite_number(machine_values.get(position_key)) is not None:
            terms.append({"setting": position_key, "scale": 1.0})
        if dimension_key is not None:
            correction: dict[str, Any] = {
                "setting": dimension_key,
                "scale": -0.5,
            }
            if origin_setting is not None:
                correction["when"] = {
                    "setting": origin_setting,
                    "equals": False,
                }
                terms.append(correction)
            elif not centered:
                terms.append(correction)
        return {"constant": 0.0, "terms": terms}

    origin: str | dict[str, Any]
    if origin_setting is not None:
        origin = {
            "setting": origin_setting,
            "values": {
                "true": "build_volume_center",
                "false": "build_volume_minimum_xy",
            },
        }
    else:
        origin = "build_volume_center" if centered else "build_volume_minimum_xy"

    return {
        "schema": "scene-transform.v1",
        "coordinate_space": "machine_build_volume",
        "origin": origin,
        "model_transform": {
            "translation": {
                "unit": "mm",
                "x": axis_terms("mesh_position_x", "machine_width"),
                "y": axis_terms("mesh_position_y", "machine_depth"),
                "z": axis_terms("mesh_position_z"),
            }
        },
    }


def _parse_compatible_value(value: Any, default: bool = True) -> bool:
    """Match Cura's material compatibility semantics for source metadata."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in {"yes", "unknown", "true", "1"}


def _deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(base))
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _flatten_setting_tree(
    tree: Mapping[str, Any], output: dict[str, dict[str, Any]]
) -> None:
    for key, value in tree.items():
        if not isinstance(value, Mapping):
            continue
        properties = {k: copy.deepcopy(v) for k, v in value.items() if k != "children"}
        if properties.get("type") != "category" and properties:
            output[key] = _deep_merge(output.get(key, {}), properties)
        children = value.get("children")
        if isinstance(children, Mapping):
            _flatten_setting_tree(children, output)


def _definition_schema(data: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    settings = data.get("settings")
    if isinstance(settings, Mapping):
        _flatten_setting_tree(settings, output)
    overrides = data.get("overrides")
    if isinstance(overrides, Mapping):
        for key, properties in overrides.items():
            if isinstance(properties, Mapping):
                output[key] = _deep_merge(output.get(key, {}), properties)
            else:
                output[key] = _deep_merge(
                    output.get(key, {}), {"default_value": properties}
                )
    return output


def _merge_schemas(
    base: Mapping[str, Mapping[str, Any]],
    overlay: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    result = {key: copy.deepcopy(dict(value)) for key, value in base.items()}
    for key, value in overlay.items():
        result[key] = _deep_merge(result.get(key, {}), value)
    return result


@dataclass(frozen=True)
class _ResolvedDefinition:
    native_id: str
    name: str
    metadata: dict[str, Any]
    schema: dict[str, dict[str, Any]]
    inheritance: tuple[str, ...]


class _DefinitionGraph:
    def __init__(self, paths: Sequence[Path]):
        self._raw: dict[str, tuple[Path, dict[str, Any]]] = {}
        self._cache: dict[str, _ResolvedDefinition] = {}
        for path in sorted(paths):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Skipping invalid Cura definition %s: %s", path, exc)
                continue
            self._raw[_resource_id(path)] = (path, data)

    def __contains__(self, native_id: str) -> bool:
        return native_id in self._raw

    @property
    def ids(self) -> list[str]:
        return sorted(self._raw)

    def resolve(self, native_id: str) -> _ResolvedDefinition:
        return self._resolve(native_id, ())

    def _resolve(
        self, native_id: str, visiting: tuple[str, ...]
    ) -> _ResolvedDefinition:
        cached = self._cache.get(native_id)
        if cached is not None:
            return cached
        if native_id in visiting:
            chain = " -> ".join((*visiting, native_id))
            raise ValueError(f"Cura definition inheritance cycle: {chain}")
        if native_id not in self._raw:
            raise KeyError(native_id)

        _, data = self._raw[native_id]
        parent_id = data.get("inherits")
        metadata: dict[str, Any] = {}
        schema: dict[str, dict[str, Any]] = {}
        inheritance: tuple[str, ...] = ()
        if isinstance(parent_id, str) and parent_id:
            if parent_id not in self._raw:
                logger.warning(
                    "Cura definition %s inherits missing %s", native_id, parent_id
                )
            else:
                parent = self._resolve(parent_id, (*visiting, native_id))
                metadata = parent.metadata
                schema = parent.schema
                inheritance = (*parent.inheritance, parent.native_id)

        raw_metadata = data.get("metadata", {})
        if isinstance(raw_metadata, Mapping):
            metadata = _deep_merge(metadata, raw_metadata)
        schema = _merge_schemas(schema, _definition_schema(data))
        result = _ResolvedDefinition(
            native_id=native_id,
            name=str(data.get("name") or native_id),
            metadata=metadata,
            schema=schema,
            inheritance=inheritance,
        )
        self._cache[native_id] = result
        return result


def _coerce_scalar(value: Any, setting_type: str | None = None) -> Any:
    if setting_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().casefold()
            if normalized in {"true", "yes", "1", "on"}:
                return True
            if normalized in {"false", "no", "0", "off"}:
                return False
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if setting_type in {"str", "enum", "polygon", "polygons", "intlist"}:
        return stripped
    lowered = stripped.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"none", "null"}:
        return None
    try:
        parsed = ast.literal_eval(stripped)
    except (SyntaxError, ValueError):
        return stripped
    if isinstance(parsed, (str, int, float, bool, list, tuple, dict)) or parsed is None:
        return parsed
    return stripped


def _scope_for(properties: Mapping[str, Any]) -> str:
    # Cura registers this property with default=True in CuraApplication.
    return "extruder.0" if properties.get("settable_per_extruder", True) else "global"


def _overlay_expression(properties: Mapping[str, Any]) -> str | None:
    """Return the active-tool expression used after a role overlay.

    Global Cura settings can expose both an extruder-local ``value`` and a
    stack ``resolve`` expression.  The cloud path currently composes one
    active extruder, so evaluating the local value first and then publishing
    it in the global stack is equivalent to Cura's one-element resolve.
    """

    value = properties.get("value")
    if isinstance(value, str):
        return value
    resolve = properties.get("resolve")
    return resolve if isinstance(resolve, str) else None


def _expression_dependencies(expression: str, schema_keys: set[str]) -> set[str]:
    """Extract setting references from a Cura expression without executing it."""

    try:
        tree = ast.parse(expression, mode="eval")
    except (SyntaxError, ValueError):
        return {
            key
            for key in schema_keys
            if re.search(
                rf"(?<![A-Za-z0-9_]){re.escape(key)}(?![A-Za-z0-9_])", expression
            )
        }

    dependencies = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in schema_keys
    }
    # Cura helpers such as extruderValues('setting_key') carry the dependency
    # as a string literal rather than an AST Name.
    dependencies.update(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value in schema_keys
    )
    return dependencies


def _build_overlay_recompute_plan(
    schema: Mapping[str, Mapping[str, Any]], trigger_keys: set[str]
) -> dict[str, dict[str, str]]:
    """Build the transitive expression closure affected by role settings."""

    schema_keys = set(schema)
    expressions: dict[str, str] = {}
    dependencies: dict[str, set[str]] = {}
    for key, properties in schema.items():
        expression = _overlay_expression(properties)
        if expression is None:
            continue
        expressions[key] = expression
        dependencies[key] = _expression_dependencies(expression, schema_keys)

    affected = set(trigger_keys) & schema_keys
    changed = True
    while changed:
        changed = False
        for key, setting_dependencies in dependencies.items():
            if key not in affected and setting_dependencies.intersection(affected):
                affected.add(key)
                changed = True

    return {
        key: {
            "expression": expressions[key],
            "scope": _scope_for(schema[key]),
        }
        for key in sorted(affected.intersection(expressions))
    }


def resolve_cura_overlay(
    baseline: Mapping[str, Any],
    overlay: Mapping[str, Any],
    plan: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Expand one sparse role into the concrete delta CuraEngine requires.

    Cura's container stack re-evaluates expressions after every material,
    hotend or quality overlay.  CuraEngine's resolved-settings reader does
    not, so generated roles must carry the changed dependent values as well.
    The plan is generated from the selected machine schema and contains no
    printer-specific logic.
    """

    data = dict(overlay)
    if not isinstance(plan, Mapping) or not plan:
        return data, {}

    values = {**dict(baseline), **data}
    explicit = set(data)
    expressions = {
        str(key): specification
        for key, specification in plan.items()
        if isinstance(specification, Mapping)
        and isinstance(specification.get("expression"), str)
    }
    for _ in range(min(max(len(expressions), 1), 64)):
        changed = False
        evaluator = _SafeExpressionEvaluator(values)
        for key, specification in expressions.items():
            if key in explicit:
                continue
            try:
                value = evaluator.evaluate(str(specification["expression"]))
            except (
                _UnresolvedExpression,
                _UnsafeExpression,
                ArithmeticError,
                TypeError,
                ValueError,
            ):
                continue
            if key not in values or values[key] != value:
                values[key] = value
                changed = True
        if not changed:
            break

    dependent_scopes: dict[str, str] = {}
    for key, specification in expressions.items():
        if key not in values or baseline.get(key) == values[key]:
            continue
        data[key] = values[key]
        scope = specification.get("scope")
        if isinstance(scope, str):
            dependent_scopes[key] = scope
    return data, dependent_scopes


def _resolve_schema(
    schema: Mapping[str, Mapping[str, Any]],
    overrides: Mapping[str, Any] | None = None,
    seed: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, str], set[str]]:
    """Resolve definition properties and optional instance overrides.

    Returns concrete values, scopes and the keys whose expressions could not
    be evaluated.  Unresolved keys retain a concrete ``default_value`` where
    one exists; raw expressions are never returned as data.
    """

    values: dict[str, Any] = {}
    expressions: dict[str, str] = {}
    for key, properties in schema.items():
        setting_type = properties.get("type")
        if "default_value" in properties:
            values[key] = _coerce_scalar(properties["default_value"], setting_type)
        expression = None
        if _scope_for(properties) == "global" and isinstance(
            properties.get("resolve"), str
        ):
            expression = properties["resolve"]
        elif isinstance(properties.get("value"), str):
            expression = properties["value"]
        elif "value" in properties:
            values[key] = properties["value"]
        if expression is not None:
            expressions[key] = expression

    # A seed represents already-selected lower layers in Cura's container
    # stack and therefore takes precedence over definition defaults.
    values.update(seed or {})

    explicit_keys: set[str] = set()
    for key, raw_value in (overrides or {}).items():
        explicit_keys.add(key)
        setting_type = schema.get(key, {}).get("type")
        if isinstance(raw_value, str) and raw_value.lstrip().startswith("="):
            expressions[key] = raw_value.lstrip()[1:]
        else:
            values[key] = _coerce_scalar(raw_value, setting_type)
            expressions.pop(key, None)

    unresolved = set(expressions)
    # Dependency graphs are usually shallow, but the fdmprinter definition has
    # a few longer chains.  Stop once stable rather than relying on recursion.
    for _ in range(min(max(len(expressions), 1), 64)):
        changed = False
        evaluator = _SafeExpressionEvaluator(values)
        for key, expression in expressions.items():
            try:
                value = evaluator.evaluate(expression)
            except (
                _UnresolvedExpression,
                _UnsafeExpression,
                ArithmeticError,
                TypeError,
                ValueError,
            ):
                continue
            if values.get(key) != value or key not in values:
                values[key] = value
                changed = True
            unresolved.discard(key)
        if not changed:
            break

    scopes = {key: _scope_for(schema.get(key, {})) for key in values}
    return values, scopes, unresolved & explicit_keys


def _resolve_instance_values(
    schema: Mapping[str, Mapping[str, Any]],
    overrides: Mapping[str, Any],
    seed: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, str], set[str]]:
    """Resolve only an instance container against an existing stack.

    Re-evaluating all ~700 fdmprinter expressions for every one of Cura's
    thousands of quality containers is both unnecessary and subtly wrong:
    the instance sits above the selected machine/variant stack.  This helper
    evaluates its explicit values against that stack and returns only the
    role-local keys.
    """

    values = dict(seed)
    expressions: dict[str, str] = {}
    # Instance files occasionally retain settings removed from the engine
    # definition graph.  The resolved schema is the authoritative runtime
    # contract, so unknown keys must not be forwarded to CuraEngine.  This is
    # deliberately data-driven: no legacy-setting or printer allowlist.
    explicit = set(overrides) if not schema else set(overrides) & set(schema)
    for key, raw_value in overrides.items():
        if schema and key not in schema:
            continue
        if isinstance(raw_value, str) and raw_value.lstrip().startswith("="):
            expressions[key] = raw_value.lstrip()[1:]
        else:
            setting_type = schema.get(key, {}).get("type")
            values[key] = _coerce_scalar(raw_value, setting_type)

    unresolved = set(expressions)
    for _ in range(min(max(len(expressions), 1), 32)):
        changed = False
        evaluator = _SafeExpressionEvaluator(values)
        for key, expression in expressions.items():
            try:
                value = evaluator.evaluate(expression)
            except (
                _UnresolvedExpression,
                _UnsafeExpression,
                ArithmeticError,
                TypeError,
                ValueError,
            ):
                continue
            if values.get(key) != value or key not in values:
                values[key] = value
                changed = True
            unresolved.discard(key)
        if not changed:
            break

    data = {key: values[key] for key in explicit if key in values}
    scopes = {key: _scope_for(schema.get(key, {})) for key in data}
    return data, scopes, unresolved


@dataclass
class _InstanceResource:
    path: Path
    source_kind: str
    native_id: str
    definition: str
    name: str
    metadata: dict[str, Any] = field(default_factory=dict)
    values: dict[str, Any] = field(default_factory=dict)


def _parse_instance(path: Path, source_kind: str) -> _InstanceResource:
    parser = configparser.ConfigParser(
        interpolation=None,
        strict=False,
        inline_comment_prefixes=("#",),
        empty_lines_in_values=False,
    )
    parser.optionxform = str
    with path.open(encoding="utf-8") as stream:
        parser.read_file(stream)

    general = dict(parser.items("general")) if parser.has_section("general") else {}
    metadata = (
        {key: _coerce_scalar(value) for key, value in parser.items("metadata")}
        if parser.has_section("metadata")
        else {}
    )
    values = dict(parser.items("values")) if parser.has_section("values") else {}
    definition = str(general.get("definition") or metadata.get("definition") or "")
    file_id = _resource_id(path)
    directory_name = "variants" if source_kind == "variant" else source_kind
    try:
        directory_index = path.parts.index(directory_name)
        relative_parts = list(path.parts[directory_index + 1 :])
        relative_parts[-1] = file_id
        stable_id = "/".join(relative_parts)
    except (ValueError, IndexError):
        stable_id = file_id
    return _InstanceResource(
        path=path,
        source_kind=source_kind,
        native_id=f"{source_kind}:{definition}:{stable_id}",
        definition=definition,
        name=str(general.get("name") or file_id),
        metadata=metadata,
        values=values,
    )


def _local_name(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _namespace(element: ET.Element) -> str:
    if element.tag.startswith("{"):
        return element.tag[1:].split("}", 1)[0]
    return ""


def _material_setting_key(element: ET.Element) -> str | None:
    key = element.get("key", "")
    if not key:
        return None
    mapped = MATERIAL_SETTING_MAP.get(key)
    if mapped:
        return mapped
    # Cura-namespaced settings already use engine keys.  Unmapped settings in
    # the material namespace are serialization/UI metadata (for example
    # "hardware compatible"), not CuraEngine settings.
    if _namespace(element).endswith("/cura"):
        return key
    return None


def _child(element: ET.Element | None, name: str) -> ET.Element | None:
    if element is None:
        return None
    return next((item for item in element if _local_name(item) == name), None)


def _child_text(element: ET.Element | None, name: str) -> str:
    item = _child(element, name)
    return (item.text or "").strip() if item is not None else ""


class CuraParser(BaseParser):
    """Parse the pinned Cura resource graph into SimplyPrint profile roles."""

    slicer_type = SlicerType.CURA

    def parse_file(self, path: Path) -> ParsedProfile:
        """Parse one resource without cross-file composition.

        Production ingestion should call :meth:`parse_directory`; this method
        remains useful for material overlays and focused tooling.
        """

        if path.name.endswith(".fdm_material"):
            return self._parse_fdm_material(path, {})
        if path.name.endswith(".inst.cfg"):
            kind = self._source_kind(path)
            instance = _parse_instance(path, kind)
            profile_type = (
                ProfileType.MACHINE if kind == "variant" else ProfileType.PRINT
            )
            data = {
                key: _coerce_scalar(value)
                for key, value in instance.values.items()
                if not (isinstance(value, str) and value.startswith("="))
            }
            return ParsedProfile(
                slicer=self.slicer_type,
                profile_type=profile_type,
                name=instance.name,
                vendor="Unknown",
                settings=data,
                source_path=path,
                native_id=instance.native_id,
                setting_id=instance.native_id,
                context={
                    "native_id": instance.native_id,
                    "source_kind": kind,
                    "definition": instance.definition,
                    **instance.metadata,
                },
                setting_scopes={key: "extruder.0" for key in data},
            )
        return self._parse_definition_file(path)

    def parse_directory(
        self,
        directory: Path,
        profile_type_filter: list[ProfileType] | None = None,
        resource_version: str | None = None,
    ) -> Iterator[ParsedProfile]:
        definition_paths = sorted(directory.rglob("*.def.json"))
        graph = _DefinitionGraph(definition_paths)
        base_schema = (
            graph.resolve("fdmprinter").schema if "fdmprinter" in graph else {}
        )
        single_extruder_schema = base_schema
        if "fdmextruder" in graph:
            single_extruder_schema = _merge_schemas(
                base_schema, graph.resolve("fdmextruder").schema
            )

        instances: list[_InstanceResource] = []
        for path in sorted(directory.rglob("*.inst.cfg")):
            kind = self._source_kind(path)
            try:
                instances.append(_parse_instance(path, kind))
            except (OSError, configparser.Error, ValueError) as exc:
                logger.warning("Skipping invalid Cura instance %s: %s", path, exc)

        variants = [item for item in instances if item.source_kind == "variant"]
        variants_by_definition: dict[str, list[_InstanceResource]] = {}
        for variant in variants:
            variants_by_definition.setdefault(variant.definition, []).append(variant)

        requested = set(profile_type_filter or ProfileType)
        profiles: list[ParsedProfile] = []

        material_profiles: list[ParsedProfile] = []
        for path in sorted(directory.rglob("*.fdm_material")):
            try:
                material_profiles.append(
                    self._parse_fdm_material(
                        path, single_extruder_schema, resource_version
                    )
                )
            except (OSError, ET.ParseError, ValueError) as exc:
                logger.warning("Skipping invalid Cura material %s: %s", path, exc)
        if ProfileType.FILAMENT in requested:
            profiles.extend(material_profiles)
        material_settings = {
            profile.native_id: profile.settings
            for profile in material_profiles
            if profile.native_id
        }
        materials_by_native_id = {
            profile.native_id: profile
            for profile in material_profiles
            if profile.native_id
        }
        material_trigger_keys: set[str] = set()
        for profile in material_profiles:
            material_trigger_keys.update(profile.settings)
            overrides = profile.context.get("machine_overrides")
            if not isinstance(overrides, list):
                continue
            for override in overrides:
                if not isinstance(override, Mapping):
                    continue
                settings = override.get("settings")
                if isinstance(settings, Mapping):
                    material_trigger_keys.update(str(key) for key in settings)
                hotends = override.get("hotends")
                if not isinstance(hotends, list):
                    continue
                for hotend in hotends:
                    if not isinstance(hotend, Mapping):
                        continue
                    settings = hotend.get("settings")
                    if isinstance(settings, Mapping):
                        material_trigger_keys.update(str(key) for key in settings)

        visible_definitions: list[_ResolvedDefinition] = []
        for native_id in graph.ids:
            try:
                definition = graph.resolve(native_id)
            except (KeyError, ValueError) as exc:
                logger.warning(
                    "Skipping invalid Cura definition %s: %s", native_id, exc
                )
                continue
            resource_type = definition.metadata.get("type", "machine")
            if resource_type == "machine" and definition.metadata.get("visible", True):
                visible_definitions.append(definition)

        if requested & {ProfileType.MACHINE_MODEL, ProfileType.MACHINE}:
            for definition in visible_definitions:
                machine_profiles = self._normalise_machine(
                    definition,
                    graph,
                    variants_by_definition,
                    materials_by_native_id,
                    material_trigger_keys,
                    resource_version,
                )
                profiles.extend(
                    profile
                    for profile in machine_profiles
                    if profile.profile_type in requested
                )

        if ProfileType.PRINT in requested:
            print_instances = [
                item
                for item in instances
                if item.source_kind in {"quality", "quality_changes", "intent"}
            ]
            profiles.extend(
                self._normalise_print_instances(
                    print_instances,
                    variants,
                    graph,
                    single_extruder_schema,
                    material_settings,
                    resource_version,
                )
            )

        self._disambiguate_profile_names(profiles)

        # A malformed upstream file must not make result ordering unstable.
        profiles.sort(
            key=lambda item: (
                item.profile_type.value,
                item.vendor.casefold(),
                item.name.casefold(),
                item.native_id or "",
            )
        )
        yield from profiles

    def _normalise_machine(
        self,
        definition: _ResolvedDefinition,
        graph: _DefinitionGraph,
        variants_by_definition: Mapping[str, list[_InstanceResource]],
        materials_by_native_id: Mapping[str, ParsedProfile],
        material_trigger_keys: set[str],
        resource_version: str | None,
    ) -> list[ParsedProfile]:
        schema = definition.schema
        metadata = definition.metadata
        extruder_id = self._extruder_zero_id(metadata)
        if extruder_id and extruder_id in graph:
            schema = _merge_schemas(schema, graph.resolve(extruder_id).schema)

        base_data, base_scopes, _ = _resolve_schema(schema)
        material_recompute_plan = _build_overlay_recompute_plan(
            schema, material_trigger_keys
        )
        vendor = str(metadata.get("manufacturer") or "Unknown")
        machine_variants = list(variants_by_definition.get(definition.native_id, []))
        tool_topology, extruder_trains = self._tool_topology(metadata, base_data)
        bed_assets = _bed_assets(metadata, base_data)
        runtime_context: dict[str, Any] = {
            "material_mode": "single",
            "active_tool_index": 0,
            "supported_tool_indices": [0],
        }
        preferred_material_id = metadata.get("preferred_material")
        compatible_hotends = self._preferred_material_hotends(
            definition,
            materials_by_native_id.get(str(preferred_material_id))
            if preferred_material_id
            else None,
        )

        variant_descriptors: list[dict[str, Any]] = []
        machine_profiles: list[ParsedProfile] = []
        candidates: list[_InstanceResource | None] = machine_variants or [None]
        candidate_records: list[dict[str, Any]] = []
        for position, variant in enumerate(candidates):
            raw_values = variant.values if variant else {}
            data, scopes, unresolved = _resolve_schema(schema, raw_values)
            variant_name = variant.name if variant else "Default"
            nozzle = data.get("machine_nozzle_size")
            if nozzle is None:
                nozzle = base_data.get("machine_nozzle_size", 0.4)
                data["machine_nozzle_size"] = nozzle
                scopes["machine_nozzle_size"] = "extruder.0"
            volume_type = self._variant_volume_type(variant, data)
            variant_key = (
                "HF" if volume_type == "high_flow" else ""
            ) + self._format_variant(nozzle)
            variant_native_id = (
                variant.native_id
                if variant
                else f"variant:{definition.native_id}:default"
            )
            attributes = self._variant_attributes(
                variant.metadata if variant else {},
                data,
                variant_name if variant else None,
                volume_type,
            )
            candidate_records.append(
                {
                    "position": position,
                    "variant": variant,
                    "data": data,
                    "scopes": scopes,
                    "unresolved": unresolved,
                    "name": variant_name,
                    "nozzle": nozzle,
                    "volume_type": volume_type,
                    "key": variant_key,
                    "native_id": variant_native_id,
                    "attributes": attributes,
                }
            )

        # SimplyPrint's hardware identity is nozzle diameter plus volume type.
        # Cura can publish additional hotend/material variants in the same
        # slot, so select one source-preferred role deterministically and keep
        # the discarded identities as compatibility aliases.
        preferred_variant_name = str(metadata.get("preferred_variant_name") or "")
        records_by_key: dict[str, list[dict[str, Any]]] = {}
        for record in candidate_records:
            records_by_key.setdefault(str(record["key"]), []).append(record)

        selected_records: list[dict[str, Any]] = []
        for variant_key, records in records_by_key.items():

            def selection_key(record: Mapping[str, Any]) -> tuple[Any, ...]:
                attributes = record.get("attributes")
                hotend_id = (
                    attributes.get("hotend_id")
                    if isinstance(attributes, Mapping)
                    else None
                )
                compatible_preferred_hotend = bool(
                    compatible_hotends is not None
                    and self._normalise_native_identity(hotend_id) in compatible_hotends
                )
                name = str(record.get("name") or "")
                return (
                    not bool(
                        preferred_variant_name
                        and name.casefold() == preferred_variant_name.casefold()
                    ),
                    not compatible_preferred_hotend,
                    "default" not in name.casefold(),
                    name.casefold(),
                    str(record.get("native_id") or "").casefold(),
                )

            selected = min(records, key=selection_key)
            aliases = sorted(
                {
                    str(identity)
                    for record in records
                    if record is not selected
                    for identity in (record.get("name"), record.get("native_id"))
                    if identity not in (None, "")
                }
            )
            if aliases:
                selected["aliases"] = aliases
                logger.info(
                    "Selected Cura variant %s for %s/%s; collapsed aliases: %s",
                    selected["name"],
                    definition.native_id,
                    variant_key,
                    ", ".join(aliases),
                )
            selected_records.append(selected)

        for record in sorted(selected_records, key=lambda item: item["position"]):
            variant = record["variant"]
            data = record["data"]
            scopes = record["scopes"]
            unresolved = record["unresolved"]
            variant_name = str(record["name"])
            nozzle = record["nozzle"]
            variant_key = str(record["key"])
            variant_native_id = str(record["native_id"])
            attributes = record["attributes"]
            scene = _scene_context(data)
            context = {
                "native_id": variant_native_id,
                "source_kind": "variant" if variant else "definition_default",
                **_resource_context(resource_version),
                "definition": definition.native_id,
                "definition_inheritance": list(definition.inheritance),
                "extruder_definition": extruder_id,
                "printer_model": definition.native_id,
                "printer_variant": variant_key,
                "variant_name": variant_name,
                "nozzle_diameter": nozzle,
                "attributes": attributes,
                "tool_topology": tool_topology,
                "runtime": {
                    **runtime_context,
                    "compatible_tool_indices": [0],
                },
            }
            if record.get("aliases"):
                context["variant_aliases"] = record["aliases"]
            if material_recompute_plan:
                context[CURA_MATERIAL_RECOMPUTE_PLAN] = material_recompute_plan
            if scene is not None:
                context["scene"] = scene
            if unresolved:
                context["unresolved_settings"] = sorted(unresolved)
            machine_profiles.append(
                ParsedProfile(
                    slicer=self.slicer_type,
                    profile_type=ProfileType.MACHINE,
                    name=(
                        f"{definition.name} {variant.name}"
                        if variant
                        else f"{definition.name} {variant_key} nozzle"
                    ),
                    vendor=vendor,
                    settings=data,
                    source_path=variant.path if variant else None,
                    native_id=variant_native_id,
                    setting_id=variant_native_id,
                    context=context,
                    setting_scopes=scopes,
                )
            )
            variant_descriptors.append(
                {
                    "id": variant_native_id,
                    "key": variant_key,
                    "name": variant_name,
                    "nozzle_diameter": nozzle,
                    "attributes": attributes,
                    "runtime_compatible_tool_indices": [0],
                }
            )

        if compatible_hotends is not None:
            supported_variant_ids = {
                item["id"]
                for item in variant_descriptors
                if self._normalise_native_identity(
                    item.get("attributes", {}).get("hotend_id")
                )
                in compatible_hotends
            }
            preferred_variant_name = metadata.get("preferred_variant_name")
            preferred_variant = next(
                (
                    item
                    for item in variant_descriptors
                    if preferred_variant_name
                    and item["name"].casefold()
                    == str(preferred_variant_name).casefold()
                ),
                None,
            )
            # Only activate a finite source selector when it produces a
            # coherent default stack.  Malformed third-party material data
            # must not make every hardware variant disappear.
            if supported_variant_ids and (
                preferred_variant is None
                or preferred_variant["id"] in supported_variant_ids
            ):
                runtime_context["variant_selector"] = {
                    "kind": "preferred_material_hotend_compatibility",
                    "material_native_id": str(preferred_material_id),
                }
                for item in variant_descriptors:
                    item["runtime_compatible_tool_indices"] = (
                        [0] if item["id"] in supported_variant_ids else []
                    )
                for profile in machine_profiles:
                    profile.context["runtime"] = {
                        **runtime_context,
                        "compatible_tool_indices": (
                            [0] if profile.native_id in supported_variant_ids else []
                        ),
                    }

        preferred_variant_name = metadata.get("preferred_variant_name")
        preferred_variant_key = next(
            (
                item["key"]
                for item in variant_descriptors
                if preferred_variant_name
                and item["name"].casefold() == str(preferred_variant_name).casefold()
            ),
            None,
        )

        model_context = {
            "native_id": f"definition:{definition.native_id}",
            "source_kind": "definition",
            **_resource_context(resource_version),
            "definition": definition.native_id,
            "definition_inheritance": list(definition.inheritance),
            "extruder_definition": extruder_id,
            "quality_definition": metadata.get(
                "quality_definition", definition.native_id
            ),
            "preferred_material": metadata.get("preferred_material"),
            "preferred_quality_type": metadata.get("preferred_quality_type"),
            "preferred_variant_name": preferred_variant_name,
            "preferred_variant_key": preferred_variant_key,
            "machine_extruder_trains": extruder_trains,
            "tool_topology": tool_topology,
            "runtime": runtime_context,
            "bed_assets": bed_assets or None,
            "scene": _scene_context(base_data),
            "bed_model": metadata.get("platform"),
            "bed_texture": metadata.get("platform_texture"),
            "include_materials": metadata.get("include_materials", []),
            "exclude_materials": metadata.get("exclude_materials", []),
            "variants": variant_descriptors,
        }
        model_profile = ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.MACHINE_MODEL,
            name=definition.name,
            vendor=vendor,
            settings=base_data,
            native_id=f"definition:{definition.native_id}",
            setting_id=f"definition:{definition.native_id}",
            context=model_context,
            setting_scopes=base_scopes,
        )
        return [model_profile, *machine_profiles]

    def _normalise_print_instances(
        self,
        instances: list[_InstanceResource],
        variants: list[_InstanceResource],
        graph: _DefinitionGraph,
        base_schema: Mapping[str, Mapping[str, Any]],
        material_settings: Mapping[str, Mapping[str, Any]],
        resource_version: str | None,
    ) -> list[ParsedProfile]:
        global_instances: dict[tuple[str, str, str, str, str], _InstanceResource] = {}
        leaves: list[_InstanceResource] = []
        for instance in instances:
            metadata = instance.metadata
            key = (
                instance.source_kind,
                instance.definition,
                str(metadata.get("quality_type") or ""),
                str(metadata.get("intent_category") or ""),
                str(metadata.get("variant") or ""),
            )
            if metadata.get("global_quality") is True:
                global_instances[key] = instance
            else:
                leaves.append(instance)

        used_globals: set[str] = set()
        result: list[ParsedProfile] = []
        definition_seed_cache: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        variant_seed_cache: dict[tuple[str, str], dict[str, Any]] = {}
        for leaf in leaves:
            metadata = leaf.metadata
            key = (
                leaf.source_kind,
                leaf.definition,
                str(metadata.get("quality_type") or ""),
                str(metadata.get("intent_category") or ""),
                str(metadata.get("variant") or ""),
            )
            global_instance = global_instances.get(key)
            if global_instance is None and key[-1]:
                global_instance = global_instances.get((*key[:-1], ""))
            values: dict[str, Any] = {}
            if global_instance:
                values.update(global_instance.values)
                used_globals.add(global_instance.native_id)
            values.update(leaf.values)
            result.append(
                self._normalise_print_instance(
                    leaf,
                    values,
                    variants,
                    graph,
                    base_schema,
                    global_instance,
                    definition_seed_cache,
                    variant_seed_cache,
                    material_settings,
                    resource_version,
                )
            )

        # Some definitions provide only a global quality.  Preserve it rather
        # than silently making that printer unsliceable.
        for instance in global_instances.values():
            if instance.native_id not in used_globals:
                result.append(
                    self._normalise_print_instance(
                        instance,
                        instance.values,
                        variants,
                        graph,
                        base_schema,
                        None,
                        definition_seed_cache,
                        variant_seed_cache,
                        material_settings,
                        resource_version,
                    )
                )
        return result

    def _normalise_print_instance(
        self,
        instance: _InstanceResource,
        raw_values: Mapping[str, Any],
        variants: list[_InstanceResource],
        graph: _DefinitionGraph,
        base_schema: Mapping[str, Mapping[str, Any]],
        global_instance: _InstanceResource | None,
        definition_seed_cache: dict[str, tuple[dict[str, Any], dict[str, Any]]],
        variant_seed_cache: dict[tuple[str, str], dict[str, Any]],
        material_settings: Mapping[str, Mapping[str, Any]],
        resource_version: str | None,
    ) -> ParsedProfile:
        cached_seed = definition_seed_cache.get(instance.definition)
        if cached_seed is None:
            try:
                definition = graph.resolve(instance.definition)
                schema = definition.schema
                extruder_id = self._extruder_zero_id(definition.metadata)
                if extruder_id and extruder_id in graph:
                    schema = _merge_schemas(schema, graph.resolve(extruder_id).schema)
                vendor = str(definition.metadata.get("manufacturer") or "Unknown")
            except (KeyError, ValueError):
                schema = dict(base_schema)
                vendor = "Unknown"
            seed, _, _ = _resolve_schema(schema)
            definition_seed_cache[instance.definition] = (schema, seed)
        else:
            schema, seed = cached_seed
            try:
                definition = graph.resolve(instance.definition)
                vendor = str(definition.metadata.get("manufacturer") or "Unknown")
            except (KeyError, ValueError):
                vendor = "Unknown"

        variant_name = str(instance.metadata.get("variant") or "")
        variant = next(
            (
                candidate
                for candidate in variants
                if candidate.name == variant_name
                and self._definition_is_compatible(
                    candidate.definition, instance.definition, graph
                )
            ),
            None,
        )
        if variant is not None:
            cache_key = (instance.definition, variant.native_id)
            variant_seed = variant_seed_cache.get(cache_key)
            if variant_seed is None:
                variant_values, _, _ = _resolve_instance_values(
                    schema, variant.values, seed
                )
                variant_seed = {**seed, **variant_values}
                variant_seed_cache[cache_key] = variant_seed
            seed = variant_seed

        material_id = str(instance.metadata.get("material") or "")
        if material_id in material_settings:
            seed = {**seed, **material_settings[material_id]}

        # A print role contains only settings introduced by the selected
        # quality/intent resources.  Expressions may read from the seeded
        # machine stack, but machine defaults must not overwrite the selected
        # machine when this role is merged at runtime.
        data, role_scopes, unresolved = _resolve_instance_values(
            schema, raw_values, seed
        )

        quality_type = str(instance.metadata.get("quality_type") or "")
        intent_category = str(instance.metadata.get("intent_category") or "")
        name_parts = [instance.name, f"@{instance.definition}"]
        if variant_name:
            name_parts.append(variant_name)
        if material_id:
            name_parts.append(material_id)
        if intent_category:
            name_parts.append(intent_category)
        display_name = " · ".join(name_parts)
        context: dict[str, Any] = {
            "native_id": instance.native_id,
            "source_kind": instance.source_kind,
            **_resource_context(resource_version),
            "definition": instance.definition,
            "quality_type": quality_type or None,
            "intent_category": intent_category or None,
            "variant_name": variant_name or None,
            "material_id": material_id or None,
            "global_quality_id": global_instance.native_id if global_instance else None,
            "compatibility": {
                "machine_definition_ids": [instance.definition],
                **({"variant_names": [variant_name]} if variant_name else {}),
                **({"material_native_ids": [material_id]} if material_id else {}),
            },
        }
        if unresolved:
            context["unresolved_settings"] = sorted(unresolved)
        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.PRINT,
            name=display_name,
            vendor=vendor,
            settings=data,
            source_path=instance.path,
            native_id=instance.native_id,
            setting_id=instance.native_id,
            context=context,
            setting_scopes=role_scopes,
        )

    def _parse_fdm_material(
        self,
        path: Path,
        schema: Mapping[str, Mapping[str, Any]],
        resource_version: str | None = None,
    ) -> ParsedProfile:
        root = ET.parse(path).getroot()
        metadata = _child(root, "metadata")
        name_element = _child(metadata, "name")
        brand = _child_text(name_element, "brand")
        material_type = _child_text(name_element, "material")
        color = _child_text(name_element, "color")
        label = _child_text(name_element, "label")
        guid = _child_text(metadata, "GUID")
        native_id = _resource_id(path)
        profile_name = label or f"{brand} {material_type} {color}".strip() or native_id

        raw_settings: dict[str, Any] = {
            "material_guid": guid,
            "material_type": material_type,
            "material_brand": brand,
        }
        properties = _child(root, "properties")
        diameter = _child_text(properties, "diameter")
        if diameter:
            raw_settings["material_diameter"] = diameter

        settings_element = _child(root, "settings")
        machine_overrides: list[dict[str, Any]] = []
        default_compatible = True
        if settings_element is not None:
            # Machine blocks inherit the material-wide value regardless of
            # their XML order, so resolve it before parsing any block.
            for setting in settings_element:
                if (
                    _local_name(setting) == "setting"
                    and setting.get("key") == "hardware compatible"
                ):
                    default_compatible = _parse_compatible_value(setting.text)
                    break
            for setting in settings_element:
                local_name = _local_name(setting)
                if local_name == "setting":
                    if setting.get("key") == "hardware compatible":
                        continue
                    mapped_key = _material_setting_key(setting)
                    if mapped_key:
                        raw_settings[mapped_key] = (setting.text or "").strip()
                elif local_name == "machine":
                    machine_overrides.append(
                        self._parse_material_machine(
                            setting, schema, default_compatible
                        )
                    )

        compatibility = {
            "default": default_compatible,
            "machines": [
                {
                    "identifiers": override["identifiers"],
                    "compatible": override["compatible"],
                    "hotends": {
                        str(hotend["id"]): bool(hotend["compatible"])
                        for hotend in override["hotends"]
                        if hotend.get("id")
                    },
                }
                for override in machine_overrides
                if override["identifiers"]
            ],
        }

        seed, _, _ = _resolve_schema(schema) if schema else ({}, {}, set())
        data, scopes, unresolved = _resolve_instance_values(schema, raw_settings, seed)
        for override in machine_overrides:
            override_keys = set(override["settings"])
            for hotend in override["hotends"]:
                override_keys.update(hotend["settings"])
            scopes.update(
                {key: _scope_for(schema.get(key, {})) for key in override_keys}
            )

        context = {
            "native_id": native_id,
            "source_kind": "material",
            **_resource_context(resource_version),
            "guid": guid or None,
            "brand": brand,
            "material_type": material_type,
            "color": color,
            "color_code": _child_text(metadata, "color_code") or None,
            "description": _child_text(metadata, "description") or None,
            "diameter": _coerce_scalar(diameter) if diameter else None,
            "machine_overrides": machine_overrides,
            "compatibility": compatibility,
        }
        if unresolved:
            context["unresolved_settings"] = sorted(unresolved)
        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.FILAMENT,
            name=profile_name,
            vendor=brand or "Generic",
            settings=data,
            source_path=path,
            filament_id=guid or native_id,
            filament_type=material_type,
            native_id=native_id,
            setting_id=native_id,
            context=context,
            setting_scopes=scopes,
        )

    def _parse_material_machine(
        self,
        element: ET.Element,
        schema: Mapping[str, Mapping[str, Any]],
        default_compatible: bool = True,
    ) -> dict[str, Any]:
        identifiers = [
            {
                "manufacturer": child.get("manufacturer"),
                "product": child.get("product"),
            }
            for child in element
            if _local_name(child) == "machine_identifier" and child.get("product")
        ]
        first_identifier = identifiers[0] if identifiers else {}
        result: dict[str, Any] = {
            # Retain the established singular fields for older importers while
            # preserving every source identifier in the generic list.
            "manufacturer": first_identifier.get("manufacturer"),
            "product": first_identifier.get("product"),
            "identifiers": identifiers,
            "compatible": default_compatible,
            "settings": {},
            "hotends": [],
        }
        for child in element:
            local_name = _local_name(child)
            if local_name == "setting":
                if child.get("key") == "hardware compatible":
                    result["compatible"] = _parse_compatible_value(
                        child.text, default_compatible
                    )
                    continue
                key = _material_setting_key(child)
                if key and (not schema or key in schema):
                    result["settings"][key] = _coerce_scalar(
                        (child.text or "").strip(), schema.get(key, {}).get("type")
                    )
            elif local_name == "hotend":
                hotend_settings: dict[str, Any] = {}
                hotend_compatible = result["compatible"]
                for setting in child:
                    if _local_name(setting) != "setting":
                        continue
                    if setting.get("key") == "hardware compatible":
                        hotend_compatible = _parse_compatible_value(
                            setting.text, result["compatible"]
                        )
                        continue
                    key = _material_setting_key(setting)
                    if key and (not schema or key in schema):
                        hotend_settings[key] = _coerce_scalar(
                            (setting.text or "").strip(),
                            schema.get(key, {}).get("type"),
                        )
                result["hotends"].append(
                    {
                        "id": child.get("id"),
                        "compatible": hotend_compatible,
                        "settings": hotend_settings,
                    }
                )
        return result

    def _parse_definition_file(
        self, path: Path, resource_version: str | None = None
    ) -> ParsedProfile:
        data = json.loads(path.read_text(encoding="utf-8"))
        native_id = _resource_id(path)
        metadata = data.get("metadata", {})
        schema = _definition_schema(data)
        settings, scopes, unresolved = _resolve_schema(schema)
        context: dict[str, Any] = {
            "native_id": f"definition:{native_id}",
            "source_kind": "definition",
            **_resource_context(resource_version),
            "definition": native_id,
            "inherits": data.get("inherits"),
            **metadata,
        }
        if unresolved:
            context["unresolved_settings"] = sorted(unresolved)
        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.MACHINE_MODEL,
            name=str(data.get("name") or native_id),
            vendor=str(metadata.get("manufacturer") or "Unknown"),
            settings=settings,
            source_path=path,
            native_id=f"definition:{native_id}",
            setting_id=f"definition:{native_id}",
            context=context,
            setting_scopes=scopes,
        )

    @staticmethod
    def _source_kind(path: Path) -> str:
        for part in reversed(path.parts):
            if part in {"variants", "quality", "quality_changes", "intent"}:
                return "variant" if part == "variants" else part
        metadata_hint = path.parent.name.lower()
        if "variant" in metadata_hint:
            return "variant"
        if "intent" in metadata_hint:
            return "intent"
        return "quality"

    @staticmethod
    def _extruder_zero_id(metadata: Mapping[str, Any]) -> str | None:
        trains = metadata.get("machine_extruder_trains")
        if not isinstance(trains, Mapping):
            return None
        value = trains.get("0")
        if value is None:
            value = trains.get(0)
        return str(value) if value else None

    @staticmethod
    def _tool_topology(
        metadata: Mapping[str, Any], machine_values: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        raw_trains = metadata.get("machine_extruder_trains")
        trains: dict[str, str] = {}
        indexed_trains: dict[int, str] = {}
        if isinstance(raw_trains, Mapping):
            for raw_index, raw_definition in raw_trains.items():
                if raw_definition in (None, ""):
                    continue
                try:
                    index = int(raw_index)
                except (TypeError, ValueError):
                    continue
                definition_id = str(raw_definition)
                trains[str(index)] = definition_id
                indexed_trains[index] = definition_id

        try:
            tool_count = max(int(machine_values.get("machine_extruder_count", 1)), 1)
        except (TypeError, ValueError):
            tool_count = 1
        if indexed_trains:
            tool_count = max(tool_count, max(indexed_trains) + 1)

        tools = []
        for index in range(tool_count):
            tool: dict[str, Any] = {
                "index": index,
                "setting_scope": f"extruder.{index}",
            }
            if index in indexed_trains:
                tool["native_definition_id"] = indexed_trains[index]
            tools.append(tool)
        return {"tool_count": tool_count, "tools": tools}, trains

    @staticmethod
    def _variant_volume_type(
        variant: _InstanceResource | None, values: Mapping[str, Any]
    ) -> str:
        """Classify the generic nozzle-volume capability of a Cura variant."""

        if variant is None:
            return "standard"

        explicit_keys = {
            "flow_type",
            "high_flow",
            "is_high_flow",
            "nozzle_flow_type",
            "nozzle_volume_type",
            "volume_type",
        }
        for source in (variant.metadata, variant.values):
            for raw_key, raw_value in source.items():
                key = re.sub(r"[^a-z0-9]+", "_", str(raw_key).casefold()).strip("_")
                if key not in explicit_keys:
                    continue
                if isinstance(raw_value, bool):
                    return "high_flow" if raw_value else "standard"
                value = re.sub(r"[^a-z0-9]+", "_", str(raw_value).casefold()).strip("_")
                if value in {
                    "1",
                    "true",
                    "yes",
                    "high",
                    "high_flow",
                    "highflow",
                    "hf",
                    "volcano",
                    "cht",
                }:
                    return "high_flow"
                if value in {
                    "0",
                    "false",
                    "no",
                    "standard",
                    "normal",
                    "regular",
                    "default",
                    "low_flow",
                }:
                    return "standard"

        identity = " ".join(
            str(item)
            for item in (
                variant.name,
                variant.native_id,
                variant.path.stem,
                values.get("machine_nozzle_id"),
            )
            if item not in (None, "")
        )
        return "high_flow" if _HIGH_FLOW_VARIANT_RE.search(identity) else "standard"

    @staticmethod
    def _variant_attributes(
        metadata: Mapping[str, Any],
        values: Mapping[str, Any],
        variant_name: str | None,
        volume_type: str,
    ) -> dict[str, Any]:
        attributes = {
            "hardware_type": metadata.get("hardware_type"),
            "nozzle_diameter": values.get("machine_nozzle_size"),
            "nozzle_volume_type": volume_type,
            "hotend_id": values.get("machine_nozzle_id") or variant_name,
        }
        return {key: value for key, value in attributes.items() if value is not None}

    @staticmethod
    def _normalise_native_identity(value: Any) -> str:
        """Normalise an upstream identifier for punctuation-insensitive matching."""

        return "".join(
            character for character in str(value).casefold() if character.isalnum()
        )

    @classmethod
    def _preferred_material_hotends(
        cls,
        definition: _ResolvedDefinition,
        material: ParsedProfile | None,
    ) -> set[str] | None:
        """Return the finite hotend set declared by the preferred material.

        Cura's default single-material stack is described by the inherited
        ``preferred_material`` plus that material's machine/hotend matrix.  A
        finite machine-specific matrix is authoritative for which hardware
        variants can be exposed on the currently supported primary tool.  No
        filter is inferred when the source only declares machine-wide/default
        compatibility.
        """

        if material is None:
            return None
        compatibility = material.context.get("compatibility")
        if not isinstance(compatibility, Mapping):
            return None
        constraints = compatibility.get("machines")
        if not isinstance(constraints, list):
            return None

        machine_names = {
            cls._normalise_native_identity(definition.name),
            cls._normalise_native_identity(definition.native_id),
            *(cls._normalise_native_identity(item) for item in definition.inheritance),
        }
        machine_manufacturers = {
            cls._normalise_native_identity(
                definition.metadata.get("manufacturer") or ""
            )
        }
        matching_constraints: list[Mapping[str, Any]] = []
        for constraint in constraints:
            if not isinstance(constraint, Mapping):
                continue
            identifiers = constraint.get("identifiers")
            if not isinstance(identifiers, list):
                continue
            for identifier in identifiers:
                if not isinstance(identifier, Mapping):
                    continue
                product = identifier.get("product")
                if (
                    not product
                    or cls._normalise_native_identity(product) not in machine_names
                ):
                    continue
                manufacturer = identifier.get("manufacturer")
                if (
                    manufacturer
                    and cls._normalise_native_identity(manufacturer)
                    not in machine_manufacturers
                ):
                    continue
                matching_constraints.append(constraint)
                break

        if not matching_constraints:
            return None

        compatible_hotends: set[str] = set()
        has_finite_hotend_matrix = False
        for constraint in matching_constraints:
            hotends = constraint.get("hotends")
            if not isinstance(hotends, Mapping) or not hotends:
                # A machine-wide compatible declaration means every variant
                # is valid; it cannot be narrowed into a finite selector.
                if bool(constraint.get("compatible", True)):
                    return None
                continue
            has_finite_hotend_matrix = True
            compatible_hotends.update(
                cls._normalise_native_identity(hotend_id)
                for hotend_id, is_compatible in hotends.items()
                if bool(is_compatible)
            )
        return compatible_hotends if has_finite_hotend_matrix else None

    @staticmethod
    def _format_variant(nozzle: Any) -> str:
        try:
            return f"{float(nozzle):g}"
        except (TypeError, ValueError):
            return str(nozzle)

    @staticmethod
    def _definition_is_compatible(
        candidate: str, target: str, graph: _DefinitionGraph
    ) -> bool:
        if candidate == target:
            return True
        try:
            resolved = graph.resolve(candidate)
        except (KeyError, ValueError):
            return False
        return target in resolved.inheritance

    def _glob_profiles(self, vendor_dir: Path) -> Iterator[Path]:
        yield from sorted(vendor_dir.rglob("*.fdm_material"))
        yield from sorted(vendor_dir.rglob("*.def.json"))
        yield from sorted(vendor_dir.rglob("*.inst.cfg"))

    @staticmethod
    def _disambiguate_profile_names(profiles: list[ParsedProfile]) -> None:
        groups: dict[tuple[ProfileType, str, str], list[ParsedProfile]] = {}
        for profile in profiles:
            key = (profile.profile_type, profile.vendor, profile.name)
            groups.setdefault(key, []).append(profile)
        for (_, _, display_name), matches in groups.items():
            if len(matches) < 2:
                continue
            for profile in matches:
                profile.context.setdefault("display_name", display_name)
                suffix = (profile.native_id or "profile").rsplit(":", 1)[-1]
                profile.name = f"{display_name} · {suffix}"
