"""Explicit engine inputs for reproducible profile catalogues."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .models import SlicerType
from .versions import normalize_version


@dataclass(frozen=True)
class LaneTarget:
    version: str
    format: str
    gcode_abi: str


@dataclass(frozen=True)
class SettingSchemaSource:
    url: str
    sha256: str


@dataclass(frozen=True)
class CompatibilityTarget:
    version: str
    gcode_abi: str
    setting_schema: SettingSchemaSource


@dataclass(frozen=True)
class EngineTarget:
    version: str
    gcode_abi: str = "text-gcode/v1"
    lanes: dict[str, LaneTarget] | None = None
    setting_schema: SettingSchemaSource | None = None
    compatibility: tuple[CompatibilityTarget, ...] = ()


def _setting_schema(value: object, description: str) -> SettingSchemaSource:
    if not isinstance(value, dict):
        raise TypeError(f"{description} has no setting schema")
    url = value.get("url")
    digest = value.get("sha256")
    if (
        not isinstance(url, str)
        or not url.startswith("https://")
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise TypeError(f"{description} has an invalid setting schema")
    return SettingSchemaSource(url=url, sha256=digest)


def load_engine_targets(path: Path) -> dict[SlicerType, EngineTarget]:
    """Load and validate the committed engine/profile compatibility inputs."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid engine lock {path}: {error}") from error

    if document.get("schema_version") != 2 or not isinstance(
        document.get("engines"), dict
    ):
        raise ValueError(f"Invalid engine lock schema in {path}")

    targets: dict[SlicerType, EngineTarget] = {}
    for name, value in document["engines"].items():
        try:
            slicer = SlicerType(name)
        except ValueError as error:
            raise ValueError(f"Unknown engine {name!r} in {path}") from error
        if not isinstance(value, dict) or not isinstance(value.get("version"), str):
            raise TypeError(f"Engine {name!r} has no version in {path}")
        lane_values = value.get("lanes") or {}
        if not isinstance(lane_values, dict):
            raise TypeError(f"Engine {name!r} has invalid lanes")
        lanes: dict[str, LaneTarget] = {}
        for lane, lane_value in lane_values.items():
            if (
                not isinstance(lane, str)
                or not isinstance(lane_value, dict)
                or not isinstance(lane_value.get("version"), str)
                or not isinstance(lane_value.get("format"), str)
            ):
                raise TypeError(f"Engine {name!r} has an invalid lane")
            lanes[lane] = LaneTarget(
                version=normalize_version(lane_value["version"]),
                format=lane_value["format"],
                gcode_abi=str(
                    lane_value.get("gcode_abi")
                    or value.get("gcode_abi")
                    or "text-gcode/v1"
                ),
            )
        compatibility_values = value.get("compatibility") or []
        if not isinstance(compatibility_values, list):
            raise TypeError(f"Engine {name!r} has invalid compatibility targets")
        compatibility = tuple(
            CompatibilityTarget(
                version=normalize_version(target["version"]),
                gcode_abi=str(target["gcode_abi"]),
                setting_schema=_setting_schema(
                    target.get("setting_schema"),
                    f"Engine {name!r} compatibility target",
                ),
            )
            for target in compatibility_values
            if isinstance(target, dict)
            and isinstance(target.get("version"), str)
            and isinstance(target.get("gcode_abi"), str)
        )
        if len(compatibility) != len(compatibility_values):
            raise TypeError(f"Engine {name!r} has an invalid compatibility target")
        if len({target.gcode_abi for target in compatibility}) != len(compatibility):
            raise ValueError(f"Engine {name!r} has duplicate compatibility ABIs")
        setting_schema = (
            _setting_schema(value["setting_schema"], f"Engine {name!r}")
            if "setting_schema" in value
            else None
        )
        if compatibility and setting_schema is None:
            raise TypeError(f"Engine {name!r} compatibility needs a setting schema")
        targets[slicer] = EngineTarget(
            version=normalize_version(value["version"]),
            gcode_abi=str(value.get("gcode_abi") or "text-gcode/v1"),
            lanes=lanes,
            setting_schema=setting_schema,
            compatibility=compatibility,
        )

    missing = sorted(slicer.value for slicer in SlicerType if slicer not in targets)
    if missing:
        raise ValueError(f"Engine lock is missing: {', '.join(missing)}")
    return targets
