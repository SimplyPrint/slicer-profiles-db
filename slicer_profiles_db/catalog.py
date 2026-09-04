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
class ProfileTarget:
    version: str
    profile_abi: str


@dataclass(frozen=True)
class EngineTarget:
    version: str
    gcode_abi: str = "text-gcode/v1"
    lanes: dict[str, LaneTarget] | None = None
    profile_override_settings: tuple[str, ...] = ()
    profile_targets: tuple[ProfileTarget, ...] = ()


def load_engine_targets(path: Path) -> dict[SlicerType, EngineTarget]:
    """Load and validate the committed engine/profile compatibility inputs."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid engine lock {path}: {error}") from error

    if document.get("schema_version") != 4 or not isinstance(
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
        target_values = value.get("profile_targets") or []
        if not isinstance(target_values, list):
            raise TypeError(f"Engine {name!r} has invalid profile targets")
        profile_targets = tuple(
            ProfileTarget(
                version=normalize_version(target["version"]),
                profile_abi=str(target["profile_abi"]),
            )
            for target in target_values
            if isinstance(target, dict)
            and isinstance(target.get("version"), str)
            and isinstance(target.get("profile_abi"), str)
        )
        if len(profile_targets) != len(target_values):
            raise TypeError(f"Engine {name!r} has an invalid profile target")
        if len({target.profile_abi for target in profile_targets}) != len(
            profile_targets
        ):
            raise ValueError(f"Engine {name!r} has duplicate profile target ABIs")
        settings_value = value.get("profile_override_settings") or []
        if (
            not isinstance(settings_value, list)
            or not all(
                isinstance(setting, str) and setting for setting in settings_value
            )
            or len(set(settings_value)) != len(settings_value)
        ):
            raise TypeError(f"Engine {name!r} has invalid profile override settings")
        if profile_targets and not settings_value:
            raise TypeError(f"Engine {name!r} profile targets need override settings")
        targets[slicer] = EngineTarget(
            version=normalize_version(value["version"]),
            gcode_abi=str(value.get("gcode_abi") or "text-gcode/v1"),
            lanes=lanes,
            profile_override_settings=tuple(settings_value),
            profile_targets=profile_targets,
        )

    missing = sorted(slicer.value for slicer in SlicerType if slicer not in targets)
    if missing:
        raise ValueError(f"Engine lock is missing: {', '.join(missing)}")
    return targets
