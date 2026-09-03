"""Explicit engine inputs for reproducible profile catalogues."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .models import SlicerType
from .versions import normalize_version


@dataclass(frozen=True)
class EngineTarget:
    stable: str
    prerelease: str | None = None
    catalog_lane: str | None = None
    gcode_abi: str = "text-gcode/v1"


def load_engine_targets(path: Path) -> dict[SlicerType, EngineTarget]:
    """Load and validate the committed engine/profile compatibility inputs."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid engine lock {path}: {error}") from error

    if document.get("schema_version") != 1 or not isinstance(
        document.get("engines"), dict
    ):
        raise ValueError(f"Invalid engine lock schema in {path}")

    targets: dict[SlicerType, EngineTarget] = {}
    for name, value in document["engines"].items():
        try:
            slicer = SlicerType(name)
        except ValueError as error:
            raise ValueError(f"Unknown engine {name!r} in {path}") from error
        if not isinstance(value, dict) or not isinstance(value.get("stable"), str):
            raise TypeError(f"Engine {name!r} has no stable version in {path}")
        prerelease = value.get("prerelease")
        lane = value.get("catalog_lane")
        if prerelease is not None and not isinstance(prerelease, str):
            raise ValueError(f"Engine {name!r} has an invalid prerelease version")
        if lane is not None and not isinstance(lane, str):
            raise ValueError(f"Engine {name!r} has an invalid catalog lane")
        targets[slicer] = EngineTarget(
            stable=normalize_version(value["stable"]),
            prerelease=normalize_version(prerelease) if prerelease else None,
            catalog_lane=lane,
            gcode_abi=str(value.get("gcode_abi") or "text-gcode/v1"),
        )

    missing = sorted(slicer.value for slicer in SlicerType if slicer not in targets)
    if missing:
        raise ValueError(f"Engine lock is missing: {', '.join(missing)}")
    return targets
