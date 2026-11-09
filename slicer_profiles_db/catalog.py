"""Explicit engine inputs for reproducible profile catalogues."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .models import SlicerType, _version_key
from .versions import normalize_version


@dataclass(frozen=True)
class LaneTarget:
    version: str
    format: str
    gcode_abi: str


@dataclass(frozen=True)
class EngineTarget:
    version: str
    gcode_abi: str = "text-gcode/v1"
    lanes: dict[str, LaneTarget] | None = None
    release_history: bool = False
    # The engine releases the cloud slicer can slice with, oldest first: the
    # releases ``version_settings`` describes, each one exactly.
    releases: tuple[str, ...] = ()

    @property
    def nightly_release(self) -> str | None:
        """The release whose settings a nightly build slices with: the newest listed.

        A nightly is built from upstream's default branch after its newest
        release, whose profiles the store does not ingest.
        """
        return self.releases[-1] if self.releases else None


def _releases(name: str, value: dict, version: str) -> tuple[str, ...]:
    raw = value.get("releases", [])
    if not isinstance(raw, list) or not all(
        isinstance(item, str) and item.strip() for item in raw
    ):
        raise TypeError(f"Engine {name!r} has invalid releases")
    releases = [normalize_version(item) for item in raw]
    if len(set(releases)) != len(releases):
        raise ValueError(f"Engine {name!r} lists a release twice")
    if any(release.startswith("nightly") for release in releases):
        raise ValueError(f"Engine {name!r} lists a nightly as a release")
    if releases != sorted(releases, key=_version_key):
        raise ValueError(f"Engine {name!r} releases are not oldest first")
    if value.get("release_history") and not releases:
        raise ValueError(f"Engine {name!r} keeps release history but lists no releases")
    if releases and version not in releases:
        raise ValueError(
            f"Engine {name!r} version {version} is not one of its releases"
        )
    return tuple(releases)


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
        release_history = value.get("release_history", False)
        if not isinstance(release_history, bool):
            raise TypeError(f"Engine {name!r} has invalid release history flag")
        version = normalize_version(value["version"])
        targets[slicer] = EngineTarget(
            version=version,
            gcode_abi=str(value.get("gcode_abi") or "text-gcode/v1"),
            lanes=lanes,
            release_history=release_history,
            releases=_releases(name, value, version),
        )

    missing = sorted(slicer.value for slicer in SlicerType if slicer not in targets)
    if missing:
        raise ValueError(f"Engine lock is missing: {', '.join(missing)}")
    return targets


def _nightly_build(version: str, entry: object) -> bool:
    if version.startswith("nightly"):
        return True
    return isinstance(entry, dict) and entry.get("channel") == "nightly"


def release_lock_problems(
    targets: dict[SlicerType, EngineTarget], slicers_lock: dict
) -> list[str]:
    """Where the listed releases differ from what the cloud slicer can slice with.

    ``slicers_lock`` is SimplyPrint/site-cloud-slicer's ``slicers.lock.json``.
    Every engine that keeps release history must list exactly its stable
    (non-nightly) versions there.
    """
    engines = slicers_lock.get("slicers") if isinstance(slicers_lock, dict) else None
    if not isinstance(engines, dict):
        return ["slicers.lock.json has no slicers"]
    by_name = {str(name).lower(): value for name, value in engines.items()}
    problems = []
    for slicer, target in sorted(targets.items(), key=lambda item: item[0].value):
        if not target.release_history:
            continue
        engine = by_name.get(slicer.value)
        versions = engine.get("versions") if isinstance(engine, dict) else None
        if not isinstance(versions, dict):
            problems.append(f"{slicer.value}: not in slicers.lock.json")
            continue
        available = {
            normalize_version(version)
            for version, entry in versions.items()
            if not _nightly_build(version, entry)
        }
        listed = set(target.releases)
        if listed != available:
            problems.append(
                f"{slicer.value}: engines.lock.json lists {sorted(listed)}, "
                f"the cloud slicer has {sorted(available)}"
            )
    return problems
