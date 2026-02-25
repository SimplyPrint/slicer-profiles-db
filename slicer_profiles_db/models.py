from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class SlicerType(str, Enum):
    BAMBUSTUDIO = "bambustudio"
    ORCASLICER = "orcaslicer"
    PRUSASLICER = "prusaslicer"
    CURA = "cura"
    ELEGOOSLICER = "elegooslicer"
    SUPERSLICER = "superslicer"


class ProfileType(str, Enum):
    FILAMENT = "filament"
    MACHINE = "machine"
    MACHINE_MODEL = "machine_model"
    PRINT = "print"


def _version_key(v: str) -> tuple[int, ...]:
    """Convert a version string to a comparable tuple of ints.

    Splits on '.', '-', '_' and converts each part to int (non-numeric → 0).
    This is the single source of truth for version ordering throughout the
    slicer module.  Imported here so StoredProfile can use it without
    depending on versions.py (which imports models.py).
    """
    import re
    parts: list[int] = []
    for part in re.split(r"[.\-_]", v):
        try:
            parts.append(int(part))
        except ValueError:
            parts.append(0)
    return tuple(parts)


class ParsedProfile(BaseModel):
    """
    Raw parsed output from a slicer parser.
    Represents one profile file as-is, before versioning.
    """

    model_config = {"arbitrary_types_allowed": True}

    slicer: SlicerType
    profile_type: ProfileType
    name: str
    vendor: str
    settings: dict[str, Any]
    source_path: Path | None = None

    filament_id: str | None = None
    setting_id: str | None = None
    filament_type: str | None = None
    filament_settings_id: str | None = None


class StoredProfile(BaseModel):
    """
    The persistent intermediate format.
    Settings are versioned: { key: { version: value } }.
    """

    slicer: str
    profile_type: str
    name: str
    vendor: str
    first_seen: str
    last_seen: str
    filament_id: str | None = None
    setting_id: str | None = None
    renamed_from: str | None = None
    settings: dict[str, dict[str, Any]] = Field(default_factory=dict)

    def get_latest(self, key: str) -> Any:
        """Get the most recent value for a setting key."""
        versions = self.settings.get(key)
        if not versions:
            return None
        return list(versions.values())[-1]

    def get_at_version(self, key: str, version: str) -> Any:
        """Get the value of a setting at a specific version.

        Walks all version entries for the key and returns the value from
        the latest version that is <= the requested version (using semantic
        comparison via ``_version_key``).
        """
        versions = self.settings.get(key)
        if not versions:
            return None
        target = _version_key(version)
        result = None
        for ver, val in versions.items():
            if _version_key(ver) <= target:
                result = val
        return result

    def changed_settings(self, from_version: str, to_version: str) -> dict[str, tuple[Any, Any]]:
        """Return settings that changed between two versions: { key: (old_value, new_value) }."""
        changes: dict[str, tuple[Any, Any]] = {}
        for key in self.settings:
            old_val = self.get_at_version(key, from_version)
            new_val = self.get_at_version(key, to_version)
            if old_val != new_val:
                changes[key] = (old_val, new_val)
        return changes

    def evaluate(self, version: str) -> dict[str, Any]:
        """Snapshot all settings at a specific version.

        Returns a flat ``{key: value}`` dict representing this profile's
        state at the given version.  Keys whose first recorded version is
        after the requested version are omitted.
        """
        target = _version_key(version)
        snapshot: dict[str, Any] = {}
        for key, versions in self.settings.items():
            value = None
            found = False
            for ver, val in versions.items():
                if _version_key(ver) <= target:
                    value = val
                    found = True
            if found:
                snapshot[key] = value
        return snapshot


class IngestionReport(BaseModel):
    """Result of ingesting a new slicer version into the store."""

    slicer: SlicerType
    version: str
    profiles_processed: int
    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    changed: dict[str, list[str]] = Field(default_factory=dict)
    unchanged: int = 0


class SourceConfig(BaseModel):
    """Configuration for a slicer's profile source."""

    slicer: SlicerType
    github_repo: str  # "bambulab/BambuStudio"
    profile_path_in_repo: str = ""  # "resources/profiles"
    branch: str | None = None  # for repos using branch HEAD (PrusaSlicer)
    tag_pattern: str | None = None  # regex to filter tags
    ini_bundle: bool = False  # True for PrusaSlicer
    filament_library_name: str | None = None  # "OrcaFilamentLibrary"
    nightly_branch: str = "main"  # branch to use for nightly builds
    min_version: str | None = None  # minimum version to ingest (normalized)
    profile_type_dirs: dict[ProfileType, str] = Field(default_factory=dict)
    additional_repos: list[str] = Field(default_factory=list)


class VersionInfo(BaseModel):
    """A normalized slicer version."""

    raw: str  # "v02.05.00.66"
    normalized: str  # "02.05.00.66"
    slicer: SlicerType


class DownloadResult(BaseModel):
    """Result of downloading and extracting slicer profiles."""

    model_config = {"arbitrary_types_allowed": True}

    slicer: SlicerType
    version: VersionInfo
    extracted_dir: Path
    profile_types_found: list[ProfileType] = Field(default_factory=list)
