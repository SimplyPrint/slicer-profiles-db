"""Per-release settings of official profiles, in the shape web's CatalogVersions reads.

Each profile carries its values for every ingested engine release as changes from its
exported settings. Web applies them when a slicer asks for a specific release.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from .catalog import EngineTarget
from .models import SlicerType, StoredProfile, _version_key
from .store import ProfileStore


def _nightly(release: str) -> bool:
    return release.startswith("nightly")


def _same(left: Any, right: Any) -> bool:
    """PHP's ``===`` on decoded JSON: types, values and key order all match."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return list(left) == list(right) and all(
            _same(value, right[key]) for key, value in left.items()
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _same(item, other) for item, other in zip(left, right, strict=True)
        )
    return left == right


def _changes(snapshot: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict:
    changes = {
        key: value
        for key, value in snapshot.items()
        if key not in baseline or not _same(baseline[key], value)
    }
    for key in baseline:
        if key not in snapshot:
            changes[key] = None
    return changes


def _evaluate(history: Mapping[str, Mapping[str, Any]], release: str) -> dict:
    target = _version_key(release)
    snapshot = {}
    for key, values in history.items():
        found = False
        value = None
        for at, candidate in values.items():
            if not _nightly(at) and _version_key(at) <= target:
                found = True
                value = candidate
        if found and value is not None:
            snapshot[key] = value
    return snapshot


def release_index(
    history: Mapping[str, Mapping[str, Any]],
    baseline: Mapping[str, Any],
    releases: list[str],
) -> dict[str, Any] | None:
    """The changes from ``baseline`` per release; None when no release exports it."""
    ordered = sorted(
        (release for release in releases if not _nightly(release)), key=_version_key
    )
    snapshots = {}
    for release in ordered:
        snapshot = _evaluate(history, release)
        if snapshot:
            snapshots[release] = snapshot
    base = next(
        (
            release
            for release in reversed(snapshots)
            if not _changes(snapshots[release], baseline)
        ),
        None,
    )
    if base is None:
        return None
    return {
        "base": base,
        "versions": {
            release: _changes(snapshot, baseline)
            for release, snapshot in snapshots.items()
        },
    }


def twin(
    index: Mapping[str, Any], source: Mapping[str, Any], copy: Mapping[str, Any]
) -> dict[str, Any]:
    """A variant the store derived from another: its changes on the settings both share."""
    return {
        "base": index["base"],
        "versions": {
            release: {
                key: value
                for key, value in changes.items()
                if _same(source.get(key), copy.get(key))
            }
            for release, changes in index["versions"].items()
        },
    }


def _source_id(profile: StoredProfile) -> str:
    return (
        profile.storage_key
        or profile.native_id
        or f"{profile.vendor}/{profile.profile_type}/{profile.name}"
    )


class _History:
    def __init__(self, store: ProfileStore, slicer: SlicerType):
        self.releases = store.get_versions(slicer)
        self.profiles: dict[str, list[StoredProfile]] = {}
        for profile in sorted(
            store.list_profiles(slicer), key=lambda item: (item.vendor, item.name)
        ):
            self.profiles.setdefault(_source_id(profile), []).append(profile)

    def index(self, owner: Mapping[str, Any]) -> dict[str, Any] | None:
        data = owner.get("data")
        context = owner.get("context")
        source_id = context.get("source_id") if isinstance(context, Mapping) else None
        if not isinstance(data, Mapping) or not isinstance(source_id, str):
            return None
        for profile in self.profiles.get(source_id, []):
            index = release_index(profile.settings, data, self.releases)
            if profile.renamed_from:
                index = {**(index or {}), "renamed_from": profile.renamed_from}
            if index is not None:
                return index
        return None


def apply_release_history(
    records: Mapping[str, MutableMapping[str, Any]],
    store: ProfileStore,
    targets: Mapping[SlicerType, EngineTarget],
) -> None:
    """Attach ``version_settings`` to the profiles of engines that keep release history."""
    histories = {
        slicer: _History(store, slicer)
        for slicer, target in targets.items()
        if target.release_history
    }
    for record in records.values():
        try:
            history = histories.get(SlicerType(str(record.get("engine"))))
        except ValueError:
            continue
        owner = record.get("profile")
        if history is None or record.get("catalog_lane") is not None:
            continue
        if not isinstance(owner, MutableMapping):
            continue
        if record.get("kind") != "machine":
            index = history.index(owner)
            if index is not None:
                owner["version_settings"] = index
            continue
        variants = owner.get("variants")
        if not isinstance(variants, Mapping):
            continue
        indexes = {key: history.index(variant) for key, variant in variants.items()}
        for key, variant in variants.items():
            index = indexes[key]
            # The store has no history for a High Flow variant it made from its
            # Standard one.
            standard = key[2:] if key.startswith("HF") else ""
            if index is None and "base" in (indexes.get(standard) or {}):
                index = twin(
                    indexes[standard],
                    variants[standard].get("data") or {},
                    variant.get("data") or {},
                )
            if index is not None and isinstance(variant, MutableMapping):
                variant["version_settings"] = index
