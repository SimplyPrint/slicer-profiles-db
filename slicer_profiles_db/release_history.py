"""Per-release settings of official profiles, in the shape web's CatalogVersions reads.

Each profile carries its values for every stable release the store has ingested, as
changes from its exported settings. Web applies them when a slicer asks for a release.

Each release must be exact: the store evaluated at the release must give the very
settings a complete ingest of that release parsed (its release manifest). A release
the store cannot give that way fails the build.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, MutableMapping, Sequence
from typing import Any

from .catalog import EngineTarget
from .models import SlicerType, StoredProfile, _version_key
from .store import ProfileStore


class ReleaseHistoryError(ValueError):
    """An ingested release or an exported profile the store cannot describe exactly."""


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
    """Each setting's value at its newest stable entry up to ``release``."""
    target = _version_key(release)
    snapshot = {}
    for key, values in history.items():
        newest = None
        value = None
        for at, candidate in values.items():
            if _nightly(at):
                continue
            at_key = _version_key(at)
            if at_key <= target and (newest is None or at_key >= newest):
                newest = at_key
                value = candidate
        if newest is not None and value is not None:
            snapshot[key] = value
    return snapshot


def _ordered(releases: Sequence[str]) -> list[str]:
    return sorted(
        (release for release in releases if not _nightly(release)), key=_version_key
    )


def _index(
    snapshots: Mapping[str, Mapping[str, Any]], baseline: Mapping[str, Any]
) -> dict[str, Any] | None:
    base = next(
        (
            release
            for release in reversed(snapshots)
            if snapshots[release] and not _changes(snapshots[release], baseline)
        ),
        None,
    )
    if base is None:
        return None
    return {
        "base": base,
        "versions": {
            release: _changes(snapshot, baseline) if snapshot else None
            for release, snapshot in snapshots.items()
        },
    }


def release_index(
    history: Mapping[str, Mapping[str, Any]],
    baseline: Mapping[str, Any],
    releases: Sequence[str],
) -> dict[str, Any] | None:
    """The changes from ``baseline`` per release; None when no release exports it.

    A release without the profile (before it was added, or after the store
    tombstoned it as removed) maps to ``None``, so a slicer asking for that
    release gets no profile.
    """
    snapshots = {release: _evaluate(history, release) for release in _ordered(releases)}
    return _index(snapshots, baseline)


def twin(
    index: Mapping[str, Any], source: Mapping[str, Any], copy: Mapping[str, Any]
) -> dict[str, Any]:
    """A variant the store derived from another: its changes on the settings both share."""
    return {
        "base": index["base"],
        "versions": {
            release: None
            if changes is None
            else {
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


def verify_release(
    store: ProfileStore,
    slicer: SlicerType,
    release: str,
    snapshots: Sequence[Mapping[str, Any]],
) -> None:
    """Fail unless ``snapshots`` (every stored profile at ``release``) are exactly
    the profiles a complete ingest of ``release`` parsed.

    Profiles are compared by content, not by name: a profile a later release
    renamed or moved carries the history of the one it replaced.
    """
    manifest = store.release_manifest(slicer, release)
    if manifest is None:
        raise ReleaseHistoryError(
            f"The {slicer.value} store has {release} but no complete ingest of it; "
            f"re-ingest with `slicer-profiles-db ingest-releases {slicer.value}`"
        )
    expected = Counter(manifest.values())
    actual = Counter(
        ProfileStore.settings_digest(dict(snapshot))
        for snapshot in snapshots
        if snapshot
    )
    if expected != actual:
        missing = expected - actual
        wrong = sorted(key for key, digest in manifest.items() if digest in missing)
        raise ReleaseHistoryError(
            f"The {slicer.value} store does not reproduce {release}: "
            f"{sum(missing.values())} of its profiles differ or are missing "
            f"({', '.join(wrong[:5]) or 'none'}), "
            f"{sum((actual - expected).values())} are extra; rebuild with "
            f"`slicer-profiles-db ingest-releases {slicer.value} --rebuild`"
        )


class _History:
    def __init__(self, store: ProfileStore, slicer: SlicerType, target: EngineTarget):
        self.releases = store.ingested_releases(slicer)
        if target.version not in self.releases:
            raise ReleaseHistoryError(
                f"{slicer.value} {target.version}, the version engines.lock.json "
                f"exports, is not an ingested release; ingest it with "
                f"`slicer-profiles-db ingest-releases {slicer.value}`"
            )
        self.profiles: dict[str, list[tuple[StoredProfile, dict[str, dict]]]] = {}
        every_snapshot: dict[str, list[dict]] = {r: [] for r in self.releases}
        for profile in sorted(
            store.list_profiles(slicer), key=lambda item: (item.vendor, item.name)
        ):
            snapshots = {
                release: _evaluate(profile.settings, release)
                for release in self.releases
            }
            for release, snapshot in snapshots.items():
                every_snapshot[release].append(snapshot)
            self.profiles.setdefault(_source_id(profile), []).append(
                (profile, snapshots)
            )
        for release, snapshots in every_snapshot.items():
            verify_release(store, slicer, release, snapshots)

    def index(self, owner: Mapping[str, Any]) -> dict[str, Any] | None:
        data = owner.get("data")
        context = owner.get("context")
        source_id = context.get("source_id") if isinstance(context, Mapping) else None
        if not isinstance(data, Mapping) or not isinstance(source_id, str):
            return None
        for profile, snapshots in self.profiles.get(source_id, []):
            index = _index(snapshots, data)
            if index is not None:
                if profile.renamed_from:
                    index["renamed_from"] = profile.renamed_from
                return index
        return None


def _require(index: dict[str, Any] | None, record: Mapping[str, Any]) -> dict:
    if index is None:
        raise ReleaseHistoryError(
            f"{record.get('id')} is exported with settings no ingested release has, "
            "so it cannot be given per release"
        )
    return index


def apply_release_history(
    records: Mapping[str, MutableMapping[str, Any]],
    store: ProfileStore,
    targets: Mapping[SlicerType, EngineTarget],
) -> None:
    """Attach ``version_settings`` to the profiles of engines that keep release history.

    Every such profile gets one entry per ingested release, so web never has to
    guess a release; one the store cannot give exactly fails the build.
    """
    histories = {
        slicer: _History(store, slicer, target)
        for slicer, target in sorted(targets.items(), key=lambda item: item[0].value)
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
            owner["version_settings"] = _require(history.index(owner), record)
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
            if index is None and indexes.get(standard) is not None:
                index = twin(
                    indexes[standard],
                    variants[standard].get("data") or {},
                    variant.get("data") or {},
                )
            if isinstance(variant, MutableMapping):
                variant["version_settings"] = _require(index, record)
