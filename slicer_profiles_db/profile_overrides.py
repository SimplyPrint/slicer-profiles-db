"""Sparse profile values required by older engine profile ABIs."""

from __future__ import annotations

import json
from collections.abc import Mapping, MutableMapping
from typing import Any

from .catalog import EngineTarget
from .models import SlicerType, StoredProfile, _version_key
from .store import ProfileStore

_MISSING = object()


def _source_id(profile: StoredProfile) -> str:
    return (
        profile.storage_key
        or profile.native_id
        or f"{profile.vendor}/{profile.profile_type}/{profile.name}"
    )


def _profile_for_owner(
    candidates: list[StoredProfile], current: Mapping[str, Any], version: str
) -> StoredProfile:
    if len(candidates) == 1:
        return candidates[0]
    exact = [
        profile
        for profile in candidates
        if profile.evaluate_at_or_before(version) == current
    ]
    if len(exact) != 1:
        raise ValueError("Profile override source identity is ambiguous")
    return exact[0]


def build_profile_overrides(
    profile: StoredProfile,
    current: Mapping[str, Any],
    target: EngineTarget,
) -> list[dict[str, Any]]:
    """Group changed values by target set; missing and unchanged values are derived."""
    by_targets: dict[tuple[str, ...], dict[str, Any]] = {}
    for setting in target.profile_override_settings:
        by_value: dict[str, dict[str, Any]] = {}
        current_value = current.get(setting, _MISSING)
        for compatibility in target.profile_targets:
            if _version_key(profile.first_seen) > _version_key(compatibility.version):
                continue
            previous = profile.evaluate_at_or_before(compatibility.version)
            previous_value = previous.get(setting, _MISSING)
            if previous_value is _MISSING or previous_value == current_value:
                continue
            identity = json.dumps(
                previous_value,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            group = by_value.setdefault(
                identity, {"targets": [], "value": previous_value}
            )
            group["targets"].append(compatibility.profile_abi)
        for group in by_value.values():
            targets = tuple(group["targets"])
            by_targets.setdefault(targets, {})[setting] = group["value"]
    return [
        {"targets": list(targets), "settings": settings}
        for targets, settings in sorted(by_targets.items())
    ]


def _overrides_for_owner(
    record: Mapping[str, Any],
    owner: MutableMapping[str, Any],
    slicer: SlicerType,
    target: EngineTarget,
    profiles: Mapping[tuple[SlicerType, str], list[StoredProfile]],
) -> list[dict[str, Any]]:
    current = owner.get("data")
    context = owner.get("context")
    source_id = context.get("source_id") if isinstance(context, Mapping) else None
    if not isinstance(source_id, str) or not isinstance(current, Mapping):
        raise TypeError(f"Profile record {record.get('id')} has no source identity")
    candidates = profiles.get((slicer, source_id), [])
    if not candidates:
        raise ValueError(f"Profile record {record.get('id')} has no stored source")
    profile = _profile_for_owner(candidates, current, target.version)
    return build_profile_overrides(profile, current, target)


def apply_profile_overrides(
    records: Mapping[str, MutableMapping[str, Any]],
    store: ProfileStore,
    targets: Mapping[SlicerType, EngineTarget],
) -> None:
    """Attach sparse backwards overrides to their profile owner."""
    applicable = {
        slicer: target for slicer, target in targets.items() if target.profile_targets
    }
    profiles: dict[tuple[SlicerType, str], list[StoredProfile]] = {}
    for slicer in applicable:
        for profile in store.list_profiles(slicer):
            profiles.setdefault((slicer, _source_id(profile)), []).append(profile)

    for record in records.values():
        try:
            slicer = SlicerType(str(record.get("engine")))
        except ValueError:
            continue
        target = applicable.get(slicer)
        owner = record.get("profile")
        if target is None or not isinstance(owner, MutableMapping):
            continue
        if record.get("kind") != "machine":
            overrides = _overrides_for_owner(record, owner, slicer, target, profiles)
            if overrides:
                owner["profile_overrides"] = overrides
            continue

        variants = owner.get("variants")
        if not isinstance(variants, Mapping):
            continue
        groups: dict[str, dict[str, Any]] = {}
        for variant, variant_owner in variants.items():
            if not isinstance(variant_owner, MutableMapping):
                continue
            for override in _overrides_for_owner(
                record, variant_owner, slicer, target, profiles
            ):
                identity = json.dumps(
                    override,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                group = groups.setdefault(
                    identity,
                    {**override, "variants": []},
                )
                group["variants"].append(str(variant))
        if groups:
            owner["profile_overrides"] = sorted(
                groups.values(),
                key=lambda group: (tuple(group["targets"]), tuple(group["variants"])),
            )
