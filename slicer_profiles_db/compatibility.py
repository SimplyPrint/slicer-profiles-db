"""Sparse backwards projection for engine-coupled profile settings."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, MutableMapping
from functools import cache
from typing import Any

import requests

from .catalog import EngineTarget, SettingSchemaSource
from .models import ProfileType, SlicerType, StoredProfile, _version_key
from .store import ProfileStore


@cache
def _load_schema(source: SettingSchemaSource) -> dict[str, dict[str, Any]]:
    response = requests.get(source.url, timeout=30)
    response.raise_for_status()
    content = response.content
    if hashlib.sha256(content).hexdigest() != source.sha256:
        raise ValueError(f"Setting schema hash mismatch: {source.url}")
    value = json.loads(content)
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(specification, dict)
        for key, specification in value.items()
    ):
        raise TypeError(f"Invalid setting schema: {source.url}")
    return value


def _source_id(profile: StoredProfile) -> str:
    return (
        profile.storage_key
        or profile.native_id
        or f"{profile.vendor}/{profile.profile_type}/{profile.name}"
    )


def _enum_accepts(specification: Mapping[str, Any], value: Any) -> bool:
    allowed = specification.get("enum_values")
    if not isinstance(allowed, list):
        return True
    values = value if isinstance(value, list) else [value]
    return all(item in allowed for item in values)


def backwards_delta(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
    current_schema: Mapping[str, Mapping[str, Any]],
    target_schema: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Keep current values except where the target engine cannot read them."""
    unset = sorted(set(current) & (set(current_schema) - set(target_schema)))
    replacements: dict[str, Any] = {}

    for key in sorted(set(target_schema) - set(current_schema)):
        if key in previous:
            replacements[key] = previous[key]

    for key in sorted(set(current) & set(target_schema)):
        current_specification = current_schema.get(key, {})
        target_specification = target_schema[key]
        historical = previous.get(key)
        changed_value = key in previous and historical != current[key]
        changed_schema_type = (
            current_specification.get("type") != target_specification.get("type")
            and changed_value
        )
        invalid_enum = (
            changed_value
            and not _enum_accepts(target_specification, current[key])
            and _enum_accepts(target_specification, historical)
        )
        changed_gcode = "gcode" in key.casefold() and changed_value
        changed_runtime_type = key in previous and type(historical) is not type(
            current[key]
        )
        if changed_schema_type or invalid_enum or changed_gcode or changed_runtime_type:
            replacements[key] = historical

    unset = sorted(set(unset) - set(replacements))
    if not replacements and not unset:
        return None
    result: dict[str, Any] = {}
    if replacements:
        result["set"] = replacements
    if unset:
        result["unset"] = unset
    return result


def _profile_for_record(
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
        raise ValueError("Profile compatibility source identity is ambiguous")
    return exact[0]


def apply_profile_compatibility(
    records: Mapping[str, MutableMapping[str, Any]],
    store: ProfileStore,
    targets: Mapping[SlicerType, EngineTarget],
) -> None:
    """Attach ABI-keyed deltas to exported filament and process records."""
    applicable = {
        slicer: target for slicer, target in targets.items() if target.compatibility
    }
    if not applicable:
        return

    profiles: dict[tuple[SlicerType, str], list[StoredProfile]] = {}
    for slicer in applicable:
        for profile in store.list_profiles(slicer):
            if profile.profile_type not in {
                ProfileType.FILAMENT.value,
                ProfileType.PRINT.value,
            }:
                continue
            profiles.setdefault((slicer, _source_id(profile)), []).append(profile)

    schemas = {
        slicer: (
            _load_schema(target.setting_schema),
            {
                compatibility.gcode_abi: _load_schema(compatibility.setting_schema)
                for compatibility in target.compatibility
            },
        )
        for slicer, target in applicable.items()
        if target.setting_schema is not None
    }

    for record in records.values():
        if record.get("kind") not in {"filament", "print"}:
            continue
        try:
            slicer = SlicerType(str(record.get("engine")))
        except ValueError:
            continue
        target = applicable.get(slicer)
        if target is None:
            continue
        payload = record.get("profile")
        context = payload.get("context") if isinstance(payload, Mapping) else None
        current = payload.get("data") if isinstance(payload, Mapping) else None
        source_id = context.get("source_id") if isinstance(context, Mapping) else None
        if not isinstance(source_id, str) or not isinstance(current, Mapping):
            raise TypeError(f"Profile record {record.get('id')} has no source identity")
        candidates = profiles.get((slicer, source_id), [])
        if not candidates:
            raise ValueError(f"Profile record {record.get('id')} has no stored source")
        profile = _profile_for_record(candidates, current, target.version)
        current_schema, target_schemas = schemas[slicer]
        compat: dict[str, Any] = {}
        for compatibility in target.compatibility:
            previous = (
                profile.evaluate_at_or_before(compatibility.version)
                if _version_key(profile.first_seen)
                <= _version_key(compatibility.version)
                else {}
            )
            delta = backwards_delta(
                current,
                previous,
                current_schema,
                target_schemas[compatibility.gcode_abi],
            )
            if delta is not None:
                compat[compatibility.gcode_abi] = delta
        if compat:
            record["compat"] = compat
