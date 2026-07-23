"""
Full mapping pipeline: printer models → filament profiles → print profiles → export.

Maps slicer profiles to SimplyPrint printer model IDs, resolves filament
and print profile compatibility, and exports the results.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

import requests

from .brands import BRAND_MAPS, normalize_brand
from .conditions import evaluate_printer_condition
from .index import (
    ProfileIndex,
    is_profile_model_specific,
    build_generic_profile_index,
    resolve_generic_id,
)
from .matching import match_printer_model
from .models import ProfileType, SlicerType, StoredProfile
from .parsers.cura import (
    CURA_MATERIAL_RECOMPUTE_PLAN,
    _expression_dependencies,
    build_cura_scene_context,
    resolve_cura_overlay,
)
from .resources import ResourceStore
from .store import ProfileStore
from .versions import normalize_version, version_key

logger = logging.getLogger(__name__)

_DEFAULT_SP_SLICER_VERSIONS_API_URL = (
    "https://slicing-test.simplyprint.io/api/v1/slicers/versions"
)


def _get_sp_api_url() -> str:
    """Get the SimplyPrint API URL from the SP_API_URL environment variable."""
    url = os.environ.get("SP_API_URL")
    if not url:
        raise RuntimeError(
            "SP_API_URL environment variable is not set. "
            "Set it to the full SimplyPrint printer model endpoint URL."
        )
    return url


def _get_sp_slicer_versions_url() -> str:
    """Return the SimplyPrint endpoint that declares supported slicer versions."""
    return os.environ.get(
        "SP_SLICER_VERSIONS_API_URL", _DEFAULT_SP_SLICER_VERSIONS_API_URL
    )


# Slicers that participate in model mapping.
_MAPPING_SLICERS = list(SlicerType)

# These slicers publish versioned vendor-profile releases whose version
# namespace matches the runtime versions returned by SimplyPrint.  Branch and
# externally sourced profile sets use their own version stream, so comparing
# those versions to an engine/runtime version can incorrectly hide every
# profile (for example Prusa profile data at 3.0.0 with a 2.9.6 runtime).
_VERSION_GUARDED_SLICERS = frozenset(
    {
        SlicerType.ANYCUBICSLICER,
        SlicerType.BAMBUSTUDIO,
        SlicerType.CREALITYPRINT,
        SlicerType.ELEGOOSLICER,
        SlicerType.ORCASLICER,
    }
)

_IMPORT_ARTIFACT_FILENAMES = {
    "machine_profiles.json",
    "print_profiles.json",
    "filament_profiles.json",
    "generic_filament_profiles.json",
}


def _stable_version(profile: StoredProfile) -> str:
    """Return the latest non-nightly version for a profile.

    Falls back to last_seen if no stable version exists.
    """
    last = profile.last_seen
    if not last.startswith("nightly"):
        return last

    # Walk all versioned settings to find the latest stable version key
    best: str | None = None
    for versions_dict in profile.settings.values():
        for ver in versions_dict:
            if not ver.startswith("nightly"):
                if best is None or version_key(ver) > version_key(best):
                    best = ver
    return best or last


def _evaluate_stable(
    profile: StoredProfile,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> dict[str, Any]:
    """Evaluate a profile at its latest SimplyPrint-supported stable version."""
    version = _stable_version(profile)
    try:
        slicer = SlicerType(profile.slicer)
    except ValueError:
        slicer = None
    if version_guards and slicer in _VERSION_GUARDED_SLICERS:
        guard = version_guards.get(slicer)
        if guard and version_key(guard) < version_key(version):
            version = guard
    return profile.evaluate(version)


def _profile_payload(
    profile: StoredProfile, data: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Build the public role wrapper while preserving legacy slicer output."""

    snapshot = data if data is not None else _evaluate_stable(profile)
    payload: dict[str, Any] = {"data": snapshot}
    if profile.context:
        payload["context"] = profile.context
    if profile.setting_scopes:
        payload["setting_scopes"] = {
            key: scope
            for key, scope in profile.setting_scopes.items()
            if key in snapshot
        }
    for metadata_key in ("attributes", "compatibility"):
        metadata = profile.context.get(metadata_key)
        if isinstance(metadata, Mapping):
            payload[metadata_key] = dict(metadata)
    return payload


def _model_variants(profile: StoredProfile, data: Mapping[str, Any]) -> list[str]:
    variants = profile.context.get("variants")
    if isinstance(variants, list):
        runtime = profile.context.get("runtime")
        active_tool_index = (
            runtime.get("active_tool_index") if isinstance(runtime, Mapping) else None
        )
        return [
            str(item["key"])
            for item in variants
            if isinstance(item, dict)
            and item.get("key") is not None
            and (
                active_tool_index is None
                or not isinstance(item.get("runtime_compatible_tool_indices"), list)
                or active_tool_index in item["runtime_compatible_tool_indices"]
            )
        ]

    nozzle_str = data.get("nozzle_diameter", "")
    if isinstance(nozzle_str, list):
        nozzle_str = ";".join(str(nozzle) for nozzle in nozzle_str)
    variants_raw = data.get("variants", nozzle_str)
    if isinstance(variants_raw, str):
        return [value.strip() for value in variants_raw.split(";") if value.strip()]
    return [str(value) for value in variants_raw] if variants_raw else []


def _format_variant_scalar(value: Any) -> str:
    """Format a numeric hardware variant without changing opaque identifiers."""
    try:
        return f"{float(value):g}"
    except (TypeError, ValueError):
        return str(value)


def _machine_profile_variant(
    profile: StoredProfile, data: Mapping[str, Any]
) -> str | None:
    """Resolve one machine profile's variant from its concrete hardware data.

    In inherited sources either ``printer_variant`` or ``nozzle_diameter`` may
    be stale.  When they disagree, the source profile's final numeric token
    before ``nozzle`` disambiguates them.  Opaque parser-defined identifiers
    remain authoritative.
    """

    declared = profile.context.get("printer_variant") or data.get("printer_variant")
    raw_nozzles = data.get("nozzle_diameter")
    if raw_nozzles is None:
        raw_nozzles = data.get("machine_nozzle_size")
    if isinstance(raw_nozzles, str):
        raw_nozzles = [
            item.strip()
            for item in raw_nozzles.replace(";", ",").split(",")
            if item.strip()
        ]
    elif not isinstance(raw_nozzles, list):
        raw_nozzles = [raw_nozzles] if raw_nozzles not in (None, "") else []
    nozzles = {
        _format_variant_scalar(nozzle)
        for nozzle in raw_nozzles
        if nozzle not in (None, "")
    }

    if len(nozzles) == 1:
        nozzle_variant = next(iter(nozzles))
        if declared is None:
            return nozzle_variant
        try:
            declared_variant = _format_variant_scalar(float(declared))
        except (TypeError, ValueError):
            return str(declared)
        if declared_variant != nozzle_variant:
            for source_name in (data.get("name"), profile.name):
                if (
                    not isinstance(source_name, str)
                    or "nozzle" not in source_name.casefold()
                ):
                    continue
                before_nozzle = source_name.casefold().rsplit("nozzle", 1)[0]
                numeric_tokens = re.findall(
                    r"(?<![a-z0-9])(\d+(?:\.\d+)?)(?:\s*mm)?(?=$|[^a-z0-9])",
                    before_nozzle,
                )
                if numeric_tokens:
                    named_variant = _format_variant_scalar(numeric_tokens[-1])
                    if named_variant in {declared_variant, nozzle_variant}:
                        return named_variant
            return declared_variant

    if declared is not None:
        return str(declared)
    return None


def _variant_identity_lookup_key(identity: Mapping[str, Any]) -> str:
    """Return a stable lookup key for a parser-declared hardware identity."""

    required = {
        "native_id",
        "nozzle_diameter",
        "nozzle_volume_type",
        "tool_indices",
        "hotend_id",
    }
    missing = sorted(
        key for key in required if key not in identity or identity[key] is None
    )
    if missing:
        raise ValueError(
            "incomplete hardware variant identity; missing " + ", ".join(missing)
        )
    native_id = identity["native_id"]
    hotend_id = identity["hotend_id"]
    if not isinstance(native_id, str) or not native_id.strip():
        raise ValueError("hardware variant identity has invalid native_id")
    if not isinstance(hotend_id, str) or not hotend_id.strip():
        raise ValueError("hardware variant identity has invalid hotend_id")

    nozzle_diameter = identity["nozzle_diameter"]
    if isinstance(nozzle_diameter, bool):
        raise ValueError("hardware variant identity has invalid nozzle_diameter")
    try:
        nozzle_diameter = float(nozzle_diameter)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "hardware variant identity has invalid nozzle_diameter"
        ) from exc
    if not math.isfinite(nozzle_diameter) or nozzle_diameter <= 0:
        raise ValueError("hardware variant identity has invalid nozzle_diameter")

    tool_indices = identity["tool_indices"]
    if not isinstance(tool_indices, list) or not tool_indices or not all(
        isinstance(index, int) and not isinstance(index, bool) and index >= 0
        for index in tool_indices
    ):
        raise ValueError("hardware variant identity has invalid tool_indices")
    if len(set(tool_indices)) != len(tool_indices):
        raise ValueError("hardware variant identity has duplicate tool_indices")

    nozzle_volume_type = identity["nozzle_volume_type"]
    if nozzle_volume_type not in {"standard", "high_flow"}:
        raise ValueError("hardware variant identity has invalid nozzle_volume_type")
    canonical = {
        "hotend_id": hotend_id,
        "native_id": native_id,
        "nozzle_diameter": nozzle_diameter,
        "nozzle_volume_type": nozzle_volume_type,
        "tool_indices": sorted(tool_indices),
    }
    return "__variant_identity__:" + json.dumps(
        canonical, sort_keys=True, separators=(",", ":")
    )


def _structured_variant_identity(source: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read the normalized identity, including the pre-schema-v1 field layout."""

    declared = source.get("variant_identity") or source.get("identity")
    if isinstance(declared, Mapping):
        return dict(declared)

    attributes = source.get("attributes")
    if not isinstance(attributes, Mapping):
        attributes = {}
    runtime = source.get("runtime")
    if not isinstance(runtime, Mapping):
        runtime = {}
    identity = {
        "native_id": source.get("native_id") or source.get("id"),
        "nozzle_diameter": source.get("nozzle_diameter")
        or attributes.get("nozzle_diameter"),
        "nozzle_volume_type": attributes.get("nozzle_volume_type"),
        "tool_indices": source.get("runtime_compatible_tool_indices")
        or runtime.get("supported_tool_indices")
        or runtime.get("compatible_tool_indices"),
        "hotend_id": attributes.get("hotend_id"),
    }
    return identity if all(value is not None for value in identity.values()) else None


def _model_variant_identity(
    machine_model: StoredProfile, variant: str
) -> Mapping[str, Any] | None:
    descriptors = machine_model.context.get("variants")
    if not isinstance(descriptors, list):
        return None
    matches = [
        _structured_variant_identity(item)
        for item in descriptors
        if isinstance(item, Mapping)
        and str(item.get("key")) == variant
    ]
    matches = [identity for identity in matches if identity is not None]
    if len(matches) > 1:
        raise ValueError(
            f"ambiguous hardware identity for {machine_model.name!r} variant {variant!r}"
        )
    return matches[0] if matches else None


def _profile_name_lookup_key(name: Any) -> str:
    return f"__profile_name__:{name}"


def _variant_lookup_key(
    profile: StoredProfile, data: Mapping[str, Any], fallback_name: str, variant: str
) -> str:
    model_key = profile.context.get("definition") or data.get("name", fallback_name)
    return str(model_key) + variant


def _find_variant_lookup(
    machine_model: StoredProfile,
    machine_data: Mapping[str, Any],
    fallback_name: str,
    variant: str,
    variant_lookup: Mapping[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Find a concrete machine role across native and display-name identities."""

    declared_identity = _model_variant_identity(machine_model, variant)
    if declared_identity is not None:
        return variant_lookup.get(_variant_identity_lookup_key(declared_identity))

    model_name = _model_display_name(machine_model, machine_data, fallback_name)
    display_model_names = _variant_display_model_names(model_name)
    family_name = machine_data.get("family")
    if isinstance(family_name, str) and family_name:
        display_model_names.extend(_variant_display_model_names(family_name))
    display_model_names = list(dict.fromkeys(display_model_names))

    display_names: list[str] = []
    for display_model_name in display_model_names:
        display_names.extend(
            [
                f"{display_model_name} {variant} nozzle",
                f"{display_model_name} {variant} Nozzle",
                f"{display_model_name} {variant}mm nozzle",
                f"{display_model_name} {variant}Nozzle",
                f"{display_model_name} ({variant} mm nozzle)",
                f"{display_model_name} ({variant}mm nozzle)",
                display_model_name,
            ]
        )

    for display_name in display_names:
        lookup = variant_lookup.get(_profile_name_lookup_key(display_name))
        if lookup is None:
            lookup = variant_lookup.get(display_name)
        if lookup is not None and _variant_matches_item(variant, lookup):
            return lookup

    lookup_keys = [
        _variant_lookup_key(machine_model, machine_data, fallback_name, variant)
    ]
    for candidate in (
        machine_model.context.get("model_id"),
        machine_data.get("model_id"),
        machine_data.get("printer_model"),
        family_name,
    ):
        if candidate not in (None, ""):
            lookup_keys.append(str(candidate) + variant)
    for lookup_key in dict.fromkeys(lookup_keys):
        lookup = variant_lookup.get(lookup_key)
        if lookup is not None:
            return lookup

    unique_items: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in variant_lookup.values():
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        unique_items.append(item)

    display_names_lower = {name.casefold() for name in display_names}
    for item in unique_items:
        item_name = str(item.get("name", ""))
        if item_name.casefold() in display_names_lower and _variant_matches_item(
            variant, item
        ):
            return item

    compact_display_names = {
        name.replace(" ", "").casefold() for name in display_names
    }
    compact_prefix_matches: list[dict[str, Any]] = []
    for item in unique_items:
        item_name = str(item.get("name", ""))
        compact_name = item_name.replace(" ", "").casefold()
        if compact_name in compact_display_names and _variant_matches_item(
            variant, item
        ):
            return item
        if any(compact_name.startswith(name) for name in compact_display_names) and (
            _variant_matches_item(variant, item)
        ):
            compact_prefix_matches.append(item)
    compact_prefix_matches = list(
        {str(item.get("name", "")): item for item in compact_prefix_matches}.values()
    )
    if len(compact_prefix_matches) == 1:
        return compact_prefix_matches[0]
    preferred_prefix_matches = [
        item
        for item in compact_prefix_matches
        if "+m4hotend" in str(item.get("name", "")).replace(" ", "").casefold()
    ]
    if len(preferred_prefix_matches) == 1:
        return preferred_prefix_matches[0]

    candidates: list[dict[str, Any]] = []
    for item in unique_items:
        if _named_machine_variant_matches(item, model_name, variant):
            candidates.append(item)
    if candidates:
        normalized_model = _normalise_native_identity(model_name)
        return min(
            candidates,
            key=lambda item: (
                _normalise_native_identity(item.get("name")) != normalized_model,
                _normalise_native_identity(item.get("name")),
            ),
        )

    return None


def _variant_printer_identities(lookup: Mapping[str, Any]) -> set[str]:
    """Return every exact upstream printer identity carried by a machine role."""

    data = lookup.get("data")
    if not isinstance(data, Mapping):
        data = {}
    candidates = (
        lookup.get("name"),
        data.get("name"),
        data.get("printer_settings_id"),
    )
    identities = {
        str(candidate) for candidate in candidates if candidate not in (None, "")
    }
    aliases = lookup.get("_compatible_printer_identities")
    if isinstance(aliases, list):
        identities.update(str(alias) for alias in aliases if alias not in (None, ""))
    return identities


def _index_variant_payload(
    index: dict[str, dict[str, Any]],
    key: str,
    payload: dict[str, Any],
    *,
    replace: bool,
) -> None:
    """Index a machine role and preserve aliases from hardware-key collisions.

    Upstream slicers can publish several named machine presets for one physical
    model/nozzle combination.  SimplyPrint exposes one runtime variant for that
    hardware slot, but material and print compatibility may reference any of
    the upstream preset names.  The aliases are internal mapping metadata and
    are removed before export.
    """

    existing = index.get(key)
    if existing is not None and existing is not payload:
        identities = _variant_printer_identities(existing)
        identities.update(_variant_printer_identities(payload))
        aliases = sorted(identities)
        existing["_compatible_printer_identities"] = aliases
        payload["_compatible_printer_identities"] = aliases
    if replace or existing is None:
        index[key] = payload


def _profile_selection_defaults(profile: StoredProfile) -> dict[str, Any] | None:
    """Return generic profile-selection metadata for a machine model."""
    declared = profile.context.get("selection_defaults")
    return dict(declared) if isinstance(declared, Mapping) else None


def _public_variant_payload(
    lookup: Mapping[str, Any],
    selection_defaults: Mapping[str, Any] | None = None,
    variant_key: str | None = None,
) -> dict[str, Any]:
    """Remove mapper-only metadata from a runtime machine role."""

    payload = {
        key: value
        for key, value in lookup.items()
        if key != "_compatible_printer_identities"
    }
    context = payload.get("context")
    public_context: dict[str, Any] = {}
    if isinstance(context, Mapping):
        public_context = {
            key: value
            for key, value in context.items()
            if key
            not in {
                CURA_MATERIAL_RECOMPUTE_PLAN,
                "scene",
                "variant_aliases",
                "variant_identity_aliases",
            }
        }
    if selection_defaults:
        public_context.setdefault("selection_defaults", dict(selection_defaults))
    if variant_key is not None:
        public_context.setdefault("variant_key", str(variant_key))
    identity = _structured_variant_identity(public_context)
    if identity is not None:
        # Validate and normalize the identity contract before it reaches the
        # importer. The map key remains the canonical UI/runtime selector.
        _variant_identity_lookup_key(identity)
        public_context.setdefault("variant_identity", identity)
    if public_context:
        payload["context"] = public_context
    return payload


def _model_display_name(
    profile: StoredProfile, data: Mapping[str, Any], fallback: str
) -> str:
    return str(profile.context.get("display_name") or data.get("name", fallback))


def _uses_material_resource_constraints(profile: StoredProfile) -> bool:
    """Whether material compatibility is described by native resource IDs."""
    return any(
        key in profile.context for key in ("include_materials", "exclude_materials")
    )


def _uses_definition_quality_constraints(profile: StoredProfile) -> bool:
    """Whether print compatibility is described by native definition metadata."""
    return "quality_definition" in profile.context


def _diameters_match(machine: Any, material: Any) -> bool:
    if machine is None or material is None:
        return True
    try:
        return abs(float(machine) - float(material)) < 0.01
    except (TypeError, ValueError):
        return False


def _normalise_native_identity(value: Any) -> str:
    """Normalise source identifiers for exact, punctuation-insensitive matching."""
    return "".join(
        character for character in str(value).casefold() if character.isalnum()
    )


def _named_machine_variant_matches(
    lookup: Mapping[str, Any], model_name: str, variant: str
) -> bool:
    """Match equivalent source-native spellings of a hardware variant name."""

    data = lookup.get("data")
    if not isinstance(data, Mapping):
        return False

    declared_values: list[Any] = [data.get("printer_variant")]
    raw_nozzles = data.get("nozzle_diameter")
    if isinstance(raw_nozzles, str):
        declared_values.extend(
            item.strip()
            for item in raw_nozzles.replace(";", ",").split(",")
            if item.strip()
        )
    elif isinstance(raw_nozzles, list):
        declared_values.extend(raw_nozzles)
    elif raw_nozzles is not None:
        declared_values.append(raw_nozzles)
    if str(variant) not in {
        _format_variant_scalar(value)
        for value in declared_values
        if value not in (None, "")
    }:
        return False

    normalized_model = _normalise_native_identity(model_name)
    normalized_name = _normalise_native_identity(lookup.get("name"))
    if normalized_name == normalized_model:
        return True
    if not normalized_name.startswith(normalized_model):
        return False

    suffix = normalized_name[len(normalized_model) :]
    if suffix.endswith("nozzle"):
        suffix = suffix[: -len("nozzle")]
    if suffix.endswith("mm"):
        suffix = suffix[: -len("mm")]
    return suffix == _normalise_native_identity(variant)


def _material_matches_machine_identifier(
    machine_model: StoredProfile,
    machine_data: Mapping[str, Any],
    identifier: Mapping[str, Any],
) -> bool:
    product = identifier.get("product")
    if not product:
        return False

    product_candidates: set[str] = set()
    for candidate in (
        machine_model.name,
        machine_model.context.get("display_name"),
        machine_model.context.get("definition"),
        machine_data.get("name"),
    ):
        if candidate:
            product_candidates.add(_normalise_native_identity(candidate))
    for candidate in machine_model.context.get("definition_inheritance") or []:
        product_candidates.add(_normalise_native_identity(candidate))
    if _normalise_native_identity(product) not in product_candidates:
        return False

    manufacturer = identifier.get("manufacturer")
    if not manufacturer:
        return True
    manufacturer_candidates = {
        _normalise_native_identity(machine_model.vendor),
        _normalise_native_identity(machine_model.context.get("manufacturer") or ""),
    }
    return _normalise_native_identity(manufacturer) in manufacturer_candidates


def _material_is_compatible(
    machine_model: StoredProfile,
    machine_data: Mapping[str, Any],
    variant_data: Mapping[str, Any],
    variant_context: Mapping[str, Any],
    material_profile: StoredProfile,
    material_data: Mapping[str, Any],
) -> bool:
    """Evaluate normalized native material constraints for one machine variant."""
    material_id = material_profile.native_id or ""
    include = set(machine_model.context.get("include_materials") or [])
    exclude = set(machine_model.context.get("exclude_materials") or [])
    if material_id in exclude or (include and material_id not in include):
        return False
    if not _diameters_match(
        variant_data.get("material_diameter"), material_data.get("material_diameter")
    ):
        return False

    compatibility = material_profile.context.get("compatibility")
    if not isinstance(compatibility, Mapping):
        return True
    default_compatible = bool(compatibility.get("default", True))
    constraints = compatibility.get("machines")
    if not isinstance(constraints, list):
        return default_compatible

    matching_constraints: list[Mapping[str, Any]] = []
    for constraint in constraints:
        if not isinstance(constraint, Mapping):
            continue
        identifiers = constraint.get("identifiers")
        if not isinstance(identifiers, list):
            continue
        if any(
            isinstance(identifier, Mapping)
            and _material_matches_machine_identifier(
                machine_model, machine_data, identifier
            )
            for identifier in identifiers
        ):
            matching_constraints.append(constraint)
    if not matching_constraints:
        return default_compatible

    hotend_id = _selected_hotend_id(variant_data, variant_context)
    normalized_hotend = _normalise_native_identity(hotend_id) if hotend_id else ""

    results: list[bool] = []
    for constraint in matching_constraints:
        machine_compatible = bool(constraint.get("compatible", default_compatible))
        hotends = constraint.get("hotends")
        if not normalized_hotend or not isinstance(hotends, Mapping) or not hotends:
            results.append(machine_compatible)
            continue
        matching_hotend = next(
            (
                compatible
                for native_id, compatible in hotends.items()
                if _normalise_native_identity(native_id) == normalized_hotend
            ),
            None,
        )
        # A machine-specific hotend list is a finite source-native constraint.
        # An absent selected hotend is therefore not compatible.
        results.append(bool(matching_hotend) if matching_hotend is not None else False)
    return any(results)


def _selected_hotend_id(
    variant_data: Mapping[str, Any], variant_context: Mapping[str, Any]
) -> Any:
    attributes = variant_context.get("attributes")
    return (
        attributes.get("hotend_id") if isinstance(attributes, Mapping) else None
    ) or variant_data.get("machine_nozzle_id")


def _resolve_material_overrides(
    machine_model: StoredProfile,
    machine_data: Mapping[str, Any],
    variant_data: Mapping[str, Any],
    variant_context: Mapping[str, Any],
    material_profile: StoredProfile,
    material_data: Mapping[str, Any],
    overlay_cache: dict[str, tuple[dict[str, Any], dict[str, str]]] | None = None,
    plan_cache: dict[int, tuple[str, set[str]]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, str]]:
    """Apply source-native machine and hotend material settings to one role."""

    overrides = material_profile.context.get("machine_overrides")
    selected_hotend = _selected_hotend_id(variant_data, variant_context)
    normalized_hotend = (
        _normalise_native_identity(selected_hotend) if selected_hotend else ""
    )
    resolved = dict(material_data)
    matched_products: set[str] = set()
    matched_hotend: str | None = None
    for override in overrides if isinstance(overrides, list) else []:
        if not isinstance(override, Mapping):
            continue
        identifiers = override.get("identifiers")
        if not isinstance(identifiers, list) or not any(
            isinstance(identifier, Mapping)
            and _material_matches_machine_identifier(
                machine_model, machine_data, identifier
            )
            for identifier in identifiers
        ):
            continue

        matched_products.update(
            str(identifier["product"])
            for identifier in identifiers
            if isinstance(identifier, Mapping) and identifier.get("product")
        )
        settings = override.get("settings")
        if isinstance(settings, Mapping):
            resolved.update(
                {
                    key: value
                    for key, value in settings.items()
                    if not (isinstance(value, str) and value.lstrip().startswith("="))
                }
            )
        if not normalized_hotend:
            continue
        hotends = override.get("hotends")
        if not isinstance(hotends, list):
            continue
        for hotend in hotends:
            if not isinstance(hotend, Mapping) or (
                _normalise_native_identity(hotend.get("id")) != normalized_hotend
            ):
                continue
            hotend_settings = hotend.get("settings")
            if isinstance(hotend_settings, Mapping):
                resolved.update(
                    {
                        key: value
                        for key, value in hotend_settings.items()
                        if not (
                            isinstance(value, str) and value.lstrip().startswith("=")
                        )
                    }
                )
                matched_hotend = str(hotend.get("id") or selected_hotend)
            break
    plan = variant_context.get(CURA_MATERIAL_RECOMPUTE_PLAN)
    cache_key = _overlay_resolution_cache_key(
        variant_data, resolved, plan, plan_cache
    )
    cached = overlay_cache.get(cache_key) if overlay_cache is not None else None
    if cached is None:
        resolved, dependent_scopes = resolve_cura_overlay(
            variant_data,
            resolved,
            plan,
        )
        if overlay_cache is not None:
            overlay_cache[cache_key] = (resolved, dependent_scopes)
    else:
        resolved, dependent_scopes = cached
    if resolved == dict(material_data):
        return resolved, None, dependent_scopes
    return (
        resolved,
        {
            "hotend_id": matched_hotend or selected_hotend,
            "machine_products": sorted(matched_products),
            "dependent_settings": sorted(dependent_scopes),
        },
        dependent_scopes,
    )


def _overlay_resolution_cache_key(
    baseline: Mapping[str, Any],
    overlay: Mapping[str, Any],
    plan: Any,
    plan_cache: dict[int, tuple[str, set[str]]] | None,
) -> str:
    """Identify the immutable inputs that can affect an overlay result."""

    plan_id = id(plan)
    cached_plan = plan_cache.get(plan_id) if plan_cache is not None else None
    if cached_plan is None:
        canonical_plan = json.dumps(
            plan if isinstance(plan, Mapping) else {},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        schema_keys = set(baseline) | set(overlay)
        dependencies: set[str] = set()
        if isinstance(plan, Mapping):
            schema_keys.update(str(key) for key in plan)
            for specification in plan.values():
                if not isinstance(specification, Mapping):
                    continue
                expression = specification.get("expression")
                if isinstance(expression, str):
                    dependencies.update(
                        _expression_dependencies(expression, schema_keys)
                    )
        cached_plan = (canonical_plan, dependencies)
        if plan_cache is not None:
            plan_cache[plan_id] = cached_plan

    canonical_plan, dependencies = cached_plan
    inputs = {
        key: baseline.get(key)
        for key in dependencies
        if key not in overlay
    }
    digest = hashlib.sha256()
    for value in (canonical_plan, inputs, overlay):
        encoded = (
            value
            if isinstance(value, str)
            else json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
        )
        digest.update(encoded.encode())
        digest.update(b"\0")
    return digest.hexdigest()


def _variant_material_role(
    material_profile: StoredProfile,
    material_name: str,
    resolved_data: Mapping[str, Any],
    setting_scopes: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Create a content-addressed identity for a resolved material role.

    Machine-specific provenance is deliberately not part of the identity: two
    hardware paths that produce identical runtime settings are the same role
    and can carry a union of their compatible-printer relations.
    """

    source_native_id = (
        material_profile.native_id
        or material_profile.setting_id
        or material_profile.name
    )
    semantic_payload = {
        "data": resolved_data,
        "setting_scopes": setting_scopes,
        "source_native_id": source_native_id,
    }
    digest = hashlib.sha256(
        json.dumps(
            semantic_payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()[:16]
    context = {
        key: value
        for key, value in material_profile.context.items()
        if key not in {"compatibility", "machine_overrides"}
    }
    context.update(
        {
            "native_id": f"{source_native_id}#resolved-{digest}",
            "source_native_id": source_native_id,
        }
    )
    return material_name, context


def _machine_model_export(
    profile: StoredProfile, data: Mapping[str, Any]
) -> dict[str, Any]:
    """Project normalized discovery metadata without polluting engine settings."""
    exported = dict(data)
    exported.setdefault("name", _model_display_name(profile, data, profile.name))
    for key in (
        "bed_assets",
        "bed_model",
        "bed_texture",
        "machine_extruder_trains",
        "preferred_variant_key",
        "preferred_variant_name",
        "runtime",
        "scene",
        "tool_topology",
    ):
        value = profile.context.get(key)
        if value is not None:
            exported[key] = value
    if profile.slicer == SlicerType.CURA.value:
        # Cura's upstream platform models and textures do not share a stable
        # render-space contract.  Never expose them to SimplyPrint clients;
        # Cura machines must use the generic build surface.
        for key in ("bed_assets", "bed_model", "bed_texture"):
            exported.pop(key, None)
        scene = build_cura_scene_context(data)
        if scene is None:
            exported.pop("scene", None)
        else:
            exported["scene"] = scene
    selection_defaults = _profile_selection_defaults(profile)
    if selection_defaults:
        exported["selection_defaults"] = selection_defaults
    return exported


@dataclass
class ModelMap:
    """Result of mapping slicer machine_model profiles to SimplyPrint model IDs."""

    # model_id → slicer_value → list of StoredProfile keys (vendor/name)
    model_to_profiles: dict[int, dict[str, list[str]]] = field(default_factory=dict)

    # slicer_value → lookup_key → {name, data (flat settings snapshot)}
    variant_map: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    # Tracking
    failed_brands: set[str] = field(default_factory=set)
    failed_models: set[str] = field(default_factory=set)


def fetch_sp_model_data() -> dict[str, Any]:
    """Fetch printer model data from the SimplyPrint API."""
    url = _get_sp_api_url()
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_sp_slicer_versions() -> dict[SlicerType, str]:
    """Fetch the newest stable slicer version supported by SimplyPrint."""
    resp = requests.get(_get_sp_slicer_versions_url(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    guards: dict[SlicerType, str] = {}
    for slicer_data in data.get("slicers", []):
        if not isinstance(slicer_data, Mapping):
            continue
        name = slicer_data.get("name")
        latest = slicer_data.get("latest")
        if not isinstance(name, str) or not isinstance(latest, str) or not latest:
            continue
        try:
            slicer = SlicerType(name.casefold())
            if slicer in _VERSION_GUARDED_SLICERS:
                guards[slicer] = normalize_version(latest)
        except ValueError:
            logger.warning("Ignoring unsupported SimplyPrint slicer %r", name)
    return guards


def _prepare_sp_data(
    raw: dict[str, Any],
) -> tuple[list[str], list[dict], dict[int, list[str]]]:
    """Normalise the raw SP API response into lookup-friendly structures.

    Child variants inherit their ancestors' canonical and slicer profile names.
    Slicers commonly publish only the base machine profile for variants such as
    Combo editions, so the ``parent`` relationship is part of profile identity
    rather than catalog-only metadata.

    Returns (sp_brands, sp_models, sp_slicer_names).
    """
    sp_brands = [b.casefold() for b in raw["brands"]]
    sp_models: list[dict] = []
    sp_slicer_names: dict[int, list[str]] = {}

    for source_model in raw["models"]:
        model = dict(source_model)
        model["brand"] = model["brand"].casefold()
        model["name"] = model["name"].casefold()
        sp_models.append(model)

        # ``slicerProfileNames`` is the canonical API field.  Accept the
        # snake_case spelling as a compatibility input, but prefer the
        # canonical field whenever both are present.
        alias_names = (
            model.get("slicerProfileNames")
            if "slicerProfileNames" in model
            else model.get("slicer_profile_names")
        )
        if isinstance(alias_names, str):
            alias_names = [alias_names]
        if alias_names:
            sp_slicer_names[model["id"]] = [
                name for name in alias_names if isinstance(name, str)
            ]

    models_by_id = {model["id"]: model for model in sp_models}
    direct_slicer_names = {
        model_id: list(names) for model_id, names in sp_slicer_names.items()
    }
    for model in sp_models:
        inherited_names: list[str] = []
        seen_ids = {model["id"]}
        parent_id = model.get("parent")
        while parent_id is not None and parent_id not in seen_ids:
            seen_ids.add(parent_id)
            parent = models_by_id.get(parent_id)
            if parent is None or parent["brand"] != model["brand"]:
                break

            inherited_names.append(parent["name"])
            inherited_names.extend(direct_slicer_names.get(parent_id, []))
            parent_id = parent.get("parent")

        if inherited_names:
            sp_slicer_names[model["id"]] = list(
                dict.fromkeys([*sp_slicer_names.get(model["id"], []), *inherited_names])
            )

    return sp_brands, sp_models, sp_slicer_names


def map_printer_models(
    store: ProfileStore,
    index: ProfileIndex,
    sp_data: dict[str, Any],
    slicers: list[SlicerType] | None = None,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> ModelMap:
    """
    Match slicer machine_model profiles to SimplyPrint printer model IDs
    and build a variant lookup map.

    Args:
        store: The profile store.
        index: A built ProfileIndex.
        sp_data: Raw SimplyPrint API response (brands + models).
        slicers: Which slicers to process (default: all mapping-supported slicers).

    Returns:
        A ModelMap with model→profile mappings and a variant lookup map.
    """
    sp_brands, sp_models, sp_slicer_names = _prepare_sp_data(sp_data)
    result = ModelMap()
    slicers = slicers or _MAPPING_SLICERS

    for slicer in slicers:
        brand_map = BRAND_MAPS.get(slicer, {})

        # Get all machine_model profiles for this slicer
        machine_models = index.find_by_type(slicer, ProfileType.MACHINE_MODEL)
        if not machine_models:
            continue

        for profile in machine_models:
            data = _evaluate_stable(profile, version_guards)
            if not data:
                continue
            name = (
                profile.context.get("display_name")
                or data.get("name")
                or profile.name
            )
            vendor = profile.vendor

            ids = match_printer_model(
                sp_models,
                sp_brands,
                sp_slicer_names,
                vendor,
                name,
                brand_map,
            )

            if ids:
                profile_key = f"{vendor}/{profile.name}"
                for model_id in ids:
                    result.model_to_profiles.setdefault(model_id, {}).setdefault(
                        slicer.value, []
                    ).append(profile_key)
            else:
                normalized = normalize_brand(slicer, vendor)
                if normalized not in sp_brands:
                    result.failed_brands.add(vendor)
                else:
                    result.failed_models.add(f"{vendor}/{name}")

        # Build variant lookup map for this slicer
        _build_variant_map(store, index, slicer, result, version_guards)

    return result


def _build_variant_map(
    store: ProfileStore,
    index: ProfileIndex,
    slicer: SlicerType,
    result: ModelMap,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> None:
    """Build the variant lookup map for a slicer.

    For each machine profile, creates a lookup key from printer_model + variant
    (nozzle size) so that downstream mapping can quickly find the printer
    configuration for a given model + nozzle combination.
    """
    slicer_val = slicer.value
    result.variant_map.setdefault(slicer_val, {})

    machine_profiles = index.find_by_type(slicer, ProfileType.MACHINE)
    for profile in machine_profiles:
        data = _evaluate_stable(profile, version_guards)
        if not data:
            continue

        printer_model = profile.context.get("printer_model") or data.get(
            "printer_model"
        )
        if not printer_model:
            continue

        # Parser-declared identities are authoritative. Legacy profile families
        # retain their established name-based compatibility path below.
        variant = _machine_profile_variant(profile, data)
        declared_identity = _structured_variant_identity(profile.context)
        if slicer == SlicerType.CURA:
            if not isinstance(declared_identity, Mapping):
                raise ValueError(
                    f"Cura machine profile {profile.native_id or profile.name!r} "
                    "has no structured variant identity"
                )
        else:
            name_variant = _parse_variant_from_name(data.get("name", profile.name))
            if variant is None:
                variant = name_variant
            elif name_variant and not _same_variant(str(variant), name_variant):
                variant = name_variant
        if variant is None:
            continue

        ptype = data.get("type", "machine")
        if ptype not in ("machine", None):
            continue

        lookup_key = printer_model + variant
        profile_name = data.get("name", profile.name)

        payload = {
            "name": profile_name,
            **_profile_payload(profile, data),
        }
        variant_aliases = profile.context.get("variant_aliases")
        if isinstance(variant_aliases, list):
            payload["_compatible_printer_identities"] = [
                str(alias) for alias in variant_aliases if alias not in (None, "")
            ]
        existing = result.variant_map[slicer_val].get(lookup_key)
        replace = existing is None or _variant_candidate_is_better(
            lookup_key, payload, existing
        )
        _index_variant_payload(
            result.variant_map[slicer_val], lookup_key, payload, replace=replace
        )
        if isinstance(declared_identity, Mapping):
            identity_key = _variant_identity_lookup_key(declared_identity)
            if identity_key in result.variant_map[slicer_val]:
                raise ValueError(
                    f"duplicate structured hardware identity for {profile.name!r}"
                )
            result.variant_map[slicer_val][identity_key] = payload
        # Preserve every source profile even when multiple profiles share the
        # same native model + nozzle key.  Name-based fallback is part of the
        # established machine-model contract and must not depend on directory
        # iteration order.
        _index_variant_payload(
            result.variant_map[slicer_val],
            _profile_name_lookup_key(profile_name),
            payload,
            replace=True,
        )
        # Retain the established raw display-name fallback for non-Cura
        # profile families while the collision-safe key above preserves every
        # named source profile.
        _index_variant_payload(
            result.variant_map[slicer_val], str(profile_name), payload, replace=False
        )

        # Also index by model_id + variant (Orca/BBS use model_id)
        model_id = profile.context.get("model_id") or data.get("model_id")
        if model_id and model_id != printer_model:
            alt_key = model_id + variant
            _index_variant_payload(
                result.variant_map[slicer_val], alt_key, payload, replace=False
            )


def _parse_variant_from_name(name: str) -> str | None:
    """Extract nozzle-like variant value from a machine profile name."""
    match = re.search(
        r"(?<![A-Za-z0-9.])(HF)?(0\.\d+|[12]\.\d+)\s*(?:mm\s*)?nozzle\b",
        name,
        re.IGNORECASE,
    )
    if match:
        prefix = "HF" if match.group(1) else ""
        return prefix + match.group(2)

    # Some upstream machine profiles encode only the nozzle value in the name
    # without the word "nozzle" (for example Kentstrapper ``0.4 v20``).
    # Restrict this fallback to 0.x values so model names like MK2.5 are not
    # misread as nozzle variants.
    match = re.search(r"(?<![A-Za-z0-9.])(0\.\d+)(?![A-Za-z0-9.])", name)
    if match:
        return match.group(1)
    return None


def _variant_candidate_is_better(
    lookup_key: str, candidate: dict[str, Any], existing: dict[str, Any]
) -> bool:
    """Resolve duplicate variant lookup keys.

    Some upstream profiles have a wrong ``printer_model`` value (for example a
    Creality K1 variant claiming ``Creality K1 Max``). Prefer the candidate
    whose display name best matches the lookup model part.
    """
    candidate_name = str(candidate.get("name", "")).lower()
    existing_name = str(existing.get("name", "")).lower()
    model_part = lookup_key
    variant = candidate.get("data", {}).get("printer_variant") or candidate.get(
        "context", {}
    ).get("printer_variant")
    if variant is not None and lookup_key.endswith(str(variant)):
        model_part = lookup_key[: -len(str(variant))]
    model_part = model_part.strip().lower()

    candidate_matches = bool(model_part and model_part in candidate_name)
    existing_matches = bool(model_part and model_part in existing_name)
    return candidate_matches and not existing_matches


def map_filament_profiles(
    store: ProfileStore,
    index: ProfileIndex,
    model_map: ModelMap,
    ofd_index: Any | None = None,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> dict[int, dict[str, list[dict]]]:
    """
    For each mapped printer model, find compatible filament profiles.

    Returns: {model_id: {slicer: [filament_entry, ...]}}
    Each filament_entry is {name, compatible_printers: {model_name: [variants]}, data}.
    """
    output: dict[int, dict[str, list[dict]]] = {}

    # A profile's ``compatible_printers`` relation already describes every
    # machine/variant it applies to.  Keep one shared role pool per engine and
    # write each semantic payload once instead of cloning it into every model
    # directory.  The importer consumes those relations independently of the
    # artifact's containing model directory.
    roles_by_slicer: dict[str, dict[str, dict[str, dict]]] = {}
    role_owners: dict[int, int] = {}
    profile_snapshots: dict[int, dict[str, Any]] = {}
    overlay_cache: dict[str, tuple[dict[str, Any], dict[str, str]]] = {}
    overlay_plan_cache: dict[int, tuple[str, set[str]]] = {}

    def snapshot(profile: StoredProfile) -> dict[str, Any]:
        """Evaluate each immutable source profile once for this mapping run."""
        key = id(profile)
        cached = profile_snapshots.get(key)
        if cached is None:
            cached = profile_snapshots[key] = _evaluate_stable(profile, version_guards)
        return cached

    # Build generic ID lookup per slicer (for resolve_generic_id).
    active_slicers = set()
    for slicer_profiles in model_map.model_to_profiles.values():
        active_slicers.update(slicer_profiles.keys())
    _generic_profiles = build_generic_profile_index(
        index, [SlicerType(s) for s in active_slicers]
    )
    _global_templates = {
        slicer: _global_filament_templates(
            index, SlicerType(slicer), version_guards
        )
        for slicer in active_slicers
    }

    for model_id, slicer_profiles in model_map.model_to_profiles.items():
        for slicer_val, profile_keys in slicer_profiles.items():
            slicer = SlicerType(slicer_val)

            # Gather machine_model profile data and build variant lists
            compatible_filaments = roles_by_slicer.setdefault(slicer_val, {})

            for profile_key in profile_keys:
                vendor, name = profile_key.split("/", 1)
                mm_profile = index.find_by_type(
                    slicer, ProfileType.MACHINE_MODEL, vendor, name
                )
                if not mm_profile:
                    continue
                mm = mm_profile[0]
                mm_data = snapshot(mm)
                if not mm_data:
                    continue
                model_name = _model_display_name(mm, mm_data, name)

                # Get nozzle variants
                variants = _model_variants(mm, mm_data)

                variant_lookup = model_map.variant_map.get(slicer_val, {})
                uses_resource_constraints = _uses_material_resource_constraints(mm)

                # For each variant, find compatible filament profiles
                for variant in variants:
                    lookup = _find_variant_lookup(
                        mm, mm_data, name, variant, variant_lookup
                    )
                    if lookup is None:
                        continue

                    variant_data = lookup["data"]
                    printer_identities = _variant_printer_identities(lookup)
                    printer_name = variant_data.get("name", lookup["name"])
                    if slicer == SlicerType.PRUSASLICER:
                        printer_name = variant_data.get(
                            "printer_settings_id", printer_name
                        )

                    # Find all filament profiles for this vendor
                    filament_profiles = index.find_by_type(
                        slicer,
                        ProfileType.FILAMENT,
                        None if uses_resource_constraints else vendor,
                    )
                    for fp in filament_profiles:
                        fp_data = snapshot(fp)
                        if not fp_data:
                            continue
                        filament_name = fp_data.get("name", fp.name)
                        filament_type = (
                            fp.filament_type
                            or fp.context.get("material_type")
                            or fp_data.get("filament_type", "")
                        )
                        if isinstance(filament_type, list):
                            filament_type = filament_type[0] if filament_type else ""

                        # Check compatibility
                        compat = fp_data.get("compatible_printers", [])
                        if isinstance(compat, str):
                            compat = [
                                x.strip().strip('"')
                                for x in compat.split(";")
                                if x.strip()
                            ]

                        is_compatible = False
                        if uses_resource_constraints:
                            is_compatible = _material_is_compatible(
                                mm,
                                mm_data,
                                variant_data,
                                lookup.get("context", {}),
                                fp,
                                fp_data,
                            )
                        elif printer_identities.intersection(
                            compat
                        ) or _compat_matches_printer(
                            compat, printer_name, model_name, variant
                        ):
                            is_compatible = True
                        else:
                            condition = fp_data.get("compatible_printers_condition")
                            if condition:
                                is_compatible = evaluate_printer_condition(
                                    condition, variant_data, slicer_val
                                )

                        if not is_compatible:
                            continue

                        (
                            resolved_fp_data,
                            resolution,
                            dependent_scopes,
                        ) = _resolve_material_overrides(
                            mm,
                            mm_data,
                            variant_data,
                            lookup.get("context", {}),
                            fp,
                            fp_data,
                            overlay_cache,
                            overlay_plan_cache,
                        )
                        role_name = filament_name
                        role_payload = _profile_payload(fp, resolved_fp_data)
                        if dependent_scopes:
                            role_payload.setdefault("setting_scopes", {}).update(
                                dependent_scopes
                            )
                        if resolution is not None:
                            role_name, role_context = _variant_material_role(
                                fp,
                                filament_name,
                                resolved_fp_data,
                                role_payload.get("setting_scopes", {}),
                            )
                            role_payload["context"] = role_context

                        role = _add_filament_output(
                            compatible_filaments=compatible_filaments,
                            profile=fp,
                            profile_data=resolved_fp_data,
                            filament_name=filament_name,
                            filament_type=filament_type,
                            model_name=model_name,
                            variant=variant,
                            slicer_val=slicer_val,
                            generic_profiles=_generic_profiles,
                            ofd_index=ofd_index,
                            role_name=role_name,
                            role_payload=role_payload,
                        )
                        if role is not None:
                            role_owners.setdefault(id(role), model_id)

                    # Shared Orca library generic @System filament presets are
                    # material presets, not printer-vendor presets. Brand-specific
                    # @System presets must not be attached globally; otherwise
                    # every printer gets unrelated filament brands like AliZ/NIT.
                    for fp in _global_templates.get(slicer_val, []):
                        fp_data = snapshot(fp)
                        if not fp_data:
                            continue
                        filament_name = fp_data.get("name", fp.name)
                        filament_type = fp_data.get("filament_type", "")
                        if isinstance(filament_type, list):
                            filament_type = filament_type[0] if filament_type else ""
                        role = _add_filament_output(
                            compatible_filaments=compatible_filaments,
                            profile=fp,
                            profile_data=fp_data,
                            filament_name=filament_name,
                            filament_type=filament_type,
                            model_name=model_name,
                            variant=variant,
                            slicer_val=slicer_val,
                            generic_profiles=_generic_profiles,
                            ofd_index=ofd_index,
                        )
                        if role is not None:
                            role_owners.setdefault(id(role), model_id)

    # Assign every coalesced role to the first compatible model.  Its complete
    # compatible-printer map retains all model and variant relations.
    for slicer_val, profiles_by_name in roles_by_slicer.items():
        for entries_by_payload in profiles_by_name.values():
            for entry in entries_by_payload.values():
                owner = role_owners[id(entry)]
                output.setdefault(owner, {}).setdefault(slicer_val, []).append(entry)

    return output


def _variant_display_model_names(model_name: str) -> list[str]:
    """Return known display-name aliases used by Prusa-family machine profiles."""
    names = [model_name]

    normalized = model_name.replace("&&", "&")
    if normalized != model_name:
        names.append(normalized)

    core_one = model_name.replace(" && CORE One+", "")
    if core_one != model_name:
        names.append(core_one)

    size_suffix = re.sub(r" (\d+)mm$", r"-\1", model_name)
    if size_suffix != model_name:
        names.append(size_suffix)

    return list(dict.fromkeys(names))


def _variant_matches_item(variant: str, item: dict[str, Any]) -> bool:
    """Check whether a variant lookup item represents the requested nozzle."""
    name_variant = _parse_variant_from_name(str(item.get("name", "")))
    if name_variant is not None:
        return _same_variant(name_variant, variant)

    data = item.get("data", {})
    context = item.get("context", {})
    item_variant = data.get("printer_variant") or context.get("printer_variant")
    if item_variant is not None and _same_variant(str(item_variant), variant):
        return True

    nozzle = data.get("nozzle_diameter") or data.get("machine_nozzle_size")
    if isinstance(nozzle, list):
        nozzle = nozzle[0] if nozzle else None
    if isinstance(nozzle, str):
        nozzle = re.split("[;,]", nozzle)[0].strip()
    if nozzle is not None and _same_variant(str(nozzle), variant):
        return True

    return False


def _same_variant(left: str, right: str) -> bool:
    if left == right:
        return True
    try:
        return float(left.removeprefix("HF")) == float(right.removeprefix("HF"))
    except ValueError:
        return False


def _compat_matches_printer(
    compat: list[str], printer_name: str, model_name: str, variant: str
) -> bool:
    """Check direct, model-level, and named-variant compatibility."""
    if printer_name in compat or model_name in compat:
        return True
    variant_prefix = f"{model_name} {variant}".strip()
    return any(item.startswith(variant_prefix) for item in compat)


def _global_filament_templates(
    index: ProfileIndex,
    slicer: SlicerType,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> list[StoredProfile]:
    """Return cross-vendor generic filament library templates for a slicer."""
    if slicer != SlicerType.ORCASLICER:
        return []

    templates: list[StoredProfile] = []
    for profile in index.find_by_type(
        slicer, ProfileType.FILAMENT, "OrcaFilamentLibrary"
    ):
        data = _evaluate_stable(profile, version_guards)
        name = data.get("name", profile.name)
        if not isinstance(name, str) or not name.endswith("@System"):
            continue

        filament_vendor = data.get("filament_vendor")
        if isinstance(filament_vendor, list):
            filament_vendor = filament_vendor[0] if filament_vendor else ""

        if filament_vendor != "Generic" and not name.lower().startswith("generic"):
            continue

        compat = data.get("compatible_printers")
        if compat not in (None, [], ""):
            continue
        templates.append(profile)
    return templates


def _add_filament_output(
    *,
    compatible_filaments: dict[str, dict[str, dict]],
    profile: StoredProfile,
    profile_data: dict[str, Any],
    filament_name: str,
    filament_type: str,
    model_name: str,
    variant: str,
    slicer_val: str,
    generic_profiles: dict[str, list[tuple[str, str, str]]],
    ofd_index: Any | None,
    role_name: str | None = None,
    role_payload: dict[str, Any] | None = None,
) -> dict | None:
    """Add one filament profile to the mapper output, merging variants."""
    output_name = role_name or filament_name
    payload = role_payload or _profile_payload(profile, profile_data)
    filament_db_id = None
    if ofd_index:
        filament_db_id = ofd_index.resolve_path(
            profile.vendor,
            filament_type,
            filament_name,
            slicer_val,
            filament_id=profile.filament_id,
        )

    is_generic_name = filament_name.lower().startswith("generic")
    if ofd_index and not filament_db_id and not is_generic_name:
        return

    entries_by_payload = compatible_filaments.setdefault(output_name, {})
    payload_key = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    )
    existing_entry = entries_by_payload.get(payload_key)

    if existing_entry is None:
        entry = {
            "name": output_name,
            "compatible_printers": {model_name: [variant]},
            **payload,
        }
        if filament_db_id:
            entry["filament_db_ids"] = [filament_db_id]
        gid = resolve_generic_id(
            generic_profiles.get(slicer_val, []),
            filament_type,
            filament_name,
        )
        if gid:
            entry["generic_id"] = gid
        entries_by_payload[payload_key] = entry
        return entry

    cp = existing_entry["compatible_printers"]
    if model_name not in cp:
        cp[model_name] = []
    if variant not in cp[model_name]:
        cp[model_name].append(variant)
    if filament_db_id and filament_db_id not in existing_entry.get(
        "filament_db_ids", []
    ):
        existing_entry.setdefault("filament_db_ids", []).append(filament_db_id)
    return existing_entry


def map_print_profiles(
    store: ProfileStore,
    index: ProfileIndex,
    model_map: ModelMap,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> dict[int, dict[str, list[dict]]]:
    """
    For each mapped printer model, find compatible print profiles.

    Returns: {model_id: {slicer: [print_entry, ...]}}
    Each print_entry is {name, compatible_printers: {model_name: [variants]}, data}.
    """
    output: dict[int, dict[str, list[dict]]] = {}

    for model_id, slicer_profiles in model_map.model_to_profiles.items():
        for slicer_val, profile_keys in slicer_profiles.items():
            slicer = SlicerType(slicer_val)

            compatible_prints: dict[str, dict] = {}

            for profile_key in profile_keys:
                vendor, name = profile_key.split("/", 1)
                mm_profile = index.find_by_type(
                    slicer, ProfileType.MACHINE_MODEL, vendor, name
                )
                if not mm_profile:
                    continue
                mm = mm_profile[0]
                mm_data = _evaluate_stable(mm, version_guards)
                if not mm_data:
                    continue
                model_name = _model_display_name(mm, mm_data, name)

                # Get variants
                variants = _model_variants(mm, mm_data)

                variant_lookup = model_map.variant_map.get(slicer_val, {})
                uses_definition_constraints = _uses_definition_quality_constraints(mm)

                # Get all print profiles for this vendor
                print_profiles = index.find_by_type(
                    slicer,
                    ProfileType.PRINT,
                    None if uses_definition_constraints else vendor,
                )

                for variant in variants:
                    lookup = _find_variant_lookup(
                        mm, mm_data, name, variant, variant_lookup
                    )
                    if lookup is None:
                        continue

                    variant_data = lookup["data"]
                    printer_identities = _variant_printer_identities(lookup)
                    printer_name = variant_data.get("name", lookup["name"])
                    if slicer == SlicerType.PRUSASLICER:
                        printer_name = variant_data.get(
                            "printer_settings_id", printer_name
                        )

                    for pp in print_profiles:
                        pp_data = _evaluate_stable(pp, version_guards)
                        if not pp_data:
                            continue
                        print_name = (
                            pp_data.get("name")
                            or pp_data.get("print_settings_id")
                            or pp.name
                        )

                        # Check compatibility
                        compat = pp_data.get("compatible_printers", [])
                        if isinstance(compat, str):
                            compat = [
                                x.strip().strip('"')
                                for x in compat.split(";")
                                if x.strip()
                            ]

                        is_compatible = False
                        if uses_definition_constraints:
                            quality_definition = str(
                                mm.context.get("quality_definition")
                                or mm.context.get("definition")
                                or ""
                            )
                            profile_definition = str(pp.context.get("definition") or "")
                            compatibility = pp.context.get("compatibility")
                            compatible_definitions = (
                                compatibility.get("machine_definition_ids")
                                if isinstance(compatibility, Mapping)
                                else None
                            )
                            compatible_variants = (
                                compatibility.get("variant_names")
                                if isinstance(compatibility, Mapping)
                                else None
                            )
                            if not isinstance(compatible_definitions, list):
                                compatible_definitions = [profile_definition]
                            if not isinstance(compatible_variants, list):
                                required_variant = pp.context.get("variant_name")
                                compatible_variants = (
                                    [required_variant] if required_variant else []
                                )
                            selected_variant = lookup.get("context", {}).get(
                                "variant_name"
                            )
                            is_compatible = (
                                quality_definition in compatible_definitions
                                and (
                                    not compatible_variants
                                    or selected_variant in compatible_variants
                                )
                            )
                        elif printer_identities.intersection(
                            compat
                        ) or _compat_matches_printer(
                            compat, printer_name, model_name, variant
                        ):
                            is_compatible = True
                        else:
                            condition = pp_data.get("compatible_printers_condition")
                            if condition:
                                is_compatible = evaluate_printer_condition(
                                    condition, variant_data, slicer_val
                                )

                        if not is_compatible:
                            continue

                        if print_name in compatible_prints:
                            out = compatible_prints[print_name]
                        else:
                            out = compatible_prints[print_name] = {
                                "name": print_name,
                                "compatible_printers": {},
                                **_profile_payload(pp, pp_data),
                            }

                        if model_name not in out["compatible_printers"]:
                            out["compatible_printers"][model_name] = []
                        if variant not in out["compatible_printers"][model_name]:
                            out["compatible_printers"][model_name].append(variant)

            if compatible_prints:
                output.setdefault(model_id, {})[slicer_val] = list(
                    compatible_prints.values()
                )

    return output


def export_output(
    model_map: ModelMap,
    filament_map: dict[int, dict[str, list[dict]]],
    print_map: dict[int, dict[str, list[dict]]],
    store: ProfileStore,
    index: ProfileIndex,
    output_dir: Path,
    ofd_index: Any | None = None,
    version_guards: Mapping[SlicerType, str] | None = None,
    required_slicers: Sequence[SlicerType] | None = None,
) -> None:
    """
    Write the mapped profile data to the output directory.

    Structure:
        output_dir/models/{printer_id}/{slicer}/
            machine_profiles.json  (resource filenames; resolved via resources.json)
            filament_profiles.json
            print_profiles.json
        output_dir/brands/{slicer}/{vendor}/
            generic_filament_profiles.json
        output_dir/resources.json  (sha256 → repo-relative path manifest)
        output_dir/profile_map_out.json
        output_dir/import_manifest.json  (authoritative profile artifacts + hashes)
    """
    models_dir = output_dir / "models"
    brands_dir = output_dir / "brands"

    # Clean previous output
    if output_dir.exists():
        shutil.rmtree(output_dir)

    # --- Machine profiles + assets ---
    for model_id, slicer_profiles in model_map.model_to_profiles.items():
        for slicer_val, profile_keys in slicer_profiles.items():
            slicer = SlicerType(slicer_val)
            slicer_path = models_dir / str(model_id) / slicer_val
            slicer_path.mkdir(parents=True, exist_ok=True)

            machine_profiles_data: list[dict] = []

            for profile_key in profile_keys:
                vendor, name = profile_key.split("/", 1)
                mm_profiles = index.find_by_type(
                    slicer, ProfileType.MACHINE_MODEL, vendor, name
                )
                if not mm_profiles:
                    continue
                mm = mm_profiles[0]
                mm_data = _evaluate_stable(mm, version_guards)
                if not mm_data:
                    continue
                # Keep /out small: resource files live under
                # profiles/{slicer}/_resources and are resolved by the
                # ecosystem importer using resources.json.
                _canonicalize_resource_refs(mm_data, store, slicer)
                machine_model_export = _machine_model_export(mm, mm_data)
                selection_defaults = _profile_selection_defaults(mm)
                # Discovery metadata is required by the legacy ecosystem
                # importer, but need not be persisted in a runtime variant's
                # engine settings.  Parsers can provide it as role context.
                sub_data: dict[str, Any] = {
                    "vendor": vendor,
                    # The ecosystem importer stores this discovery record
                    # directly and expects its established flat shape.  The
                    # selected machine variant below is the runtime role and
                    # carries Cura's data/context/setting_scopes wrapper.
                    "machine_model": machine_model_export,
                }

                # Build variants
                model_name_key = _model_display_name(mm, mm_data, name)
                variants = _model_variants(mm, mm_data)

                variant_lookup = model_map.variant_map.get(slicer_val, {})
                sub_data["variants"] = {}

                for variant in variants:
                    lookup = _find_variant_lookup(
                        mm, mm_data, model_name_key, variant, variant_lookup
                    )
                    if lookup is not None:
                        sub_data["variants"][variant] = _public_variant_payload(
                            lookup,
                            selection_defaults,
                            variant,
                        )

                machine_profiles_data.append(sub_data)

            _write_json(slicer_path / "machine_profiles.json", machine_profiles_data)

            # Write filament profiles
            fil_data = filament_map.get(model_id, {}).get(slicer_val)
            if fil_data:
                _write_json(slicer_path / "filament_profiles.json", fil_data)

            # Write print profiles
            prt_data = print_map.get(model_id, {}).get(slicer_val)
            if prt_data:
                _write_json(slicer_path / "print_profiles.json", prt_data)

    # --- Generic filament profiles per brand ---
    _export_generic_filaments(
        store, index, model_map, brands_dir, ofd_index, version_guards
    )

    # --- Top-level profile map ---
    sorted_map = dict(sorted(model_map.model_to_profiles.items()))
    _write_json(output_dir / "profile_map_out.json", sorted_map)

    # --- OFD filament map (pre-computed profile_name → OFD path lookup) ---
    if ofd_index is not None:
        filament_map_data = ofd_index.build_filament_map()
        _write_json(output_dir / "ofd_filament_map.json", filament_map_data)
        logger.info(
            "Wrote ofd_filament_map.json with %d slicers", len(filament_map_data)
        )

    # --- Resource manifest for SHA-256 resolution ---
    _write_resource_manifest(store, output_dir)

    # --- Authoritative importer artifact manifest ---
    _write_import_manifest(output_dir, required_slicers)


def _canonicalize_resource_refs(
    data: dict[str, Any], store: ProfileStore, slicer: SlicerType
) -> None:
    """Ensure /out resource references are content-addressed sha256 refs."""
    resource_store_dir = store.root / slicer.value / "_resources"
    if not resource_store_dir.exists():
        referenced = [
            key
            for key in ("bed_model", "bed_texture", "thumbnail", "hotend_model")
            if isinstance(data.get(key), str) and data[key]
        ]
        if referenced:
            raise FileNotFoundError(
                f"{slicer.value} resource store is missing for references: "
                + ", ".join(referenced)
            )
        return
    rs = ResourceStore(resource_store_dir)

    for key in ("bed_model", "bed_texture", "thumbnail", "hotend_model"):
        value = data.get(key)
        if not isinstance(value, str) or not value:
            continue
        if value.startswith("sha256:"):
            if rs.get_path(value[7:]):
                continue
            data.pop(key, None)
            continue

        hashes = rs.find_hashes_by_filename(value)
        if hashes:
            data[key] = f"sha256:{hashes[0]}"
        else:
            # Do not emit resource references the importer cannot resolve from
            # the cloned profiles repository.
            data.pop(key, None)


def _export_generic_filaments(
    store: ProfileStore,
    index: ProfileIndex,
    model_map: ModelMap,
    brands_dir: Path,
    ofd_index: Any | None = None,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> None:
    """Export non-model-specific filament profiles per vendor."""
    # Collect vendors seen per slicer
    vendors_per_slicer: dict[str, set[str]] = {}
    for slicer_profiles in model_map.model_to_profiles.values():
        for slicer_val, profile_keys in slicer_profiles.items():
            for pk in profile_keys:
                vendor = pk.split("/", 1)[0]
                vendors_per_slicer.setdefault(slicer_val, set()).add(vendor)

    model_counts = _build_machine_profile_counts(index, version_guards)

    for slicer_val, vendors in vendors_per_slicer.items():
        slicer = SlicerType(slicer_val)
        _export_global_generic_filaments(
            index, slicer, brands_dir, ofd_index, version_guards
        )

        for vendor in vendors:
            filament_profiles = index.find_by_type(slicer, ProfileType.FILAMENT, vendor)
            if not filament_profiles:
                continue

            generic_data = []
            for fp in filament_profiles:
                fp_data = _evaluate_stable(fp, version_guards)
                if not fp_data:
                    continue
                if not is_profile_model_specific(slicer, vendor, fp, model_counts):
                    name = fp_data.get("name") or fp_data.get("filament_settings_id")
                    if name:
                        filament_type = fp_data.get("filament_type", "")
                        if isinstance(filament_type, list):
                            filament_type = filament_type[0] if filament_type else ""
                        # Use OFD path when available
                        filament_db_id = None
                        if ofd_index:
                            filament_db_id = ofd_index.resolve_path(
                                fp.vendor,
                                filament_type,
                                name,
                                slicer_val,
                                filament_id=fp.filament_id,
                            )
                        # Skip non-generic profiles without OFD linkage
                        is_generic_name = name.lower().startswith("generic")
                        if ofd_index and not filament_db_id and not is_generic_name:
                            continue

                        entry = {
                            "name": name,
                            **_profile_payload(fp, fp_data),
                        }
                        if filament_db_id:
                            entry["filament_db_ids"] = [filament_db_id]
                        generic_data.append(entry)

            if generic_data:
                out_path = brands_dir / slicer_val / vendor
                out_path.mkdir(parents=True, exist_ok=True)
                _write_json(out_path / "generic_filament_profiles.json", generic_data)


def _build_machine_profile_counts(
    index: ProfileIndex,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> dict[str, dict[str, int]]:
    """Count concrete machine/variant profiles per slicer vendor.

    Bambu/Orca-style filament profile compatibility lists concrete printer
    profiles (usually one per nozzle), so model-specific detection must compare
    against machine profile counts rather than machine_model counts.
    """
    counts: dict[str, dict[str, int]] = {}
    for slicer in SlicerType:
        for profile in index.find_by_type(slicer, ProfileType.MACHINE):
            if not _evaluate_stable(profile, version_guards):
                continue
            counts.setdefault(slicer.value, {}).setdefault(profile.vendor, 0)
            counts[slicer.value][profile.vendor] += 1
    return counts


def _export_global_generic_filaments(
    index: ProfileIndex,
    slicer: SlicerType,
    brands_dir: Path,
    ofd_index: Any | None = None,
    version_guards: Mapping[SlicerType, str] | None = None,
) -> None:
    """Export slicer-wide generic filament library profiles.

    This restores the legacy ``out/brands/{slicer}/generic_filament_profiles.json``
    file consumed by the ecosystem importer.
    """
    generic_data: dict[str, dict[str, Any]] = {}
    for fp in index.find_by_type(slicer, ProfileType.FILAMENT):
        fp_data = _evaluate_stable(fp, version_guards)
        if not fp_data:
            continue
        name = fp_data.get("name") or fp_data.get("filament_settings_id") or fp.name
        if not isinstance(name, str) or not name:
            continue
        if not name.lower().startswith("generic") or " @" in name:
            continue

        filament_vendor = fp_data.get("filament_vendor")
        if isinstance(filament_vendor, list):
            filament_vendor = filament_vendor[0] if filament_vendor else ""
        if filament_vendor != "Generic":
            continue

        entry = {"name": name, "data": fp_data}
        if ofd_index:
            filament_type = fp_data.get("filament_type", "")
            if isinstance(filament_type, list):
                filament_type = filament_type[0] if filament_type else ""
            filament_db_id = ofd_index.resolve_path(
                fp.vendor,
                filament_type,
                name,
                slicer.value,
                filament_id=fp.filament_id,
            )
            if filament_db_id:
                entry["filament_db_ids"] = [filament_db_id]

        generic_data[name] = entry

    if generic_data:
        _write_json(
            brands_dir / slicer.value / "generic_filament_profiles.json",
            list(generic_data.values()),
        )


def _all_vendors(index: ProfileIndex, slicer: SlicerType) -> set[str]:
    """Return the set of all vendors that have filament profiles for a slicer."""
    vendors: set[str] = set()
    for p in index.find_by_type(slicer, ProfileType.FILAMENT):
        vendors.add(p.vendor)
    return vendors


def _write_json(path: Path, data: Any) -> None:
    """Write JSON with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=4, ensure_ascii=False, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _import_artifact_engine(relative_path: Path) -> str | None:
    """Resolve an import artifact's engine from the generic output layout."""
    parts = relative_path.parts
    if relative_path.name not in _IMPORT_ARTIFACT_FILENAMES:
        return None
    if parts[0:1] == ("models",) and len(parts) == 4:
        return parts[2]
    if (
        parts[0:1] == ("brands",)
        and len(parts) >= 3
        and relative_path.name == "generic_filament_profiles.json"
    ):
        return parts[1]
    return None


def _sha256_file(path: Path) -> str:
    """Return a file's lowercase SHA-256 digest without loading it all at once."""
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_import_manifest(
    output_dir: Path,
    required_slicers: Sequence[SlicerType] | None = None,
) -> None:
    """Describe every generated profile artifact consumed by the importer.

    The completed output tree is the source of truth.  This keeps the contract
    independent of slicer families and ensures any otherwise-stale profile
    artifact remains declared until it is removed from the authoritative output.
    """
    artifacts_by_engine: dict[str, dict[str, str]] = {}
    for path in sorted(output_dir.rglob("*.json")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(output_dir)
        engine = _import_artifact_engine(relative_path)
        if engine is None:
            continue
        artifacts_by_engine.setdefault(engine, {})[
            relative_path.as_posix()
        ] = _sha256_file(path)

    missing_slicers = sorted(
        slicer.value
        for slicer in (required_slicers or ())
        if not artifacts_by_engine.get(slicer.value)
    )
    if missing_slicers:
        raise RuntimeError(
            "Refusing to publish an incomplete import manifest; no profile "
            f"artifacts were generated for: {', '.join(missing_slicers)}"
        )

    manifest = {
        "schema_version": 1,
        "engines": {
            engine: {"artifacts": dict(sorted(artifacts.items()))}
            for engine, artifacts in sorted(artifacts_by_engine.items())
        },
    }
    _write_json(output_dir / "import_manifest.json", manifest)
    logger.info(
        "Wrote import_manifest.json with %d artifacts across %d engines",
        sum(len(artifacts) for artifacts in artifacts_by_engine.values()),
        len(artifacts_by_engine),
    )


def _write_resource_manifest(store: ProfileStore, output_dir: Path) -> None:
    """Write a manifest for resolving sha256 resource refs from /out.

    Shape:
        {"sha256:{hash}": {path, filename, size, type}}

    The path is repo-relative, so the ecosystem importer can resolve assets from
    the cloned profiles repository without duplicating them under /out.
    """
    manifest: dict[str, dict[str, Any]] = {}

    for slicer in _MAPPING_SLICERS:
        resource_dir = store.root / slicer.value / "_resources"
        if not resource_dir.exists():
            continue
        rs = ResourceStore(resource_dir)
        for hash_hex, meta in rs._manifest.items():
            ref_key = f"sha256:{hash_hex}"
            if ref_key in manifest:
                continue
            suffix = f".{meta['type']}" if meta.get("type") else ""
            manifest[ref_key] = {
                "path": f"profiles/{slicer.value}/_resources/{hash_hex}{suffix}",
                "filename": meta.get("filename", ""),
                "source_path": meta.get("source_path", ""),
                "size": meta.get("size", 0),
                "type": meta.get("type", ""),
            }

    _write_json(output_dir / "resources.json", manifest)
    logger.info("Wrote resources.json with %d resources", len(manifest))


def run_mapping_pipeline(
    store: ProfileStore,
    output_dir: Path,
    slicers: list[SlicerType] | None = None,
    ofd_path: Path | None = None,
) -> ModelMap:
    """
    Run the complete mapping pipeline: fetch SP data → match models →
    map filaments → map print profiles → export.

    This is the single function that replaces the old 5-script workflow.

    Args:
        store: ProfileStore with ingested profiles.
        output_dir: Where to write the output.
        slicers: Which slicers to process (default: all supported).
        ofd_path: Optional path to OFD repo data/ dir. When provided,
            filament_db_ids are resolved to OFD filesystem paths.

    Returns:
        The ModelMap for inspection/logging.
    """
    # Build index
    index = ProfileIndex(store)
    target_slicers = slicers or _MAPPING_SLICERS
    index.build(target_slicers)

    # Build OFD index if path provided
    ofd_index = None
    if ofd_path is not None:
        from .ofd import OFDRepo, OFDFilamentIndex

        logger.info("Loading OFD data from %s ...", ofd_path)
        ofd_repo = OFDRepo(ofd_path)
        ofd_index = OFDFilamentIndex(ofd_repo)

    # Fetch SimplyPrint model data
    logger.info("Fetching SimplyPrint model data...")
    sp_data = fetch_sp_model_data()
    logger.info("Fetching SimplyPrint slicer versions...")
    version_guards = fetch_sp_slicer_versions()
    logger.info(
        "Using SimplyPrint newest-version guards: %s",
        ", ".join(
            f"{slicer.value}={version}"
            for slicer, version in sorted(version_guards.items(), key=lambda item: item[0].value)
        ),
    )

    # Step 1: Map printer models
    logger.info("Mapping printer models...")
    model_map = map_printer_models(
        store, index, sp_data, target_slicers, version_guards
    )
    logger.info(
        "Mapped %d SimplyPrint models. Failed brands: %d, Failed models: %d",
        len(model_map.model_to_profiles),
        len(model_map.failed_brands),
        len(model_map.failed_models),
    )

    # Step 2: Map filament profiles
    logger.info("Mapping filament profiles...")
    filament_map = map_filament_profiles(
        store, index, model_map, ofd_index, version_guards
    )

    # Step 3: Map print profiles
    logger.info("Mapping print profiles...")
    print_map = map_print_profiles(store, index, model_map, version_guards)

    # Step 4: Export
    logger.info("Exporting to %s ...", output_dir)
    export_output(
        model_map,
        filament_map,
        print_map,
        store,
        index,
        output_dir,
        ofd_index,
        version_guards,
        target_slicers,
    )

    return model_map
