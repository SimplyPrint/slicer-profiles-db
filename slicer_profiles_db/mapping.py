"""
Full mapping pipeline: printer models → filament profiles → print profiles → export.

Maps slicer profiles to SimplyPrint printer model IDs, resolves filament
and print profile compatibility, and exports the results.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
from .resources import ResourceStore
from .store import ProfileStore
from .versions import version_key

logger = logging.getLogger(__name__)


def _get_sp_api_url() -> str:
    """Get the SimplyPrint API URL from the SP_API_URL environment variable."""
    url = os.environ.get("SP_API_URL")
    if not url:
        raise RuntimeError(
            "SP_API_URL environment variable is not set. "
            "Set it to the full SimplyPrint printer model endpoint URL."
        )
    return url


# Slicers that participate in model mapping.
_MAPPING_SLICERS = [
    SlicerType.PRUSASLICER,
    SlicerType.ORCASLICER,
    SlicerType.BAMBUSTUDIO,
    SlicerType.CREALITYPRINT,
    SlicerType.ELEGOOSLICER,
    SlicerType.ANYCUBICSLICER,
    SlicerType.SUPERSLICER,
    SlicerType.CURA,
]


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


def _evaluate_stable(profile: StoredProfile) -> dict[str, Any]:
    """Evaluate a profile at its latest stable (non-nightly) version."""
    return profile.evaluate(_stable_version(profile))


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


def _prepare_sp_data(
    raw: dict[str, Any],
) -> tuple[list[str], list[dict], dict[int, list[str]]]:
    """Normalise the raw SP API response into lookup-friendly structures.

    Returns (sp_brands, sp_models, sp_slicer_names).
    """
    sp_brands = [b.lower() for b in raw["brands"]]
    sp_models = raw["models"]
    sp_slicer_names: dict[int, list[str]] = {}

    for model in sp_models:
        model["brand"] = model["brand"].lower()
        model["name"] = model["name"].lower()
        if model.get("slicerProfileNames"):
            sp_slicer_names[model["id"]] = [
                n.lower() for n in model["slicerProfileNames"]
            ]

    return sp_brands, sp_models, sp_slicer_names


def map_printer_models(
    store: ProfileStore,
    index: ProfileIndex,
    sp_data: dict[str, Any],
    slicers: list[SlicerType] | None = None,
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
            name = profile.get_latest("name") or profile.name
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
        _build_variant_map(store, index, slicer, result)

    return result


def _build_variant_map(
    store: ProfileStore,
    index: ProfileIndex,
    slicer: SlicerType,
    result: ModelMap,
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
        data = _evaluate_stable(profile)

        printer_model = data.get("printer_model")
        if not printer_model:
            continue

        # Determine the variant identifier
        variant = data.get("printer_variant")
        if variant is None:
            nd = data.get("nozzle_diameter")
            if isinstance(nd, list) and nd:
                variant = str(nd[0])
            elif isinstance(nd, str):
                variant = nd.split(";")[0].strip() if ";" in nd else nd

        name_variant = _parse_variant_from_name(data.get("name", profile.name))
        if variant is None:
            variant = name_variant
        elif name_variant and not _same_variant(str(variant), name_variant):
            # Some upstream profiles have a stale printer_variant but the
            # display name/nozzle is correct (for example RatRig V-Minion 0.6).
            variant = name_variant

        if variant is None:
            continue

        ptype = data.get("type", "machine")
        if ptype not in ("machine", None):
            continue

        lookup_key = printer_model + variant
        profile_name = data.get("name", profile.name)

        candidate = {
            "name": profile_name,
            "data": data,
        }
        existing = result.variant_map[slicer_val].get(lookup_key)
        if existing is None or _variant_candidate_is_better(
            lookup_key, candidate, existing
        ):
            result.variant_map[slicer_val][lookup_key] = candidate

        # Keep an exact display-name index as a collision fallback. Some
        # upstream variants have wrong printer_model / printer_variant values,
        # but their display names are still correct.
        result.variant_map[slicer_val].setdefault(profile_name, candidate)

        # Also index by model_id + variant (Orca/BBS use model_id)
        model_id = data.get("model_id")
        if model_id and model_id != printer_model:
            alt_key = model_id + variant
            result.variant_map[slicer_val].setdefault(
                alt_key,
                {
                    "name": profile_name,
                    "data": data,
                },
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
    variant = candidate.get("data", {}).get("printer_variant")
    if isinstance(variant, str) and lookup_key.endswith(variant):
        model_part = lookup_key[: -len(variant)]
    model_part = model_part.strip().lower()

    candidate_matches = bool(model_part and model_part in candidate_name)
    existing_matches = bool(model_part and model_part in existing_name)
    return candidate_matches and not existing_matches


def map_filament_profiles(
    store: ProfileStore,
    index: ProfileIndex,
    model_map: ModelMap,
    ofd_index: Any | None = None,
) -> dict[int, dict[str, list[dict]]]:
    """
    For each mapped printer model, find compatible filament profiles.

    Returns: {model_id: {slicer: [filament_entry, ...]}}
    Each filament_entry is {name, compatible_printers: {model_name: [variants]}, data}.
    """
    output: dict[int, dict[str, list[dict]]] = {}

    # Build generic ID lookup per slicer (for resolve_generic_id).
    active_slicers = set()
    for slicer_profiles in model_map.model_to_profiles.values():
        active_slicers.update(slicer_profiles.keys())
    _generic_profiles = build_generic_profile_index(
        index, [SlicerType(s) for s in active_slicers]
    )
    _global_templates = {
        slicer: _global_filament_templates(index, SlicerType(slicer))
        for slicer in active_slicers
    }

    for model_id, slicer_profiles in model_map.model_to_profiles.items():
        for slicer_val, profile_keys in slicer_profiles.items():
            slicer = SlicerType(slicer_val)

            # Gather machine_model profile data and build variant lists
            compatible_filaments: dict[str, list[dict]] = {}

            for profile_key in profile_keys:
                vendor, name = profile_key.split("/", 1)
                mm_profile = index.find_by_type(
                    slicer, ProfileType.MACHINE_MODEL, vendor, name
                )
                if not mm_profile:
                    continue
                mm = mm_profile[0]
                mm_data = _evaluate_stable(mm)
                model_name = mm_data.get("name", name)

                # Get nozzle variants
                nozzle_str = mm_data.get("nozzle_diameter", "")
                if isinstance(nozzle_str, list):
                    nozzle_str = ";".join(str(n) for n in nozzle_str)
                variants_raw = mm_data.get("variants", nozzle_str)
                if isinstance(variants_raw, str):
                    variants = [v.strip() for v in variants_raw.split(";") if v.strip()]
                else:
                    variants = [str(v) for v in variants_raw] if variants_raw else []

                variant_lookup = model_map.variant_map.get(slicer_val, {})

                # For each variant, find compatible filament profiles
                for variant in variants:
                    lookup = _find_variant_lookup(
                        mm_data, name, variant, variant_lookup
                    )
                    if lookup is None:
                        continue

                    variant_data = lookup["data"]
                    printer_name = variant_data.get("name", lookup["name"])

                    # Find all filament profiles for this vendor
                    filament_profiles = index.find_by_type(
                        slicer, ProfileType.FILAMENT, vendor
                    )
                    for fp in filament_profiles:
                        fp_data = _evaluate_stable(fp)
                        filament_name = fp_data.get("name", fp.name)
                        filament_type = fp_data.get("filament_type", "")
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
                        if _compat_matches_printer(
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

                        _add_filament_output(
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

                    # Shared Orca library generic @System filament presets are
                    # material presets, not printer-vendor presets. Brand-specific
                    # @System presets must not be attached globally; otherwise
                    # every printer gets unrelated filament brands like AliZ/NIT.
                    for fp in _global_templates.get(slicer_val, []):
                        fp_data = _evaluate_stable(fp)
                        filament_name = fp_data.get("name", fp.name)
                        filament_type = fp_data.get("filament_type", "")
                        if isinstance(filament_type, list):
                            filament_type = filament_type[0] if filament_type else ""
                        _add_filament_output(
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

            # Flatten into output
            if compatible_filaments:
                flat = []
                for entries in compatible_filaments.values():
                    flat.extend(entries)
                output.setdefault(model_id, {})[slicer_val] = flat

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
    item_variant = data.get("printer_variant")
    if isinstance(item_variant, str) and _same_variant(item_variant, variant):
        return True

    nozzle = data.get("nozzle_diameter")
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


def _find_variant_lookup(
    mm_data: dict[str, Any],
    fallback_name: str,
    variant: str,
    variant_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Find the concrete machine profile for a machine_model variant.

    Prusa machine models list human-facing variants, while concrete machine
    profiles are keyed by internal ``printer_model`` values such as ``MK30.4``.
    Orca/Bambu also use ``model_id`` in some places, and some vendors only match
    by display-name patterns.
    """
    model_name = str(mm_data.get("name") or fallback_name)

    lookup_keys = [model_name + variant]
    for key_name in ("model_id", "printer_model"):
        value = mm_data.get(key_name)
        if isinstance(value, str) and value:
            lookup_keys.append(value + variant)

    for lookup_key in lookup_keys:
        lookup = variant_lookup.get(lookup_key)
        if lookup is not None:
            return lookup

    display_model_names = _variant_display_model_names(model_name)
    family_name = mm_data.get("family")
    if isinstance(family_name, str) and family_name:
        display_model_names.extend(_variant_display_model_names(family_name))
        display_model_names = list(dict.fromkeys(display_model_names))

    display_names = []
    for display_model_name in display_model_names:
        display_names.extend(
            [
                f"{display_model_name} {variant} nozzle",
                f"{display_model_name} {variant} Nozzle",
                f"{display_model_name} {variant}mm nozzle",
                f"{display_model_name} {variant}Nozzle",
                f"{display_model_name} ({variant} mm nozzle)",
                f"{display_model_name} ({variant}mm nozzle)",
            ]
        )
        display_names.append(display_model_name)

    display_names_lower = {display_name.lower() for display_name in display_names}
    for item in variant_lookup.values():
        if item["name"].lower() in display_names_lower and _variant_matches_item(
            variant, item
        ):
            return item

    compact_display_names = {
        display_name.replace(" ", "").lower() for display_name in display_names
    }
    compact_prefix_matches = []
    for item in variant_lookup.values():
        compact_item_name = item["name"].replace(" ", "").lower()
        if compact_item_name in compact_display_names and _variant_matches_item(
            variant, item
        ):
            return item
        if any(compact_item_name.startswith(name) for name in compact_display_names):
            if _variant_matches_item(variant, item):
                compact_prefix_matches.append(item)
    compact_prefix_matches = list(
        {item["name"]: item for item in compact_prefix_matches}.values()
    )
    if len(compact_prefix_matches) == 1:
        return compact_prefix_matches[0]
    preferred_prefix_matches = [
        item
        for item in compact_prefix_matches
        if "+m4hotend" in item["name"].replace(" ", "").lower()
    ]
    if len(preferred_prefix_matches) == 1:
        return preferred_prefix_matches[0]

    # Prusa machine models expose the internal printer family (MK3, MINIIS,
    # MK4IS, ...), while concrete printer profiles are keyed by that family plus
    # nozzle variant. Keep this last so MMU/multi-tool display-name matches win
    # over their single-extruder family fallback.
    family = mm_data.get("family")
    if isinstance(family, str) and family:
        return variant_lookup.get(family + variant)

    return None


def _compat_matches_printer(
    compat: list[str], printer_name: str, model_name: str, variant: str
) -> bool:
    """Check direct, model-level, and named-variant compatibility."""
    if printer_name in compat or model_name in compat:
        return True
    variant_prefix = f"{model_name} {variant}".strip()
    return any(item.startswith(variant_prefix) for item in compat)


def _global_filament_templates(
    index: ProfileIndex, slicer: SlicerType
) -> list[StoredProfile]:
    """Return cross-vendor generic filament library templates for a slicer."""
    if slicer != SlicerType.ORCASLICER:
        return []

    templates: list[StoredProfile] = []
    for profile in index.find_by_type(
        slicer, ProfileType.FILAMENT, "OrcaFilamentLibrary"
    ):
        data = _evaluate_stable(profile)
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
    compatible_filaments: dict[str, list[dict]],
    profile: StoredProfile,
    profile_data: dict[str, Any],
    filament_name: str,
    filament_type: str,
    model_name: str,
    variant: str,
    slicer_val: str,
    generic_profiles: dict[str, list[tuple[str, str, str]]],
    ofd_index: Any | None,
) -> None:
    """Add one filament profile to the mapper output, merging variants."""
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

    if filament_name not in compatible_filaments:
        compatible_filaments[filament_name] = []

    existing_entry = None
    for entry in compatible_filaments[filament_name]:
        if entry["data"] == profile_data:
            existing_entry = entry
            break

    if existing_entry is None:
        entry = {
            "name": filament_name,
            "compatible_printers": {model_name: [variant]},
            "data": profile_data,
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
        compatible_filaments[filament_name].append(entry)
        return

    cp = existing_entry["compatible_printers"]
    if model_name not in cp:
        cp[model_name] = []
    if variant not in cp[model_name]:
        cp[model_name].append(variant)
    if filament_db_id and filament_db_id not in existing_entry.get(
        "filament_db_ids", []
    ):
        existing_entry.setdefault("filament_db_ids", []).append(filament_db_id)


def map_print_profiles(
    store: ProfileStore,
    index: ProfileIndex,
    model_map: ModelMap,
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
                mm_data = _evaluate_stable(mm)
                model_name = mm_data.get("name", name)

                # Get variants
                nozzle_str = mm_data.get("nozzle_diameter", "")
                if isinstance(nozzle_str, list):
                    nozzle_str = ";".join(str(n) for n in nozzle_str)
                variants_raw = mm_data.get("variants", nozzle_str)
                if isinstance(variants_raw, str):
                    variants = [v.strip() for v in variants_raw.split(";") if v.strip()]
                else:
                    variants = [str(v) for v in variants_raw] if variants_raw else []

                variant_lookup = model_map.variant_map.get(slicer_val, {})

                # Get all print profiles for this vendor
                print_profiles = index.find_by_type(slicer, ProfileType.PRINT, vendor)

                for variant in variants:
                    lookup = _find_variant_lookup(
                        mm_data, name, variant, variant_lookup
                    )
                    if lookup is None:
                        continue

                    variant_data = lookup["data"]
                    printer_name = variant_data.get("name", lookup["name"])

                    # PrusaSlicer uses printer_settings_id
                    if slicer == SlicerType.PRUSASLICER:
                        ps_id = variant_data.get("printer_settings_id")
                        if ps_id:
                            printer_name = ps_id

                    for pp in print_profiles:
                        pp_data = _evaluate_stable(pp)
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
                        if _compat_matches_printer(
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
                                "data": pp_data,
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
                mm_data = _evaluate_stable(mm)

                sub_data: dict[str, Any] = {"vendor": vendor, "machine_model": mm_data}

                # Keep /out small: resource files live under profiles/{slicer}/_resources
                # and are resolved by the ecosystem importer using resources.json.
                _canonicalize_resource_refs(mm_data, store, slicer)

                # Build variants
                nozzle_str = mm_data.get("nozzle_diameter", "")
                if isinstance(nozzle_str, list):
                    nozzle_str = ";".join(str(n) for n in nozzle_str)
                variants_raw = mm_data.get("variants", nozzle_str)
                if isinstance(variants_raw, str):
                    variants = [v.strip() for v in variants_raw.split(";") if v.strip()]
                else:
                    variants = [str(v) for v in variants_raw] if variants_raw else []

                variant_lookup = model_map.variant_map.get(slicer_val, {})
                sub_data["variants"] = {}

                for variant in variants:
                    lookup = _find_variant_lookup(
                        mm_data, name, variant, variant_lookup
                    )
                    if lookup is not None:
                        sub_data["variants"][variant] = lookup

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
    _export_generic_filaments(store, index, model_map, brands_dir, ofd_index)

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


def _canonicalize_resource_refs(
    data: dict[str, Any], store: ProfileStore, slicer: SlicerType
) -> None:
    """Ensure /out resource references are content-addressed sha256 refs."""
    resource_store_dir = store.root / slicer.value / "_resources"
    if not resource_store_dir.exists():
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
) -> None:
    """Export non-model-specific filament profiles per vendor."""
    # Collect vendors seen per slicer
    vendors_per_slicer: dict[str, set[str]] = {}
    for slicer_profiles in model_map.model_to_profiles.values():
        for slicer_val, profile_keys in slicer_profiles.items():
            for pk in profile_keys:
                vendor = pk.split("/", 1)[0]
                vendors_per_slicer.setdefault(slicer_val, set()).add(vendor)

    model_counts = _build_machine_profile_counts(index)

    for slicer_val, vendors in vendors_per_slicer.items():
        slicer = SlicerType(slicer_val)
        _export_global_generic_filaments(index, slicer, brands_dir, ofd_index)

        for vendor in vendors:
            filament_profiles = index.find_by_type(slicer, ProfileType.FILAMENT, vendor)
            if not filament_profiles:
                continue

            generic_data = []
            for fp in filament_profiles:
                fp_data = _evaluate_stable(fp)
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
                            "data": fp_data,
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
) -> dict[str, dict[str, int]]:
    """Count concrete machine/variant profiles per slicer vendor.

    Bambu/Orca-style filament profile compatibility lists concrete printer
    profiles (usually one per nozzle), so model-specific detection must compare
    against machine profile counts rather than machine_model counts.
    """
    counts: dict[str, dict[str, int]] = {}
    for slicer in SlicerType:
        for profile in index.find_by_type(slicer, ProfileType.MACHINE):
            counts.setdefault(slicer.value, {}).setdefault(profile.vendor, 0)
            counts[slicer.value][profile.vendor] += 1
    return counts


def _export_global_generic_filaments(
    index: ProfileIndex,
    slicer: SlicerType,
    brands_dir: Path,
    ofd_index: Any | None = None,
) -> None:
    """Export slicer-wide generic filament library profiles.

    This restores the legacy ``out/brands/{slicer}/generic_filament_profiles.json``
    file consumed by the ecosystem importer.
    """
    generic_data: dict[str, dict[str, Any]] = {}
    for fp in index.find_by_type(slicer, ProfileType.FILAMENT):
        fp_data = _evaluate_stable(fp)
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

    # Step 1: Map printer models
    logger.info("Mapping printer models...")
    model_map = map_printer_models(store, index, sp_data, target_slicers)
    logger.info(
        "Mapped %d SimplyPrint models. Failed brands: %d, Failed models: %d",
        len(model_map.model_to_profiles),
        len(model_map.failed_brands),
        len(model_map.failed_models),
    )

    # Step 2: Map filament profiles
    logger.info("Mapping filament profiles...")
    filament_map = map_filament_profiles(store, index, model_map, ofd_index)

    # Step 3: Map print profiles
    logger.info("Mapping print profiles...")
    print_map = map_print_profiles(store, index, model_map)

    # Step 4: Export
    logger.info("Exporting to %s ...", output_dir)
    export_output(
        model_map, filament_map, print_map, store, index, output_dir, ofd_index
    )

    return model_map
