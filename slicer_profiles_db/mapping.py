"""
Full mapping pipeline: printer models → filament profiles → print profiles → export.

Maps slicer profiles to SimplyPrint printer model IDs, resolves filament
and print profile compatibility, and exports the results.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

from .brands import BRAND_MAPS, normalize_brand, strip_brand_from_name
from .conditions import evaluate_printer_condition
from .index import (
    ProfileIndex, is_profile_generic, is_profile_model_specific,
    build_generic_profile_index, resolve_generic_id,
)
from .matching import match_printer_model
from .models import ProfileType, SlicerType, StoredProfile
from .resources import ResourceStore, RESOURCE_SETTING_KEYS
from .store import ProfileStore

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
    SlicerType.ELEGOOSLICER,
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
                if best is None or ver > best:
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
                sp_models, sp_brands, sp_slicer_names,
                vendor, name, brand_map,
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

        if variant is None:
            continue

        ptype = data.get("type", "machine")
        if ptype not in ("machine", None):
            continue

        lookup_key = printer_model + variant
        profile_name = data.get("name", profile.name)

        result.variant_map[slicer_val][lookup_key] = {
            "name": profile_name,
            "data": data,
        }

        # Also index by model_id + variant (Orca/BBS use model_id)
        model_id = data.get("model_id")
        if model_id and model_id != printer_model:
            alt_key = model_id + variant
            result.variant_map[slicer_val].setdefault(alt_key, {
                "name": profile_name,
                "data": data,
            })


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

    for model_id, slicer_profiles in model_map.model_to_profiles.items():
        for slicer_val, profile_keys in slicer_profiles.items():
            slicer = SlicerType(slicer_val)

            # Gather machine_model profile data and build variant lists
            compatible_filaments: dict[str, list[dict]] = {}

            for profile_key in profile_keys:
                vendor, name = profile_key.split("/", 1)
                mm_profile = index.find_by_type(slicer, ProfileType.MACHINE_MODEL, vendor, name)
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
                    lookup_key = (mm_data.get("name", name)) + variant
                    lookup = variant_lookup.get(lookup_key)
                    if lookup is None and "model_id" in mm_data:
                        lookup = variant_lookup.get(mm_data["model_id"] + variant)
                    if lookup is None:
                        # Try "<name> <variant> nozzle" pattern
                        nozzle_name = f"{mm_data.get('name', name)} {variant} nozzle"
                        for item in variant_lookup.values():
                            if item["name"] == nozzle_name:
                                lookup = item
                                break
                    if lookup is None:
                        continue

                    variant_data = lookup["data"]
                    printer_name = variant_data.get("name", lookup["name"])

                    # Find all filament profiles for this vendor
                    filament_profiles = index.find_by_type(slicer, ProfileType.FILAMENT, vendor)
                    for fp in filament_profiles:
                        fp_data = _evaluate_stable(fp)
                        filament_name = fp_data.get("name", fp.name)
                        filament_type = fp_data.get("filament_type", "")
                        if isinstance(filament_type, list):
                            filament_type = filament_type[0] if filament_type else ""

                        # Check compatibility
                        compat = fp_data.get("compatible_printers", [])
                        if isinstance(compat, str):
                            compat = [x.strip().strip('"') for x in compat.split(";") if x.strip()]

                        is_compatible = False
                        if printer_name in compat:
                            is_compatible = True
                        else:
                            condition = fp_data.get("compatible_printers_condition")
                            if condition:
                                is_compatible = evaluate_printer_condition(
                                    condition, variant_data, slicer_val
                                )

                        if not is_compatible:
                            continue

                        # Build traceability ID — use OFD path when available
                        filament_db_id = None
                        if ofd_index:
                            filament_db_id = ofd_index.resolve_path(
                                fp.vendor, filament_type, filament_name,
                                slicer_val, filament_id=fp.filament_id,
                            )

                        # Add to output, grouping by filament name
                        if filament_name not in compatible_filaments:
                            compatible_filaments[filament_name] = []

                        # Find or create entry with matching data
                        existing_entry = None
                        for entry in compatible_filaments[filament_name]:
                            if entry["data"] == fp_data:
                                existing_entry = entry
                                break

                        if existing_entry is None:
                            entry = {
                                "name": filament_name,
                                "compatible_printers": {model_name: [variant]},
                                "data": fp_data,
                            }
                            if filament_db_id:
                                entry["filament_db_ids"] = [filament_db_id]
                            # Add generic fallback slicer ID — pick the most
                            # specific generic whose name keywords appear in the
                            # filament name (e.g. "PLA Silk" matches
                            # "Generic PLA Silk" before "Generic PLA").
                            gid = resolve_generic_id(
                                _generic_profiles.get(slicer_val, []),
                                filament_type, filament_name,
                            )
                            if gid:
                                entry["generic_id"] = gid
                            compatible_filaments[filament_name].append(entry)
                        else:
                            cp = existing_entry["compatible_printers"]
                            if model_name not in cp:
                                cp[model_name] = []
                            if variant not in cp[model_name]:
                                cp[model_name].append(variant)
                            if filament_db_id and filament_db_id not in existing_entry.get("filament_db_ids", []):
                                existing_entry.setdefault("filament_db_ids", []).append(filament_db_id)

            # Flatten into output
            if compatible_filaments:
                flat = []
                for entries in compatible_filaments.values():
                    flat.extend(entries)
                output.setdefault(model_id, {})[slicer_val] = flat

    return output


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
                mm_profile = index.find_by_type(slicer, ProfileType.MACHINE_MODEL, vendor, name)
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
                    lookup_key = (mm_data.get("name", name)) + variant
                    lookup = variant_lookup.get(lookup_key)
                    if lookup is None and "model_id" in mm_data:
                        lookup = variant_lookup.get(mm_data["model_id"] + variant)
                    if lookup is None:
                        nozzle_name = f"{mm_data.get('name', name)} {variant} nozzle"
                        for item in variant_lookup.values():
                            if item["name"] == nozzle_name:
                                lookup = item
                                break
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
                        print_name = pp_data.get("name") or pp_data.get("print_settings_id") or pp.name

                        # Check compatibility
                        compat = pp_data.get("compatible_printers", [])
                        if isinstance(compat, str):
                            compat = [x.strip().strip('"') for x in compat.split(";") if x.strip()]

                        is_compatible = False
                        if printer_name in compat:
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
                output.setdefault(model_id, {})[slicer_val] = list(compatible_prints.values())

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
            machine_profiles.json  (sha256: refs for resources)
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
                mm_profiles = index.find_by_type(slicer, ProfileType.MACHINE_MODEL, vendor, name)
                if not mm_profiles:
                    continue
                mm = mm_profiles[0]
                mm_data = _evaluate_stable(mm)

                sub_data: dict[str, Any] = {"vendor": vendor, "machine_model": mm_data}

                # Resolve resource assets and copy to output
                _copy_assets(mm_data, store, slicer, slicer_path)

                # Build variants
                model_name_key = mm_data.get("name", name)
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
                    lookup_key = model_name_key + variant
                    lookup = variant_lookup.get(lookup_key)
                    if lookup is None and "model_id" in mm_data:
                        lookup = variant_lookup.get(mm_data["model_id"] + variant)
                    if lookup is None:
                        nozzle_name = f"{model_name_key} {variant} nozzle"
                        for item in variant_lookup.values():
                            if item["name"] == nozzle_name:
                                lookup = item
                                break
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
        logger.info("Wrote ofd_filament_map.json with %d slicers",
                     len(filament_map_data))

    # --- Resource manifest for SHA-256 resolution ---
    _write_resource_manifest(store, output_dir)


def _copy_assets(
    mm_data: dict, store: ProfileStore, slicer: SlicerType, dest_dir: Path
) -> None:
    """Inject cover/thumbnail sha256 refs for name-pattern resources.

    Resource setting keys (bed_model, bed_texture, etc.) keep their
    sha256: references — no files are copied to dest_dir.
    """
    resource_store_dir = store.root / slicer.value / "_resources"
    if not resource_store_dir.exists():
        return
    rs = ResourceStore(resource_store_dir)

    # Cover/thumbnail images discovered by name pattern
    name = mm_data.get("name", "")
    for suffix in ("_cover.png", "_thumbnail.png"):
        ref_name = f"{name}{suffix}"
        for hash_hex, meta in rs._manifest.items():
            if meta.get("filename") == ref_name:
                mm_data[suffix.replace(".", "_").lstrip("_")] = f"sha256:{hash_hex}"
                break


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

    for slicer_val, vendors in vendors_per_slicer.items():
        slicer = SlicerType(slicer_val)
        for vendor in vendors:
            filament_profiles = index.find_by_type(slicer, ProfileType.FILAMENT, vendor)
            if not filament_profiles:
                continue

            generic_data = []
            for fp in filament_profiles:
                fp_data = _evaluate_stable(fp)
                if not is_profile_model_specific(slicer, vendor, fp):
                    name = fp_data.get("name") or fp_data.get("filament_settings_id")
                    if name:
                        filament_type = fp_data.get("filament_type", "")
                        if isinstance(filament_type, list):
                            filament_type = filament_type[0] if filament_type else ""
                        # Use OFD path when available
                        filament_db_id = None
                        if ofd_index:
                            filament_db_id = ofd_index.resolve_path(
                                fp.vendor, filament_type, name,
                                slicer_val, filament_id=fp.filament_id,
                            )
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
                _write_json(
                    out_path / "generic_filament_profiles.json", generic_data
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
        json.dumps(data, indent=4, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def _write_resource_manifest(store: ProfileStore, output_dir: Path) -> None:
    """Write a unified resource manifest mapping sha256 refs to repo-relative paths.

    Generates ``out/resources.json`` so downstream consumers (e.g. PHP importer)
    can resolve ``sha256:{hash}`` refs to the actual file in the profiles repo
    without requiring resource files to be copied into the output directory.
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
                continue  # already recorded from another slicer
            suffix = f".{meta['type']}" if meta.get("type") else ""
            rel_path = f"profiles/{slicer.value}/_resources/{hash_hex}{suffix}"
            manifest[ref_key] = {
                "path": rel_path,
                "filename": meta.get("filename", ""),
                "size": meta.get("size", 0),
                "type": meta.get("type", ""),
            }

    _write_json(output_dir / "resources.json", manifest)
    logger.info("Wrote resources.json with %d entries", len(manifest))


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
    export_output(model_map, filament_map, print_map, store, index, output_dir, ofd_index)

    return model_map
