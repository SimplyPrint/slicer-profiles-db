"""
SlicerMapper: derives slicer_settings/slicer_ids mappings from profile store data.

Ported from ofd.slicer_mapper.mapper — this is the canonical implementation.
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from slicer_profiles_db import ProfileIndex, SlicerType, StoredProfile
from slicer_profiles_db.index import build_generic_profile_index, resolve_generic_id
from .vendor_map import get_profile_prefixes

logger = logging.getLogger(__name__)


@dataclass
class MappingResult:
    filament_path: Path  # path to filament.json
    slicer: str
    profile_name: str  # base name (before @)
    slicer_id: str | None  # filament_id from StoredProfile
    generic_id: str | None  # fallback generic profile ID (e.g. GFL99)
    vendor: str  # profile vendor


@dataclass
class MappingConflict:
    filament_path: Path
    slicer: str
    field: str  # "profile_name" or "slicer_id"
    existing: str
    derived: str


@dataclass
class MappingReport:
    updated: list[MappingResult] = field(default_factory=list)
    already_correct: list[MappingResult] = field(default_factory=list)
    conflicts: list[MappingConflict] = field(default_factory=list)
    skipped: list[tuple[Path, str]] = field(default_factory=list)


def _is_proper_id(value: str) -> bool:
    """Return True if value looks like a slicer-native ID (e.g. 'GFB00'), not a profile name."""
    return bool(value) and " @" not in value and " " not in value


def _best_slicer_id(profiles: list[StoredProfile]) -> str | None:
    """Pick the best slicer ID from a list of profiles.

    Prefers a proper filament_id (short code like 'GFB00').
    Falls back to setting_id if all filament_ids are profile names.
    """
    # First pass: proper filament_id
    for p in profiles:
        if p.filament_id and _is_proper_id(p.filament_id):
            return p.filament_id
    # Second pass: setting_id
    for p in profiles:
        if p.setting_id and _is_proper_id(p.setting_id):
            return p.setting_id
    return None


class SlicerMapper:
    def __init__(self, index: ProfileIndex, data_dir: Path):
        self.index = index
        self.data_dir = data_dir

    def run(
        self,
        slicers: list[str] | None = None,
        dry_run: bool = False,
        brand_filter: str | None = None,
    ) -> MappingReport:
        if slicers is None:
            slicers = [s.value for s in SlicerType]

        # Build generic profile index for generic_id resolution
        generic_profiles = build_generic_profile_index(
            self.index, [SlicerType(s) for s in slicers]
        )

        report = MappingReport()

        for brand_dir in sorted(self.data_dir.iterdir()):
            if not brand_dir.is_dir():
                continue

            brand_id = brand_dir.name
            if brand_filter and brand_id != brand_filter:
                continue

            brand_json = brand_dir / "brand.json"
            if not brand_json.exists():
                continue

            brand_data = json.loads(brand_json.read_text(encoding="utf-8"))
            brand_name = brand_data.get("name", "")

            for material_dir in sorted(brand_dir.iterdir()):
                if not material_dir.is_dir():
                    continue

                material = material_dir.name

                for filament_dir in sorted(material_dir.iterdir()):
                    if not filament_dir.is_dir():
                        continue

                    filament_path = filament_dir / "filament.json"
                    if not filament_path.exists():
                        continue

                    filament_data = json.loads(
                        filament_path.read_text(encoding="utf-8")
                    )

                    for slicer in slicers:
                        result = self._match_filament(
                            brand_id=brand_id,
                            brand_name=brand_name,
                            material=material,
                            filament_data=filament_data,
                            filament_path=filament_path,
                            slicer=slicer,
                            generic_profiles=generic_profiles,
                        )
                        if result is None:
                            report.skipped.append(
                                (filament_path, f"no match for {brand_name} {material}/{filament_data.get('name', '')} [{slicer}]")
                            )
                            continue

                        existing_settings = filament_data.get("slicer_settings", {})
                        existing_slicer = existing_settings.get(slicer, {})
                        existing_name = existing_slicer.get("profile_name")

                        existing_id = existing_slicer.get("id")

                        has_conflict = False

                        if existing_name and existing_name != result.profile_name:
                            report.conflicts.append(
                                MappingConflict(
                                    filament_path=filament_path,
                                    slicer=slicer,
                                    field="profile_name",
                                    existing=existing_name,
                                    derived=result.profile_name,
                                )
                            )
                            has_conflict = True

                        if (
                            result.slicer_id
                            and existing_id
                            and existing_id != result.slicer_id
                        ):
                            # If the existing ID isn't in the profile store,
                            # it was manually curated (likely a variant-specific
                            # ID like EPETGPROB00).  Keep it and don't conflict.
                            try:
                                slicer_type = SlicerType(slicer)
                                existing_in_store = bool(
                                    self.index.find_by_slicer_id(slicer_type, existing_id)
                                )
                            except (ValueError, AttributeError):
                                existing_in_store = False

                            if not existing_in_store:
                                # Preserve the manually curated ID — override
                                # the derived one so downstream treats it as
                                # already correct.
                                result = MappingResult(
                                    filament_path=result.filament_path,
                                    slicer=result.slicer,
                                    profile_name=result.profile_name,
                                    slicer_id=existing_id,
                                    generic_id=result.generic_id,
                                    vendor=result.vendor,
                                )
                            else:
                                report.conflicts.append(
                                    MappingConflict(
                                        filament_path=filament_path,
                                        slicer=slicer,
                                        field="slicer_id",
                                        existing=existing_id,
                                        derived=result.slicer_id,
                                    )
                                )
                                has_conflict = True

                        if has_conflict:
                            continue

                        existing_generic_id = existing_slicer.get("generic_id")

                        name_matches = existing_name == result.profile_name
                        id_matches = (
                            not result.slicer_id
                            or existing_id == result.slicer_id
                        )
                        gid_matches = (
                            not result.generic_id
                            or existing_generic_id == result.generic_id
                        )

                        if name_matches and id_matches and gid_matches:
                            report.already_correct.append(result)
                        else:
                            report.updated.append(result)

        if report.conflicts:
            return report

        if not dry_run:
            # Write all results (updated + already_correct) to ensure
            # generic_id is added and slicer_ids are migrated everywhere.
            self._write_updates(report.updated + report.already_correct)

        return report

    def _match_filament(
        self,
        brand_id: str,
        brand_name: str,
        material: str,
        filament_data: dict,
        filament_path: Path,
        slicer: str,
        generic_profiles: dict[str, list[tuple[str, str, str]]] | None = None,
    ) -> MappingResult | None:
        """Try to match a filament to a slicer profile base name.

        Searches across ALL vendors for the slicer, using candidate
        profile names derived from the brand name and filament metadata.
        """
        prefixes = get_profile_prefixes(brand_id, brand_name)
        if not prefixes:
            return None

        filament_name = filament_data.get("name", "")

        try:
            slicer_type = SlicerType(slicer)
        except ValueError:
            return None

        for prefix in prefixes:
            candidates = self._compose_candidates(prefix, material, filament_name)
            for candidate in candidates:
                matches = self.index.find_by_base_name_any_vendor(slicer_type, candidate)
                if matches:
                    vendor, profiles = matches[0]
                    profile_base_name = profiles[0].name.split(" @")[0]
                    slicer_id = _best_slicer_id(profiles)

                    # Resolve generic_id from the profile's filament_type
                    gid = None
                    if generic_profiles:
                        gid = resolve_generic_id(
                            generic_profiles.get(slicer, []),
                            material.upper(),
                            profile_base_name,
                        )

                    return MappingResult(
                        filament_path=filament_path,
                        slicer=slicer,
                        profile_name=profile_base_name,
                        slicer_id=slicer_id,
                        generic_id=gid,
                        vendor=vendor,
                    )

        return None

    def _compose_candidates(
        self, prefix: str, material: str, filament_name: str
    ) -> list[str]:
        """
        Generate candidate base profile names to search for.

        Given prefix="Bambu", material="PLA", filament_name="Matte":
        1. "Bambu PLA Matte" (prefix + material + name)
        2. "Bambu PLA-Matte" (hyphenated variant)
        3. "Bambu Support Matte" (support material pattern)
        4. "Bambu Matte" (prefix + name, for names that embed material)
        5. "Bambu PLA" (just prefix + material, when name matches material)
        """
        material_upper = material.upper()
        candidates = []

        if filament_name:
            # Primary: "{prefix} {MATERIAL} {name}"
            candidates.append(f"{prefix} {material_upper} {filament_name}")

            # Hyphenated: "{prefix} {MATERIAL}-{name}" (e.g. "Bambu ASA-Aero")
            candidates.append(f"{prefix} {material_upper}-{filament_name}")

            # Support material pattern: "{prefix} Support {name}"
            # e.g. PVA/for_abs -> name="for ABS" -> "Bambu Support for ABS"
            if filament_name.lower().startswith("for "):
                candidates.append(f"{prefix} Support {filament_name}")

            # If name starts with material, try stripping it
            # e.g., name="PLA-Matte" for material="PLA" → also try "{prefix} {MATERIAL} Matte"
            name_upper = filament_name.upper()
            if name_upper.startswith(material_upper):
                suffix = filament_name[len(material_upper):].lstrip("-+ ")
                if suffix:
                    candidates.append(f"{prefix} {material_upper} {suffix}")

            # Without material (for names that embed material like "ABS-GF")
            candidates.append(f"{prefix} {filament_name}")

        # When filament_name is the material itself or name is empty
        if not filament_name or filament_name.upper() == material_upper:
            candidates.append(f"{prefix} {material_upper}")

        return candidates

    def _write_updates(self, results: list[MappingResult]) -> None:
        """Write id, generic_id, and profile_name into slicer_settings in filament.json files.

        Also migrates any legacy slicer_ids entries into slicer_settings and
        removes the top-level slicer_ids block.
        """
        by_path: dict[Path, list[MappingResult]] = {}
        for r in results:
            by_path.setdefault(r.filament_path, []).append(r)

        for path, mappings in by_path.items():
            data = json.loads(path.read_text(encoding="utf-8"))

            # Migrate any existing slicer_ids into slicer_settings
            if "slicer_ids" in data:
                for slicer_key, sid in data["slicer_ids"].items():
                    ss = data.setdefault("slicer_settings", {}).setdefault(slicer_key, {})
                    if "id" not in ss:
                        ss["id"] = sid
                del data["slicer_ids"]

            for m in mappings:
                ss = data.setdefault("slicer_settings", {}).setdefault(m.slicer, {})
                ss["profile_name"] = m.profile_name
                if m.slicer_id:
                    ss["id"] = m.slicer_id
                if m.generic_id:
                    ss["generic_id"] = m.generic_id

            path.write_text(
                json.dumps(data, indent=4, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
