"""
ProfileIndex: in-memory O(1) lookups over stored profiles.
"""

from typing import Any, Optional

from .models import SlicerType, ProfileType, StoredProfile
from .store import ProfileStore
from .conditions import evaluate_printer_condition


class ProfileIndex:
    """
    In-memory index over stored profiles for fast lookups.

    Built from a ProfileStore, provides O(1) access by:
    - slicer_id (e.g., "GFB00")
    - name + vendor
    - material type (for generics)
    - profile type (filament, machine, machine_model, print)
    """

    def __init__(self, store: ProfileStore):
        self.store = store
        # Filament-specific indexes
        self._by_slicer_id: dict[SlicerType, dict[str, list[StoredProfile]]] = {}
        self._by_name: dict[SlicerType, dict[str, dict[str, list[StoredProfile]]]] = {}
        self._generics: dict[SlicerType, dict[str, dict[str, list[StoredProfile]]]] = {}
        # Multi-type indexes: {slicer: {profile_type: {vendor: {name: [profiles]}}}}
        self._by_type: dict[
            SlicerType, dict[str, dict[str, dict[str, list[StoredProfile]]]]
        ] = {}
        # Base-name index: {slicer: {vendor: {base_name: [profiles]}}}
        self._by_base_name: dict[
            SlicerType, dict[str, dict[str, list[StoredProfile]]]
        ] = {}

    def build(self, slicers: Optional[list[SlicerType]] = None) -> None:
        """Build indexes from the store."""
        self._by_slicer_id.clear()
        self._by_name.clear()
        self._generics.clear()
        self._by_type.clear()
        self._by_base_name.clear()

        for slicer in slicers or list(SlicerType):
            for profile in self.store.list_profiles(slicer):
                self._index(slicer, profile)

    def _index(self, slicer: SlicerType, profile: StoredProfile) -> None:
        """Index a single profile."""
        # Multi-type index
        self._by_type.setdefault(slicer, {}).setdefault(
            profile.profile_type, {}
        ).setdefault(profile.vendor, {}).setdefault(profile.name, []).append(profile)

        # Index by slicer_id (filament_id)
        if profile.filament_id:
            self._by_slicer_id.setdefault(slicer, {}).setdefault(
                profile.filament_id, []
            ).append(profile)

        # Index by setting_id
        if profile.setting_id:
            self._by_slicer_id.setdefault(slicer, {}).setdefault(
                profile.setting_id, []
            ).append(profile)

        # Index by vendor + name
        self._by_name.setdefault(slicer, {}).setdefault(
            profile.vendor, {}
        ).setdefault(profile.name, []).append(profile)

        # Index by base name (name before " @") for mapper lookups
        # Keyed case-insensitively for flexible matching
        base_name = profile.name.split(" @")[0]
        base_key = base_name.lower()
        self._by_base_name.setdefault(slicer, {}).setdefault(
            profile.vendor, {}
        ).setdefault(base_key, (base_name, []))
        self._by_base_name[slicer][profile.vendor][base_key][1].append(profile)

        # Index generics by vendor + filament_type
        filament_vendor = profile.get_latest("filament_vendor")
        if isinstance(filament_vendor, list):
            filament_vendor = filament_vendor[0] if filament_vendor else ""

        if filament_vendor == "Generic":
            filament_type = profile.get_latest("filament_type")
            if isinstance(filament_type, list):
                filament_type = filament_type[0] if filament_type else ""
            if filament_type:
                self._generics.setdefault(slicer, {}).setdefault(
                    profile.vendor, {}
                ).setdefault(filament_type, []).append(profile)

    def find_by_slicer_id(
        self, slicer: SlicerType, slicer_id: str
    ) -> list[StoredProfile]:
        """O(1) lookup by slicer-native filament/setting ID."""
        return self._by_slicer_id.get(slicer, {}).get(slicer_id, [])

    def find_by_name(
        self, slicer: SlicerType, vendor: str, name: str
    ) -> list[StoredProfile]:
        """O(1) lookup by vendor and name."""
        return self._by_name.get(slicer, {}).get(vendor, {}).get(name, [])

    def find_by_base_name(
        self, slicer: SlicerType, vendor: str, base_name: str
    ) -> list[StoredProfile]:
        """O(1) case-insensitive lookup by vendor and base profile name (without @ printer suffix)."""
        entry = self._by_base_name.get(slicer, {}).get(vendor, {}).get(base_name.lower())
        return entry[1] if entry else []

    def find_by_base_name_any_vendor(
        self, slicer: SlicerType, base_name: str
    ) -> list[tuple[str, list[StoredProfile]]]:
        """Search all vendors for a base profile name (case-insensitive).

        Returns list of (vendor, profiles) tuples.
        """
        key = base_name.lower()
        results = []
        for vendor, names in self._by_base_name.get(slicer, {}).items():
            entry = names.get(key)
            if entry:
                results.append((vendor, entry[1]))
        return results

    def find_by_type(
        self,
        slicer: SlicerType,
        profile_type: ProfileType | str,
        vendor: str | None = None,
        name: str | None = None,
    ) -> list[StoredProfile]:
        """
        Lookup profiles by type, optionally filtered by vendor and/or name.

        Args:
            slicer: Slicer to search in.
            profile_type: Profile type to filter by.
            vendor: Optional vendor filter.
            name: Optional name filter.

        Returns:
            List of matching stored profiles.
        """
        pt = profile_type.value if isinstance(profile_type, ProfileType) else profile_type
        type_idx = self._by_type.get(slicer, {}).get(pt, {})

        if vendor and name:
            return type_idx.get(vendor, {}).get(name, [])
        elif vendor:
            results = []
            for profiles in type_idx.get(vendor, {}).values():
                results.extend(profiles)
            return results
        else:
            results = []
            for vendor_profiles in type_idx.values():
                for profiles in vendor_profiles.values():
                    results.extend(profiles)
            return results

    def find_generic(
        self, slicer: SlicerType, vendor: str, filament_type: str
    ) -> list[StoredProfile]:
        """O(1) lookup for generic profiles by vendor and filament type."""
        return self._generics.get(slicer, {}).get(vendor, {}).get(filament_type, [])

    def find_compatible(
        self,
        profiles: list[StoredProfile],
        printer_name: str,
        printer_data: dict,
        slicer: str = "bambustudio",
    ) -> Optional[StoredProfile]:
        """Filter profiles by printer compatibility (evaluates conditions)."""
        for profile in profiles:
            compat = profile.get_latest("compatible_printers") or []
            if isinstance(compat, str):
                # PrusaSlicer uses semicolon-separated strings
                compat = [x.strip().strip('"') for x in compat.split(";") if x.strip()]

            if printer_name in compat:
                return profile

            condition = profile.get_latest("compatible_printers_condition")
            if condition and evaluate_printer_condition(condition, printer_data, slicer):
                return profile

        return None

    def find_all_compatible(
        self,
        profiles: list[StoredProfile],
        printer_name: str,
        printer_data: dict,
        slicer: str = "bambustudio",
    ) -> list[StoredProfile]:
        """Return all profiles compatible with the given printer."""
        compatible = []
        for profile in profiles:
            compat = profile.get_latest("compatible_printers") or []
            if isinstance(compat, str):
                compat = [x.strip().strip('"') for x in compat.split(";") if x.strip()]

            if printer_name in compat:
                compatible.append(profile)
                continue

            condition = profile.get_latest("compatible_printers_condition")
            if condition and evaluate_printer_condition(condition, printer_data, slicer):
                compatible.append(profile)

        return compatible

    def find_filament_profile(
        self,
        slicer: SlicerType,
        vendor: str,
        printer_name: str,
        printer_data: dict,
        filament_name: str,
        filament_type: str,
    ) -> Optional[StoredProfile]:
        """
        Hierarchical search (ported from printer-slicing-db/find_filament_profile.py):
        1. Specific: vendor + name + printer compatible
        2. Template: name across all vendors
        3. Printer-generic: vendor + type + printer compatible
        4. Generic: type across all vendors
        """
        slicer_str = slicer.value

        # For PrusaSlicer, use printer_settings_id as the printer name
        if slicer == SlicerType.PRUSASLICER:
            ps_id = printer_data.get("printer_settings_id")
            if ps_id:
                printer_name = ps_id

        # 1. Specific
        candidates = self.find_by_name(slicer, vendor, filament_name)
        result = self.find_compatible(candidates, printer_name, printer_data, slicer_str)
        if result:
            return result

        # 2. Template - search all vendors for this name
        for v_profiles in self._by_name.get(slicer, {}).values():
            matches = v_profiles.get(filament_name, [])
            if len(matches) == 1:
                return matches[0]

        # 3. Printer-generic
        candidates = self.find_generic(slicer, vendor, filament_type)
        result = self.find_compatible(candidates, printer_name, printer_data, slicer_str)
        if result:
            return result

        # 4. Generic - any vendor
        for v_generics in self._generics.get(slicer, {}).values():
            matches = v_generics.get(filament_type, [])
            if len(matches) == 1:
                return matches[0]

        return None


def is_profile_generic(profile: StoredProfile) -> bool:
    """Check if a stored profile is a generic profile."""
    filament_vendor = profile.get_latest("filament_vendor")
    if isinstance(filament_vendor, list):
        filament_vendor = filament_vendor[0] if filament_vendor else ""
    return filament_vendor == "Generic"


def is_profile_model_specific(
    slicer: SlicerType,
    vendor: str,
    profile: StoredProfile,
    model_counts: dict[str, dict[str, int]] | None = None,
) -> bool:
    """
    Check if a filament profile targets only a subset of a vendor's printers.

    Args:
        slicer: Slicer type.
        vendor: Vendor name.
        profile: The profile to check.
        model_counts: Optional dict of {slicer: {vendor: num_models}}.
                      Required for non-PrusaSlicer.

    Returns:
        True if the profile is model-specific.
    """
    if slicer == SlicerType.PRUSASLICER:
        condition = profile.get_latest("compatible_printers_condition")
        if not condition:
            return False
        return (
            ".*PRINTER_MODEL_" in condition
            or "printer_model=" in condition
        )

    # For BBS/Orca, compare compatible_printers count to total model count
    compat = profile.get_latest("compatible_printers") or []
    if isinstance(compat, str):
        compat = [x.strip() for x in compat.split(";") if x.strip()]

    if model_counts and slicer.value in model_counts:
        total = model_counts[slicer.value].get(vendor, 0)
        if total > 0:
            return len(compat) != total

    return False


def build_generic_profile_index(
    index: ProfileIndex,
    slicers: list[SlicerType] | None = None,
) -> dict[str, list[tuple[str, str, str]]]:
    """Build a generic profile index for resolving generic_id values.

    Returns: {slicer_val: [(generic_name_lower, filament_type_upper, filament_id), ...]}
    Sorted longest-name-first so specific generics (e.g. "Generic PLA Silk") match
    before base generics (e.g. "Generic PLA").
    """
    if slicers is None:
        slicers = list(SlicerType)

    result: dict[str, list[tuple[str, str, str]]] = {}
    for slicer in slicers:
        entries: list[tuple[str, str, str]] = []
        for fp in index.find_by_type(slicer, ProfileType.FILAMENT):
            if "Generic" not in fp.name or " @" in fp.name:
                continue
            if not fp.filament_id or " " in fp.filament_id:
                continue
            fp_data = fp.evaluate(fp.last_seen)
            ft = fp_data.get("filament_type", "")
            if isinstance(ft, list):
                ft = ft[0] if ft else ""
            if ft:
                entries.append((fp.name.lower(), ft.upper(), fp.filament_id))
        entries.sort(key=lambda e: -len(e[0]))
        result[slicer.value] = entries
    return result


def resolve_generic_id(
    generics: list[tuple[str, str, str]],
    filament_type: str,
    filament_name: str,
) -> str | None:
    """Find the best-matching generic profile ID for a filament.

    ``generics`` is sorted longest-name-first so "generic pla silk" is tried
    before "generic pla".  We check that the filament_type matches, then see
    whether the sub-type keywords from the generic name appear in the filament
    name (e.g. "Silk" from "Generic PLA Silk" found in "Bambu PLA Silk").
    Falls back to the plain "Generic {MATERIAL}" entry.
    """
    ft_upper = filament_type.upper()
    name_lower = filament_name.lower()
    base_fallback: str | None = None

    for gen_name, gen_ft, gen_fid in generics:
        if gen_ft != ft_upper:
            continue
        prefix = f"generic {gen_ft.lower()}"
        suffix = gen_name[len(prefix):].strip() if gen_name.startswith(prefix) else ""
        if suffix:
            if suffix in name_lower:
                return gen_fid
        else:
            base_fallback = gen_fid

    return base_fallback
