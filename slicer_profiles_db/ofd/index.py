"""
OFDFilamentIndex: O(1) lookups for OFD filaments by various keys.
"""

from __future__ import annotations

import logging
from typing import Optional

from .repo import OFDRepo, OFDFilament

logger = logging.getLogger(__name__)


class OFDFilamentIndex:
    """Multi-strategy index over OFD filaments.

    Lookup dictionaries:
    - ``by_path``: filesystem path -> OFDFilament
    - ``by_slicer_profile``: (slicer, lowercase profile_name) -> OFDFilament
    - ``by_slicer_id``: (slicer, slicer_id) -> OFDFilament
    - ``by_brand_material_name``: (brand_name_lower, material_upper, filament_name_lower) -> OFDFilament
    """

    def __init__(self, repo: OFDRepo):
        self.by_path: dict[str, OFDFilament] = {}
        self.by_slicer_profile: dict[str, dict[str, OFDFilament]] = {}
        self.by_slicer_id: dict[str, dict[str, OFDFilament]] = {}
        self.by_brand_material_name: dict[tuple[str, str, str], OFDFilament] = {}
        self._build(repo)

    def _build(self, repo: OFDRepo) -> None:
        for fil in repo.filaments:
            # By filesystem path
            self.by_path[fil.fs_path] = fil

            # By brand/material/name tuple
            key = (
                fil.brand_name.lower(),
                fil.material.upper(),
                fil.filament_name.lower(),
            )
            self.by_brand_material_name[key] = fil

            # By slicer profile name (from slicer_settings)
            for slicer, settings in fil.slicer_settings.items():
                profile_name = settings.get("profile_name")
                if profile_name:
                    self.by_slicer_profile.setdefault(slicer, {})[
                        profile_name.lower()
                    ] = fil

            # By slicer ID (from slicer_ids)
            for slicer, sid in fil.slicer_ids.items():
                if sid:
                    self.by_slicer_id.setdefault(slicer, {})[sid] = fil

            # Also index IDs from slicer_settings (migrated format)
            for slicer, settings in fil.slicer_settings.items():
                sid = settings.get("id")
                if sid:
                    self.by_slicer_id.setdefault(slicer, {}).setdefault(sid, fil)

        logger.info(
            "OFD index: %d paths, %d profile names, %d slicer IDs, %d brand/material/name",
            len(self.by_path),
            sum(len(v) for v in self.by_slicer_profile.values()),
            sum(len(v) for v in self.by_slicer_id.values()),
            len(self.by_brand_material_name),
        )

    def resolve_path(
        self,
        vendor: str,
        filament_type: str,
        profile_name: str,
        slicer: str,
        *,
        filament_id: Optional[str] = None,
    ) -> str | None:
        """Resolve an OFD filesystem path using multi-strategy lookup.

        Strategies (tried in order):
        1. By slicer profile name (from ``slicer_settings``)
        2. By slicer ID (from ``slicer_ids``, matched via filament_id)
        3. By brand/material/name decomposition (fallback)

        Args:
            vendor: Slicer profile vendor name (e.g. "BBL").
            filament_type: Filament material type (e.g. "PLA").
            profile_name: Slicer profile display name (e.g. "Bambu PLA Aero @BBL X1C").
            slicer: Slicer identifier (e.g. "bambustudio").
            filament_id: Optional slicer-native filament ID (e.g. "GFB00").

        Returns:
            OFD filesystem path (e.g. "bambu_lab/PLA/aero") or None.
        """
        # Strip " @printer" suffix — OFD stores base names only
        base_name = profile_name.split(" @")[0] if " @" in profile_name else profile_name

        # Sanitise filament_id — some slicers store profile names as IDs
        if filament_id and (" @" in filament_id or " " in filament_id):
            filament_id = None

        # Strategy 1: By slicer profile name (exact match on base name)
        slicer_profiles = self.by_slicer_profile.get(slicer, {})
        fil = slicer_profiles.get(base_name.lower())
        if fil:
            return fil.fs_path

        # Strategy 2: By slicer ID — only trust if the profile name contains
        # the OFD brand name (avoids generic profiles like "Generic ABS @Geeetech"
        # resolving to a specific brand via shared slicer IDs like "GFB00")
        if filament_id:
            slicer_ids = self.by_slicer_id.get(slicer, {})
            fil = slicer_ids.get(filament_id)
            if fil:
                bn_lower = base_name.lower()
                brand_lower = fil.brand_name.lower()
                if brand_lower in bn_lower or fil.brand_id.replace("_", " ") in bn_lower:
                    return fil.fs_path

        # Strategy 3: Brand/material/name decomposition
        # base_name is typically "{Brand} {MATERIAL} {Name}" or "{Brand} {MATERIAL}"
        parts = base_name.split()
        if len(parts) >= 2:
            material_upper = filament_type.upper() if filament_type else ""
            for i in range(1, len(parts)):
                candidate_brand = " ".join(parts[:i]).lower()
                remaining = parts[i:]
                # Check if remaining starts with material
                if remaining and remaining[0].upper() == material_upper and len(remaining) > 1:
                    candidate_name = " ".join(remaining[1:]).lower()
                    key = (candidate_brand, material_upper, candidate_name)
                    fil = self.by_brand_material_name.get(key)
                    if fil:
                        return fil.fs_path
                # When name equals material or material is embedded in name
                if remaining and remaining[0].upper() == material_upper and len(remaining) == 1:
                    key = (candidate_brand, material_upper, material_upper.lower())
                    fil = self.by_brand_material_name.get(key)
                    if fil:
                        return fil.fs_path

        return None

    def build_filament_map(self) -> dict[str, dict[str, str]]:
        """Build a slicer -> {profile_name -> fs_path} lookup map.

        This is the pre-computed ``ofd_filament_map.json`` content.
        """
        result: dict[str, dict[str, str]] = {}
        for slicer, profiles in self.by_slicer_profile.items():
            slicer_map: dict[str, str] = {}
            for _lower_name, fil in profiles.items():
                # Use the original-case profile name from slicer_settings
                original_name = fil.slicer_settings.get(slicer, {}).get("profile_name")
                if original_name:
                    slicer_map[original_name] = fil.fs_path
            result[slicer] = slicer_map
        return result
