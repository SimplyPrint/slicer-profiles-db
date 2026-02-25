"""
ProfileStore: persistent versioned storage with change detection.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional

from .models import (
    SlicerType,
    ProfileType,
    ParsedProfile,
    StoredProfile,
    IngestionReport,
)

logger = logging.getLogger(__name__)


class ProfileStore:
    """
    Persistent store for versioned slicer profiles.

    Usage:
        store = ProfileStore("/path/to/store")

        # Ingest a new version of profiles
        report = store.ingest(
            SlicerType.BAMBUSTUDIO,
            version="v02.05.00.66",
            profiles_dir=Path("profiles/bambustudio"),
        )
        print(report.added, report.changed)

        # Or ingest pre-parsed profiles directly
        report = store.ingest_profiles(
            SlicerType.BAMBUSTUDIO,
            version="v02.05.00.66",
            profiles=[...],
        )

        # Look up a profile
        profile = store.get(SlicerType.BAMBUSTUDIO, "filament", "BBL", "Bambu ABS")
        print(profile.get_latest("nozzle_temperature"))

        # Compare versions
        changes = profile.changed_settings("v02.04.00", "v02.05.00.66")
    """

    def __init__(self, store_path: str | Path):
        self.root = Path(store_path)

    def _get_parser(self, slicer: SlicerType):
        """Lazy-import parsers to avoid circular dependency."""
        from .parsers import (
            BambuStudioParser,
            OrcaSlicerParser,
            PrusaSlicerParser,
            CuraParser,
            ElegooSlicerParser,
            SuperSlicerParser,
        )

        parsers = {
            SlicerType.BAMBUSTUDIO: BambuStudioParser,
            SlicerType.ORCASLICER: OrcaSlicerParser,
            SlicerType.PRUSASLICER: PrusaSlicerParser,
            SlicerType.CURA: CuraParser,
            SlicerType.ELEGOOSLICER: ElegooSlicerParser,
            SlicerType.SUPERSLICER: SuperSlicerParser,
        }
        return parsers[slicer]()

    def ingest(
        self,
        slicer: SlicerType,
        version: str,
        profiles_dir: Path,
    ) -> IngestionReport:
        """
        Ingest all profiles from a slicer directory for a given version.
        Delegates to parser + ingest_profiles().
        """
        parser = self._get_parser(slicer)
        parsed = list(parser.parse_directory(profiles_dir))
        return self.ingest_profiles(slicer, version, parsed)

    def ingest_profiles(
        self,
        slicer: SlicerType,
        version: str,
        profiles: list[ParsedProfile],
    ) -> IngestionReport:
        """
        Ingest pre-parsed profiles into the store for a given version.

        - New profiles: creates StoredProfile with first_seen = version
        - Existing profiles: compares each setting value; adds new version
          entry only if the value changed
        - Missing profiles: keeps them but doesn't update last_seen
        - Returns IngestionReport with adds, removes, changes
        """
        added = []
        changed = {}
        seen_keys = set()

        for p in profiles:
            key = self._profile_key(slicer, p.profile_type, p.vendor, p.name)
            seen_keys.add(key)

            existing = self._load(slicer, p.profile_type.value, p.vendor, p.name)
            if existing is None:
                stored = self._create_stored(p, version)
                # Handle renames: merge history from old-name profile
                old_name = self._extract_renamed_from(p.settings)
                if old_name:
                    old_profile = self._load(slicer, p.profile_type.value, p.vendor, old_name)
                    if old_profile is not None:
                        self._merge_rename(stored, old_profile)
                        self._delete(slicer, p.profile_type.value, p.vendor, old_name)
                self._save(stored)
                added.append(p.name)
            else:
                prev_last_seen = existing.last_seen
                changed_keys = self._merge_version(existing, p, version)
                if changed_keys:
                    changed[p.name] = changed_keys
                # Only write to disk if something actually changed
                if changed_keys or prev_last_seen != version:
                    self._save(existing)

        # Detect removed profiles
        all_keys = self._list_profile_keys(slicer)
        removed = [k for k in all_keys if k not in seen_keys]

        # Update slicer metadata
        self._update_meta(slicer, version)

        return IngestionReport(
            slicer=slicer,
            version=version,
            profiles_processed=len(profiles),
            added=added,
            removed=removed,
            changed=changed,
            unchanged=len(profiles) - len(added) - len(changed),
        )

    def get(
        self,
        slicer: SlicerType,
        profile_type: str,
        vendor: str,
        name: str,
    ) -> Optional[StoredProfile]:
        """Load a stored profile by its coordinates."""
        return self._load(slicer, profile_type, vendor, name)

    def list_profiles(
        self,
        slicer: SlicerType,
        profile_type: Optional[str] = None,
    ) -> list[StoredProfile]:
        """List all stored profiles for a slicer, optionally filtered by type."""
        slicer_dir = self.root / slicer.value
        if not slicer_dir.exists():
            return []

        profiles = []

        for vendor_dir in slicer_dir.iterdir():
            if not vendor_dir.is_dir() or vendor_dir.name.startswith("_"):
                continue

            if profile_type:
                type_dir = vendor_dir / profile_type
                if type_dir.exists():
                    for json_file in type_dir.rglob("*.json"):
                        try:
                            profiles.append(
                                StoredProfile.model_validate_json(
                                    json_file.read_text(encoding="utf-8")
                                )
                            )
                        except Exception:
                            continue
            else:
                for type_dir in vendor_dir.iterdir():
                    if not type_dir.is_dir():
                        continue
                    for json_file in type_dir.rglob("*.json"):
                        try:
                            profiles.append(
                                StoredProfile.model_validate_json(
                                    json_file.read_text(encoding="utf-8")
                                )
                            )
                        except Exception:
                            continue

        return profiles

    def get_versions(self, slicer: SlicerType) -> list[str]:
        """Get all ingested versions for a slicer, in order."""
        meta = self._load_meta(slicer)
        return meta.get("versions", [])

    # --- Internal methods ---

    def _merge_version(
        self, stored: StoredProfile, parsed: ParsedProfile, version: str
    ) -> list[str]:
        """
        Compare parsed settings against stored profile.
        Add version entry for each setting that changed.
        Returns list of changed setting keys.
        """
        changed = []
        stored.last_seen = version

        for key, new_value in parsed.settings.items():
            if key in self._META_KEYS:
                continue
            current = stored.get_latest(key)
            if self._normalize(current) != self._normalize(new_value):
                stored.settings.setdefault(key, {})[version] = new_value
                changed.append(key)

        return changed

    # Keys that are profile metadata, not real settings — excluded from versioned storage
    _META_KEYS = frozenset({"renamed_from"})

    def _create_stored(self, parsed: ParsedProfile, version: str) -> StoredProfile:
        """Create a new StoredProfile from a ParsedProfile."""
        settings = {}
        for key, value in parsed.settings.items():
            if key in self._META_KEYS:
                continue
            settings[key] = {version: value}

        return StoredProfile(
            slicer=parsed.slicer.value,
            profile_type=parsed.profile_type.value,
            name=parsed.name,
            vendor=parsed.vendor,
            first_seen=version,
            last_seen=version,
            filament_id=parsed.filament_id,
            setting_id=parsed.setting_id,
            settings=settings,
        )

    def _profile_key(
        self, slicer: SlicerType, profile_type: ProfileType | str, vendor: str, name: str
    ) -> str:
        """Compute a unique key for a profile."""
        pt = profile_type.value if isinstance(profile_type, ProfileType) else profile_type
        return f"{slicer.value}/{vendor}/{pt}/{name}"

    def _profile_path(
        self,
        slicer: SlicerType | str,
        profile_type: str,
        vendor: str,
        name: str,
    ) -> Path:
        """Compute filesystem path for a profile."""
        slicer_val = slicer.value if isinstance(slicer, SlicerType) else slicer
        safe_name = self._sanitize(name)
        return self.root / slicer_val / vendor / profile_type / f"{safe_name}.json"

    def _load(
        self,
        slicer: SlicerType | str,
        profile_type: str,
        vendor: str,
        name: str,
    ) -> Optional[StoredProfile]:
        path = self._profile_path(slicer, profile_type, vendor, name)
        if not path.exists():
            return None
        return StoredProfile.model_validate_json(path.read_text(encoding="utf-8"))

    def _save(self, stored: StoredProfile) -> None:
        path = self._profile_path(
            stored.slicer, stored.profile_type, stored.vendor, stored.name
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            stored.model_dump_json(indent=2),
            encoding="utf-8",
        )

    def _list_profile_keys(self, slicer: SlicerType) -> set[str]:
        """List all existing profile keys for a slicer from disk.

        Derives keys directly from file paths instead of parsing JSON,
        since the path structure is {slicer}/{vendor}/{type}/{name}.json.
        """
        keys = set()
        slicer_dir = self.root / slicer.value
        if not slicer_dir.exists():
            return keys

        slicer_val = slicer.value
        for vendor_dir in slicer_dir.iterdir():
            if not vendor_dir.is_dir() or vendor_dir.name.startswith("_"):
                continue
            vendor = vendor_dir.name
            for type_dir in vendor_dir.iterdir():
                if not type_dir.is_dir():
                    continue
                profile_type = type_dir.name
                for json_file in type_dir.rglob("*.json"):
                    name = json_file.stem
                    keys.add(f"{slicer_val}/{vendor}/{profile_type}/{name}")

        return keys

    def _load_meta(self, slicer: SlicerType) -> dict:
        meta_path = self.root / slicer.value / "_meta.json"
        if not meta_path.exists():
            return {}
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def _update_meta(self, slicer: SlicerType, version: str) -> None:
        meta = self._load_meta(slicer)
        versions = meta.get("versions", [])
        if version not in versions:
            versions.append(version)
        meta["versions"] = versions
        meta["last_ingested"] = version

        meta_path = self.root / slicer.value / "_meta.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @staticmethod
    def _extract_renamed_from(settings: dict) -> str | None:
        """Extract the old profile name from a renamed_from field.

        OrcaSlicer raw profiles use a plain string:
        ``"renamed_from": "Old Profile Name"``

        Returns the old name string, or None if not present.
        """
        rf = settings.get("renamed_from")
        if not rf:
            return None
        if isinstance(rf, str):
            return rf
        # Handle dict form (e.g. {"version": "old name"}) in case any source uses it
        if isinstance(rf, dict):
            for old_name in rf.values():
                if isinstance(old_name, str):
                    return old_name
        return None

    def _merge_rename(self, new_profile: StoredProfile, old_profile: StoredProfile) -> None:
        """Merge version history from an old (renamed) profile into a new one.

        Prepends the old profile's versioned settings so that the new profile
        carries the full history.  Updates first_seen and sets renamed_from.
        """
        new_profile.first_seen = old_profile.first_seen
        new_profile.renamed_from = old_profile.name

        for key, old_versions in old_profile.settings.items():
            if key in new_profile.settings:
                # Prepend old versions before new versions
                merged = dict(old_versions)
                merged.update(new_profile.settings[key])
                new_profile.settings[key] = merged
            else:
                new_profile.settings[key] = dict(old_versions)

    def _delete(
        self,
        slicer: SlicerType | str,
        profile_type: str,
        vendor: str,
        name: str,
    ) -> None:
        """Delete a stored profile file from disk."""
        path = self._profile_path(slicer, profile_type, vendor, name)
        if path.exists():
            path.unlink()

    @staticmethod
    def _sanitize(name: str) -> str:
        """Sanitize a profile name for use as a filename."""
        # Replace characters that are problematic on filesystems
        sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
        # Collapse multiple underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        return sanitized.strip("_. ")

    @staticmethod
    def _normalize(value) -> str:
        """Normalize a value for comparison."""
        if value is None:
            return ""
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
