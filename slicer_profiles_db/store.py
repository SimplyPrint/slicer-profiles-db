"""
ProfileStore: persistent versioned storage with change detection.
"""

import hashlib
import json
import logging
import re
from pathlib import Path

from .models import (
    IngestionReport,
    ParsedProfile,
    ProfileType,
    SlicerType,
    StoredProfile,
    _version_key,
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

    MAX_FILENAME_STEM_BYTES = 240
    FILENAME_HASH_LENGTH = 12
    RELEASE_DIGEST_LENGTH = 32

    def __init__(self, store_path: str | Path):
        self.root = Path(store_path)

    def _get_parser(self, slicer: SlicerType):
        """Lazy-import parsers to avoid circular dependency."""
        from .parsers import (
            AnycubicSlicerParser,
            BambuStudioParser,
            CrealityPrintParser,
            CuraParser,
            ElegooSlicerParser,
            KiriMotoParser,
            OrcaSlicerParser,
            PrusaSlicerParser,
            SuperSlicerParser,
        )

        parsers = {
            SlicerType.BAMBUSTUDIO: BambuStudioParser,
            SlicerType.ORCASLICER: OrcaSlicerParser,
            SlicerType.CREALITYPRINT: CrealityPrintParser,
            SlicerType.PRUSASLICER: PrusaSlicerParser,
            SlicerType.CURA: CuraParser,
            SlicerType.ELEGOOSLICER: ElegooSlicerParser,
            SlicerType.ANYCUBICSLICER: AnycubicSlicerParser,
            SlicerType.SUPERSLICER: SuperSlicerParser,
            SlicerType.KIRIMOTO: KiriMotoParser,
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
        from .resources import (
            ResourceStore,
            collect_referenced_hashes,
            collect_resources,
            rewrite_resource_refs,
        )

        resource_store = ResourceStore(self.root / slicer.value / "_resources")
        resource_map = collect_resources(profiles_dir, resource_store)
        parser = self._get_parser(slicer)
        parsed = list(parser.parse_directory(profiles_dir, resource_version=version))
        if resource_map:
            rewrite_resource_refs(parsed, resource_map)
        report = self.ingest_profiles(slicer, version, parsed)
        resource_store.gc(collect_referenced_hashes(self.root, slicer.value))
        return report

    def ingest_profiles(
        self,
        slicer: SlicerType,
        version: str,
        profiles: list[ParsedProfile],
        complete_release: bool = False,
    ) -> IngestionReport:
        """
        Ingest pre-parsed profiles into the store for a given version.

        - New profiles: creates StoredProfile with first_seen = version.  A new
          profile that upstream renamed (``renamed_from``) or moved to another
          vendor takes over the history of the profile it replaces.
        - Existing profiles: compares each setting value; adds new version
          entry only if the value changed
        - Missing profiles: kept.  When ``complete_release`` says ``profiles``
          is a whole stable release of the types it covers, each one missing
          is tombstoned at ``version`` (every setting set to ``None``), so it
          evaluates empty from that release on.  Otherwise they are left as is.
        - Returns IngestionReport with adds, removes, changes
        """
        # Deduplicate: when multiple parsed profiles share the same key
        # (e.g. Cura generic_abs.xml and generic_abs_175.xml both produce
        # "Generic ABS Generic"), keep only the last one to prevent cross-run
        # oscillation that creates duplicate version entries.
        deduped: dict[str, ParsedProfile] = {}
        for p in profiles:
            key = self._profile_key(
                slicer, p.profile_type, p.vendor, p.name, p.storage_key
            )
            deduped[key] = p
        profiles = list(deduped.values())
        snapshot_hash = self._snapshot_hash(slicer, profiles)
        snapshot_scope = ",".join(
            sorted({profile.profile_type.value for profile in profiles})
        )
        meta = self._load_meta(slicer)
        recorded = meta.get("snapshots", {}).get(version, {}).get(snapshot_scope)
        if isinstance(recorded, str) and recorded != snapshot_hash:
            raise ValueError(
                f"Refusing to change immutable {slicer.value} snapshot {version}"
            )
        newest = self.newest_release(slicer)
        if (
            complete_release
            and not version.startswith("nightly")
            and newest is not None
            and _version_key(version) < _version_key(newest)
        ):
            # History is recorded as changes from the release ingested before,
            # so an older release ingested now would leak into the newer ones.
            raise ValueError(
                f"Refusing to ingest {slicer.value} {version} after {newest}: "
                f"rebuild with `slicer-profiles-db ingest-releases {slicer.value} --rebuild`"
            )

        added = []
        changed = {}
        seen_keys = set(deduped)
        scope_types = {profile.profile_type.value for profile in profiles}
        # Stored profiles this release no longer has, by type and name: the
        # profiles a rename or a vendor move can take over.
        replaceable: dict[tuple[str, str], list[StoredProfile]] = {}
        for stored_profile in self.list_profiles(slicer):
            if (
                stored_profile.storage_key
                or stored_profile.profile_type not in scope_types
            ):
                continue
            stored_key = self._profile_key(
                slicer,
                stored_profile.profile_type,
                stored_profile.vendor,
                stored_profile.name,
            )
            if stored_key not in seen_keys:
                replaceable.setdefault(
                    (stored_profile.profile_type, stored_profile.name), []
                ).append(stored_profile)

        for p in profiles:
            existing = self._load(
                slicer,
                p.profile_type.value,
                p.vendor,
                p.name,
                p.storage_key,
            )
            if existing is None:
                stored = self._create_stored(p, version)
                old_profile = (
                    None
                    if p.storage_key
                    else self._replaced_profile(p, replaceable, complete_release)
                )
                if old_profile is not None:
                    self._merge_rename(stored, old_profile, version)
                    self._delete(
                        slicer,
                        old_profile.profile_type,
                        old_profile.vendor,
                        old_profile.name,
                    )
                self._save(stored)
                added.append(p.name)
            else:
                changed_keys = self._merge_version(existing, p, version)
                if changed_keys:
                    changed[p.name] = changed_keys
                    self._save(existing)

        # Detect removed profiles
        all_keys = self._list_profile_keys(slicer)
        removed = [k for k in all_keys if k not in seen_keys]
        if complete_release and self._tombstones_at(meta, version):
            for stored_profile in self.list_profiles(slicer):
                key = self._profile_key(
                    slicer,
                    stored_profile.profile_type,
                    stored_profile.vendor,
                    stored_profile.name,
                    stored_profile.storage_key,
                )
                if key in seen_keys or stored_profile.profile_type not in scope_types:
                    continue
                if self._tombstone(stored_profile, version):
                    changed[stored_profile.name] = ["__removed__"]
                    self._save(stored_profile)

        # Only advance slicer metadata when this ingest changed stored state.
        # This avoids synthetic branch-based versions (for example daily Cura
        # ingests) accumulating in _meta.json when the parsed profiles are
        # identical to what is already stored.
        if (
            added
            or changed
            or (version in meta.get("versions", []) and not isinstance(recorded, str))
        ):
            self._update_meta(slicer, version, snapshot_scope, snapshot_hash)
        if complete_release and self._is_release(version):
            self._save_release_manifest(slicer, version, profiles)

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
    ) -> StoredProfile | None:
        """Load a stored profile by its coordinates."""
        return self._load(slicer, profile_type, vendor, name)

    def list_profiles(
        self,
        slicer: SlicerType,
        profile_type: str | None = None,
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
                        except (OSError, UnicodeError, ValueError) as exc:
                            logger.debug(
                                "Skipping unreadable profile %s: %s", json_file, exc
                            )
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
                        except (OSError, UnicodeError, ValueError) as exc:
                            logger.debug(
                                "Skipping unreadable profile %s: %s", json_file, exc
                            )
                            continue

        return profiles

    def get_versions(self, slicer: SlicerType) -> list[str]:
        """Get all ingested versions for a slicer, in order."""
        meta = self._load_meta(slicer)
        return meta.get("versions", [])

    def newest_release(self, slicer: SlicerType) -> str | None:
        """The newest non-nightly version ingested for a slicer."""
        versions = [v for v in self.get_versions(slicer) if not v.startswith("nightly")]
        return max(versions, key=_version_key) if versions else None

    @classmethod
    def settings_digest(cls, settings: dict) -> str:
        """A release snapshot's identity: its settings as the store evaluates them.

        ``None`` is how the store records a removed setting, and metadata keys
        are not stored, so neither is part of it.
        """
        canonical = json.dumps(
            cls._snapshot_settings(settings),
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return digest[: cls.RELEASE_DIGEST_LENGTH]

    @classmethod
    def _snapshot_settings(cls, settings: dict) -> dict:
        return {
            key: value
            for key, value in settings.items()
            if value is not None and key not in cls._META_KEYS
        }

    def release_manifest(
        self, slicer: SlicerType, release: str
    ) -> dict[str, str] | None:
        """``{profile key: settings digest}`` of every profile a complete ingest of
        ``release`` parsed, or None when the store never ingested it whole."""
        path = self._release_manifest_path(slicer, release)
        if not path.exists():
            return None
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise ValueError(f"Invalid release manifest {path}")
        profiles = document.get("profiles")
        if document.get("release") != release or not isinstance(profiles, dict):
            raise ValueError(f"Invalid release manifest {path}")
        return profiles

    def _release_manifest_path(self, slicer: SlicerType, release: str) -> Path:
        return (
            self.root / slicer.value / "_releases" / f"{self._sanitize(release)}.json"
        )

    def _save_release_manifest(
        self, slicer: SlicerType, release: str, profiles: list[ParsedProfile]
    ) -> None:
        manifest = {}
        for profile in profiles:
            # A profile without settings evaluates as absent.
            if not self._snapshot_settings(profile.settings):
                continue
            key = self._profile_key(
                slicer,
                profile.profile_type,
                profile.vendor,
                profile.name,
                profile.storage_key,
            )
            manifest[key] = self.settings_digest(profile.settings)
        document = {"release": release, "profiles": manifest}
        text = json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False)
        path = self._release_manifest_path(slicer, release)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")

    @staticmethod
    def _is_release(version: str) -> bool:
        from .versions import is_prerelease

        return not version.startswith("nightly") and not is_prerelease(version)

    def deduplicate_settings(self, slicer: SlicerType) -> int:
        """Remove consecutive duplicate values from all profiles for a slicer.

        When the same value appears in adjacent version entries, only the
        oldest (first) entry is kept.  Returns the total number of entries
        removed.
        """
        removed = 0
        for profile in self.list_profiles(slicer):
            changed = False
            for key, versions in profile.settings.items():
                entries = list(versions.items())
                if len(entries) <= 1:
                    continue
                cleaned = {entries[0][0]: entries[0][1]}
                for ver, val in entries[1:]:
                    prev_val = list(cleaned.values())[-1]
                    if self._normalize(prev_val) != self._normalize(val):
                        cleaned[ver] = val
                if len(cleaned) < len(entries):
                    profile.settings[key] = cleaned
                    changed = True
                    removed += len(entries) - len(cleaned)
            if changed:
                self._save(profile)
        return removed

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

        parsed_keys = set()

        for key, new_value in parsed.settings.items():
            if key in self._META_KEYS:
                continue
            parsed_keys.add(key)
            current = stored.get_latest(key)
            if self._normalize(current) != self._normalize(new_value):
                stored.settings.setdefault(key, {})[version] = new_value
                changed.append(key)

        # Record setting removals as a null tombstone.  Without this, a setting
        # removed by a later slicer release would incorrectly live forever in
        # evaluated snapshots.
        for key in list(stored.settings):
            if key in parsed_keys or key in self._META_KEYS:
                continue
            current = stored.get_latest(key)
            if current is not None:
                stored.settings.setdefault(key, {})[version] = None
                changed.append(key)

        metadata_changed = False
        for field_name in (
            "filament_id",
            "setting_id",
            "filament_type",
            "native_id",
            "storage_key",
        ):
            new_value = getattr(parsed, field_name)
            if new_value is not None and getattr(stored, field_name) != new_value:
                setattr(stored, field_name, new_value)
                metadata_changed = True

        if stored.context != parsed.context:
            stored.context = parsed.context
            metadata_changed = True
        if stored.setting_scopes != parsed.setting_scopes:
            stored.setting_scopes = parsed.setting_scopes
            metadata_changed = True

        if changed or metadata_changed:
            stored.last_seen = version

        if metadata_changed:
            changed.append("__profile_metadata__")

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
            filament_type=parsed.filament_type,
            native_id=parsed.native_id,
            storage_key=parsed.storage_key,
            context=parsed.context,
            setting_scopes=parsed.setting_scopes,
            settings=settings,
        )

    def _profile_key(
        self,
        slicer: SlicerType,
        profile_type: ProfileType | str,
        vendor: str,
        name: str,
        storage_key: str | None = None,
    ) -> str:
        """Compute a unique key for a profile."""
        pt = (
            profile_type.value
            if isinstance(profile_type, ProfileType)
            else profile_type
        )
        identity = storage_key or name
        return f"{slicer.value}/{vendor}/{pt}/{identity}"

    def _profile_path(
        self,
        slicer: SlicerType | str,
        profile_type: str,
        vendor: str,
        name: str,
        storage_key: str | None = None,
    ) -> Path:
        """Compute filesystem path for a profile."""
        slicer_val = slicer.value if isinstance(slicer, SlicerType) else slicer
        safe_name = self._sanitize(name)
        if storage_key:
            identity_hash = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()[
                : self.FILENAME_HASH_LENGTH
            ]
            safe_name = self._sanitize(f"{safe_name}__{identity_hash}")
        return self.root / slicer_val / vendor / profile_type / f"{safe_name}.json"

    def _load(
        self,
        slicer: SlicerType | str,
        profile_type: str,
        vendor: str,
        name: str,
        storage_key: str | None = None,
    ) -> StoredProfile | None:
        path = self._profile_path(slicer, profile_type, vendor, name, storage_key)
        if not path.exists():
            return None
        return StoredProfile.model_validate_json(path.read_text(encoding="utf-8"))

    def _save(self, stored: StoredProfile) -> None:
        path = self._profile_path(
            stored.slicer,
            stored.profile_type,
            stored.vendor,
            stored.name,
            stored.storage_key,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            stored.model_dump_json(indent=2),
            encoding="utf-8",
        )

    def _list_profile_keys(self, slicer: SlicerType) -> set[str]:
        """List all existing profile keys for a slicer from disk.

        Profile filenames are sanitized for portability, so the path stem is
        not necessarily the profile's identity (for example names containing
        ``/`` or ``:``).  Read the stored coordinates to avoid reporting such
        profiles as removed immediately after writing them.
        """
        keys = set()
        slicer_dir = self.root / slicer.value
        if not slicer_dir.exists():
            return keys

        for vendor_dir in slicer_dir.iterdir():
            if not vendor_dir.is_dir() or vendor_dir.name.startswith("_"):
                continue
            vendor = vendor_dir.name
            for type_dir in vendor_dir.iterdir():
                if not type_dir.is_dir():
                    continue
                profile_type = type_dir.name
                for json_file in type_dir.rglob("*.json"):
                    try:
                        stored = StoredProfile.model_validate_json(
                            json_file.read_text(encoding="utf-8")
                        )
                    except (OSError, ValueError):
                        logger.warning(
                            "Using filename identity for invalid stored profile: %s",
                            json_file,
                        )
                        keys.add(
                            self._profile_key(
                                slicer, profile_type, vendor, json_file.stem
                            )
                        )
                        continue
                    keys.add(
                        self._profile_key(
                            slicer,
                            stored.profile_type or profile_type,
                            stored.vendor or vendor,
                            stored.name,
                            stored.storage_key,
                        )
                    )

        return keys

    def _load_meta(self, slicer: SlicerType) -> dict:
        meta_path = self.root / slicer.value / "_meta.json"
        if not meta_path.exists():
            return {}
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def _update_meta(
        self,
        slicer: SlicerType,
        version: str,
        snapshot_scope: str,
        snapshot_hash: str,
    ) -> None:
        meta = self._load_meta(slicer)
        versions = meta.get("versions", [])
        if version not in versions:
            versions.append(version)
        meta["versions"] = versions
        meta["last_ingested"] = version
        meta.setdefault("snapshots", {}).setdefault(version, {})[snapshot_scope] = (
            snapshot_hash
        )

        meta_path = self.root / slicer.value / "_meta.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _snapshot_hash(self, slicer: SlicerType, profiles: list[ParsedProfile]) -> str:
        """Hash normalized parser output before mutating the persistent store."""
        payload = sorted(
            (
                self._profile_key(
                    slicer,
                    profile.profile_type,
                    profile.vendor,
                    profile.name,
                    profile.storage_key,
                ),
                profile.model_dump(mode="json", exclude={"source_path"}),
            )
            for profile in profiles
        )
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _extract_renamed_from(settings: dict) -> list[str]:
        """Extract the old profile names from a renamed_from field.

        OrcaSlicer and BambuStudio write one string listing the old names
        separated by ``;``, each optionally quoted C-style:
        ``"renamed_from": "Old Name;Older Name"``.  A dict form
        (``{"version": "old name"}``) is accepted too.

        Returns the old names in the order given, or an empty list.
        """
        rf = settings.get("renamed_from")
        if isinstance(rf, dict):
            rf = ";".join(value for value in rf.values() if isinstance(value, str))
        if not isinstance(rf, str):
            return []
        names = []
        for part in rf.split(";"):
            name = part.strip()
            if len(name) >= 2 and name[0] == name[-1] == '"':
                name = name[1:-1].replace('\\"', '"').replace("\\\\", "\\")
            if name and name not in names:
                names.append(name)
        return names

    def _replaced_profile(
        self,
        parsed: ParsedProfile,
        replaceable: dict[tuple[str, str], list[StoredProfile]],
        complete_release: bool,
    ) -> StoredProfile | None:
        """The stored profile a new profile replaces, if any, removed from ``replaceable``.

        A profile replaces one this release no longer has when it names it in
        ``renamed_from`` (same vendor first, then any vendor), or, in a complete
        release, when it has the same name and type and only another vendor had
        it (a move, such as OrcaSlicer 2.5 moving BBL filaments to the
        OrcaFilamentLibrary).
        """
        profile_type = parsed.profile_type.value

        def take(name: str, same_vendor: bool) -> StoredProfile | None:
            candidates = [
                candidate
                for candidate in replaceable.get((profile_type, name), [])
                if (candidate.vendor == parsed.vendor) == same_vendor
            ]
            if len(candidates) != 1:
                return None
            replaceable[(profile_type, name)].remove(candidates[0])
            return candidates[0]

        old_names = self._extract_renamed_from(parsed.settings)
        for same_vendor in (True, False):
            for old_name in old_names:
                found = take(old_name, same_vendor)
                if found is not None:
                    return found
        if complete_release:
            return take(parsed.name, False)
        return None

    def _merge_rename(
        self, new_profile: StoredProfile, old_profile: StoredProfile, version: str
    ) -> None:
        """Merge version history from an old (renamed or moved) profile into a new one.

        Prepends the old profile's versioned settings so that the new profile
        carries the full history, and tombstones at ``version`` the settings the
        new profile no longer has.  Updates first_seen and, for a rename, sets
        renamed_from.
        """
        new_profile.first_seen = old_profile.first_seen
        if old_profile.name != new_profile.name:
            new_profile.renamed_from = old_profile.name
        else:
            new_profile.renamed_from = old_profile.renamed_from

        for key, old_versions in old_profile.settings.items():
            if key in new_profile.settings:
                # Prepend old versions before new versions
                merged = dict(old_versions)
                merged.update(new_profile.settings[key])
                new_profile.settings[key] = merged
            else:
                merged = dict(old_versions)
                if list(merged.values())[-1] is not None:
                    merged[version] = None
                new_profile.settings[key] = merged

    @staticmethod
    def _tombstones_at(meta: dict, version: str) -> bool:
        """Whether removals may be recorded at ``version``.

        Only a stable release that is not older than any stable release already
        ingested: tombstoning an older one would sort after newer values in the
        insertion-ordered history.
        """
        from .versions import is_prerelease

        if version.startswith("nightly") or is_prerelease(version):
            return False
        target = _version_key(version)
        return all(
            _version_key(known) <= target
            for known in meta.get("versions", [])
            if not known.startswith("nightly") and not is_prerelease(known)
        )

    @staticmethod
    def _tombstone(stored: StoredProfile, version: str) -> bool:
        """Set every live setting of a profile to ``None`` at ``version``."""
        if _version_key(stored.first_seen) > _version_key(version):
            return False
        tombstoned = False
        for history in stored.settings.values():
            if history and list(history.values())[-1] is not None:
                history[version] = None
                tombstoned = True
        if tombstoned:
            stored.last_seen = version
        return tombstoned

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
        sanitized = sanitized.strip("_. ")
        if not sanitized:
            return "profile"

        stem_bytes = sanitized.encode("utf-8")
        if len(stem_bytes) <= ProfileStore.MAX_FILENAME_STEM_BYTES:
            return sanitized

        suffix = hashlib.sha256(stem_bytes).hexdigest()[
            : ProfileStore.FILENAME_HASH_LENGTH
        ]
        separator = "_"
        max_prefix_bytes = (
            ProfileStore.MAX_FILENAME_STEM_BYTES
            - len(separator.encode("utf-8"))
            - len(suffix.encode("utf-8"))
        )
        prefix = stem_bytes[:max_prefix_bytes].decode("utf-8", errors="ignore")
        prefix = prefix.strip("_. ") or "profile"
        return f"{prefix}{separator}{suffix}"

    @staticmethod
    def _normalize(value) -> str:
        """Normalize a value for comparison."""
        if value is None:
            return ""
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
