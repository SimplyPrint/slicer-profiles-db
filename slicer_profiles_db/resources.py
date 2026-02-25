"""
Content-addressed store for binary resource files (STL/SVG/PNG).

Resource files referenced by slicer profiles (bed models, bed textures,
thumbnails) are stored once under their SHA-256 hash, with a manifest
mapping hashes back to original filenames.
"""

import hashlib
import json
from pathlib import Path


class ResourceStore:
    """Content-addressed store for binary resource files."""

    def __init__(self, resources_dir: Path):
        self.root = resources_dir
        self.root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.root / "_manifest.json"
        self._manifest: dict[str, dict] = self._load_manifest()

    def store(self, file_path: Path) -> str:
        """Store a file and return its content hash (sha256 hex)."""
        content = file_path.read_bytes()
        hash_hex = hashlib.sha256(content).hexdigest()
        dest = self.root / f"{hash_hex}{file_path.suffix.lower()}"
        if not dest.exists():
            dest.write_bytes(content)
        self._manifest[hash_hex] = {
            "filename": file_path.name,
            "size": len(content),
            "type": file_path.suffix.lstrip(".").lower(),
        }
        return hash_hex

    def save_manifest(self) -> None:
        """Write the manifest to disk."""
        self._manifest_path.write_text(
            json.dumps(self._manifest, indent=2, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )

    def get_path(self, hash_hex: str) -> Path | None:
        """Get path to a stored resource by its hash."""
        entry = self._manifest.get(hash_hex)
        if entry is None:
            return None
        suffix = f".{entry['type']}"
        path = self.root / f"{hash_hex}{suffix}"
        return path if path.exists() else None

    def resolve_filename(self, hash_hex: str) -> str | None:
        """Get the original filename from a hash."""
        entry = self._manifest.get(hash_hex)
        return entry["filename"] if entry else None

    def gc(self, referenced_hashes: set[str]) -> list[str]:
        """Remove resources not in referenced_hashes. Returns list of removed hashes."""
        removed = []
        for hash_hex in list(self._manifest):
            if hash_hex not in referenced_hashes:
                path = self.get_path(hash_hex)
                if path and path.exists():
                    path.unlink()
                del self._manifest[hash_hex]
                removed.append(hash_hex)
        if removed:
            self.save_manifest()
        return removed

    def _load_manifest(self) -> dict[str, dict]:
        if self._manifest_path.exists():
            return json.loads(self._manifest_path.read_text(encoding="utf-8"))
        return {}


RESOURCE_EXTENSIONS = {"*.stl", "*.svg", "*.png"}
RESOURCE_SETTING_KEYS = {"bed_model", "bed_texture", "thumbnail", "hotend_model"}


def collect_resources(extracted_dir: Path, store: ResourceStore) -> dict[str, str]:
    """Walk extracted dir, store all resource files, return {filename: hash} map."""
    resource_map: dict[str, str] = {}
    for pattern in RESOURCE_EXTENSIONS:
        for f in extracted_dir.rglob(pattern):
            hash_hex = store.store(f)
            resource_map[f.name] = hash_hex
    store.save_manifest()
    return resource_map


def rewrite_resource_refs(
    profiles: list, resource_map: dict[str, str]
) -> None:
    """Rewrite bare filename references in profile settings to sha256:{hash}.

    Mutates profiles in place. Handles both flat string values and
    versioned dicts (though ParsedProfile.settings should be flat).
    """
    for profile in profiles:
        for key in RESOURCE_SETTING_KEYS:
            if key not in profile.settings:
                continue
            value = profile.settings[key]
            if isinstance(value, str) and value and value in resource_map:
                profile.settings[key] = f"sha256:{resource_map[value]}"


def collect_referenced_hashes(store_root: Path, slicer_value: str) -> set[str]:
    """Scan all stored profiles for a slicer and return the set of sha256 hashes referenced."""
    hashes = set()
    slicer_dir = store_root / slicer_value
    for json_file in slicer_dir.rglob("*.json"):
        # Skip _resources/ and _meta.json
        if any(part.startswith("_") for part in json_file.relative_to(slicer_dir).parts):
            continue
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            settings = data.get("settings", {})
            for key in RESOURCE_SETTING_KEYS:
                if key not in settings:
                    continue
                versions = settings[key]  # {"ver": "sha256:abc..."}
                for value in versions.values():
                    if isinstance(value, str) and value.startswith("sha256:"):
                        hashes.add(value[7:])  # strip "sha256:" prefix
        except Exception:
            continue
    return hashes
