"""
Content-addressed store for slicer profile binary resource files.

Resources are stored once under their SHA-256 hash. Profile settings may refer to
resources as ``sha256:{hash}``, and /out/resources.json maps those refs to
repo-relative files under profiles/{slicer}/_resources without duplicating assets
under /out.
"""

import hashlib
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ResourceStore:
    """Content-addressed store for binary resource files."""

    def __init__(self, resources_dir: Path):
        self.root = resources_dir
        self.root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.root / "_manifest.json"
        self._manifest: dict[str, dict] = self._load_manifest()

    def store(self, file_path: Path, relative_to: Path | None = None) -> str:
        """Store a file and return its content hash (sha256 hex)."""
        content = file_path.read_bytes()
        hash_hex = hashlib.sha256(content).hexdigest()
        suffix = file_path.suffix.lower()
        dest = self.root / f"{hash_hex}{suffix}"
        if not dest.exists():
            dest.write_bytes(content)

        source_path = file_path.name
        if relative_to is not None:
            try:
                source_path = file_path.relative_to(relative_to).as_posix()
            except ValueError:
                source_path = file_path.name

        self._manifest[hash_hex] = {
            "filename": file_path.name,
            "source_path": source_path,
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
        """Get path to a stored resource by hash."""
        entry = self._manifest.get(hash_hex)
        if entry is None:
            return None
        suffix = f".{entry['type']}" if entry.get("type") else ""
        path = self.root / f"{hash_hex}{suffix}"
        return path if path.exists() else None

    def resolve_filename(self, hash_hex: str) -> str | None:
        """Get the original filename from a hash."""
        entry = self._manifest.get(hash_hex)
        return entry["filename"] if entry else None

    def find_hashes_by_filename(self, filename: str) -> list[str]:
        """Find resource hashes by original filename, case-insensitively."""
        filename_lower = filename.lower()
        matches: list[str] = []
        for hash_hex, entry in self._manifest.items():
            if not _is_sha256_hex(hash_hex):
                continue
            manifest_filename = entry.get("filename", "")
            if (
                manifest_filename == filename
                or manifest_filename.lower() == filename_lower
            ) and self.get_path(hash_hex):
                matches.append(hash_hex)
        return sorted(matches)

    def find_by_filename(self, filename: str) -> list[Path]:
        """Find stored resource paths by original filename, case-insensitively."""
        paths = []
        for hash_hex in self.find_hashes_by_filename(filename):
            path = self.get_path(hash_hex)
            if path:
                paths.append(path)
        return paths

    def gc(self, referenced_hashes: set[str]) -> list[str]:
        """Remove resources not in referenced_hashes. Returns removed hashes."""
        removed = []
        for hash_hex in list(self._manifest):
            if hash_hex not in referenced_hashes:
                path = self.get_path(hash_hex)
                if path and path.exists():
                    path.unlink()
                del self._manifest[hash_hex]
                removed.append(hash_hex)

        expected_paths = set()
        for hash_hex, entry in self._manifest.items():
            suffix = f".{entry['type']}" if entry.get("type") else ""
            expected_paths.add((self.root / f"{hash_hex}{suffix}").resolve())

        for path in self.root.rglob("*"):
            if not path.is_file() or path == self._manifest_path:
                continue
            if path.resolve() not in expected_paths:
                path.unlink()

        for path in sorted(
            self.root.rglob("*"), key=lambda item: len(item.parts), reverse=True
        ):
            if path.is_dir():
                try:
                    path.rmdir()
                except OSError:
                    pass

        if removed:
            self.save_manifest()
        return removed

    def _load_manifest(self) -> dict[str, dict]:
        if self._manifest_path.exists():
            return json.loads(self._manifest_path.read_text(encoding="utf-8"))
        return {}


def _is_sha256_hex(value: str) -> bool:
    return len(value) == 64 and all(c in "0123456789abcdef" for c in value)


RESOURCE_SUFFIXES = frozenset(
    {
        ".3mf",
        ".jpeg",
        ".jpg",
        ".obj",
        ".png",
        ".stl",
        ".svg",
    }
)
RESOURCE_SETTING_KEYS = {"bed_model", "bed_texture", "thumbnail", "hotend_model"}


def collect_resources(extracted_dir: Path, store: ResourceStore) -> dict[str, str]:
    """Walk extracted dir and content-address supported resource files.

    Both the basename and the extraction-relative path are indexed.  Current
    slicer metadata generally uses basenames, while accepting relative paths
    keeps the resource layer independent from a particular slicer's layout.
    """
    resource_map: dict[str, str] = {}
    for file_path in sorted(extracted_dir.rglob("*")):
        if not file_path.is_file() or file_path.suffix.lower() not in RESOURCE_SUFFIXES:
            continue
        hash_hex = store.store(file_path, relative_to=extracted_dir)
        resource_map[file_path.name] = hash_hex
        resource_map[file_path.relative_to(extracted_dir).as_posix()] = hash_hex
    store.save_manifest()
    return resource_map


def _rewrite_resource_value(value, resource_map: dict[str, str]):
    """Rewrite resource references recursively without interpreting schemas."""
    if isinstance(value, str):
        hash_hex = resource_map.get(value)
        return f"sha256:{hash_hex}" if hash_hex else value
    if isinstance(value, dict):
        return {
            key: _rewrite_resource_value(item, resource_map)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_rewrite_resource_value(item, resource_map) for item in value]
    if isinstance(value, tuple):
        return tuple(_rewrite_resource_value(item, resource_map) for item in value)
    return value


def rewrite_resource_refs(profiles: list, resource_map: dict[str, str]) -> None:
    """Rewrite resource references in profile data to ``sha256:{hash}``.

    Mutates profiles in place.  Context is included because typed assets and
    other non-engine metadata intentionally live outside runtime settings.
    """
    for profile in profiles:
        profile.settings = _rewrite_resource_value(profile.settings, resource_map)
        profile.context = _rewrite_resource_value(profile.context, resource_map)


def _collect_hash_refs(value, hashes: set[str]) -> None:
    """Collect content-addressed references recursively from JSON data."""
    if isinstance(value, str):
        if value.startswith("sha256:"):
            hashes.add(value[7:])
        return
    if isinstance(value, dict):
        for item in value.values():
            _collect_hash_refs(item, hashes)
        return
    if isinstance(value, list):
        for item in value:
            _collect_hash_refs(item, hashes)


def collect_referenced_hashes(store_root: Path, slicer_value: str) -> set[str]:
    """Scan stored profiles for a slicer and return referenced resource hashes."""
    hashes = set()
    slicer_dir = store_root / slicer_value
    for json_file in slicer_dir.rglob("*.json"):
        if any(
            part.startswith("_") for part in json_file.relative_to(slicer_dir).parts
        ):
            continue
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            _collect_hash_refs(data, hashes)
        except (OSError, UnicodeError, ValueError) as exc:
            logger.debug("Skipping unreadable profile %s: %s", json_file, exc)
            continue
    return hashes
