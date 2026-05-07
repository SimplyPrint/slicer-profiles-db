"""
Content-addressed store for slicer profile assets (STL/SVG/PNG).

Resources are stored once under their SHA-256 hash. Profile settings may refer to
resources as ``sha256:{hash}``, and /out/resources.json maps those refs to
repo-relative files under profiles/{slicer}/_resources without duplicating assets
under /out.
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
            ):
                if self.get_path(hash_hex):
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


RESOURCE_EXTENSIONS = {".stl", ".svg", ".png"}
RESOURCE_SETTING_KEYS = {"bed_model", "bed_texture", "thumbnail", "hotend_model"}


def collect_resources(extracted_dir: Path, store: ResourceStore) -> dict[str, str]:
    """Walk extracted dir, store all resource files, return {filename: hash}."""
    resource_map: dict[str, str] = {}
    for f in extracted_dir.rglob("*"):
        if not f.is_file() or f.suffix.lower() not in RESOURCE_EXTENSIONS:
            continue
        hash_hex = store.store(f, relative_to=extracted_dir)
        resource_map[f.name] = hash_hex
    store.save_manifest()
    return resource_map


def rewrite_resource_refs(profiles: list, resource_map: dict[str, str]) -> None:
    """Rewrite bare filename resource references to sha256:{hash}."""
    for profile in profiles:
        for key in RESOURCE_SETTING_KEYS:
            if key not in profile.settings:
                continue
            value = profile.settings[key]
            if isinstance(value, str) and value and value in resource_map:
                profile.settings[key] = f"sha256:{resource_map[value]}"


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
            settings = data.get("settings", {})
            for key in RESOURCE_SETTING_KEYS:
                if key not in settings:
                    continue
                versions = settings[key]
                for value in versions.values():
                    if isinstance(value, str) and value.startswith("sha256:"):
                        hashes.add(value[7:])
        except Exception:
            continue
    return hashes
