import json
from pathlib import Path
from typing import Any, Iterator

from .base import BaseParser
from ..models import ProfileType, ParsedProfile


def _first(value: Any) -> str | None:
    """Extract the first element if value is a list, otherwise return as-is."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


# Profile type subdirectory names used by BBS/Orca.
# Note: machine_model profiles also live in the "machine/" directory
# alongside regular machine profiles, distinguished by "type": "machine_model".
_TYPE_DIR_NAMES = {"filament", "machine", "process"}


class Slic3rJsonParser(BaseParser):
    """Shared parser for BambuStudio and OrcaSlicer JSON profiles."""

    def parse_file(self, path: Path) -> ParsedProfile:
        data = json.loads(path.read_text(encoding="utf-8"))

        raw_type = data.get("type", "filament")
        # Map "process" to "print" for our unified ProfileType
        if raw_type == "process":
            profile_type = ProfileType.PRINT
        else:
            profile_type = ProfileType(raw_type)

        # Name extraction with fallbacks per type
        name = data.get("name")
        if not name:
            if profile_type == ProfileType.FILAMENT:
                name = data.get("filament_settings_id", path.stem)
            elif profile_type == ProfileType.MACHINE:
                name = data.get("printer_settings_id", path.stem)
            elif profile_type == ProfileType.PRINT:
                name = data.get("print_settings_id", path.stem)
            else:
                name = path.stem

        # Vendor detection: walk up from the file to find the vendor directory.
        # Profiles live in structures like:
        #   {vendor}/{type}/file.json           → vendor = {vendor}
        #   {vendor}/{type}/{subdir}/file.json  → vendor = {vendor}
        #   {vendor}/file.json                  → vendor = {vendor}
        # We find the type dir in the path and take its parent as vendor.
        vendor = path.parent.name
        for parent in path.parents:
            if parent.name in _TYPE_DIR_NAMES:
                vendor = parent.parent.name
                break

        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=profile_type,
            name=name,
            vendor=vendor,
            settings=data,
            source_path=path,
            filament_id=_first(data.get("filament_id")),
            setting_id=_first(data.get("setting_id")),
            filament_type=_first(data.get("filament_type")),
        )

    def _glob_profiles(self, vendor_dir: Path) -> Iterator[Path]:
        yield from sorted(vendor_dir.rglob("*.json"))
