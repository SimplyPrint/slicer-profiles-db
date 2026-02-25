import json
from pathlib import Path
from typing import Iterator

from .base import BaseParser
from ..models import SlicerType, ProfileType, ParsedProfile


class PrusaSlicerParser(BaseParser):
    """
    Parser for PrusaSlicer profiles.

    PrusaSlicer profiles are JSON files (already converted from INI bundles
    by squash.py or load_profiles.py). Values are strings rather than arrays.
    Handles all profile types: filament, machine, machine_model, print.
    """

    slicer_type = SlicerType.PRUSASLICER

    def parse_file(self, path: Path) -> ParsedProfile:
        data = json.loads(path.read_text(encoding="utf-8"))
        vendor = path.parent.name

        # Determine profile type from data content
        profile_type = self._detect_profile_type(data)

        # Name extraction per type
        name = self._extract_name(data, profile_type, path)

        filament_type = data.get("filament_type") if profile_type == ProfileType.FILAMENT else None

        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=profile_type,
            name=name,
            vendor=vendor,
            settings=data,
            source_path=path,
            filament_type=filament_type,
            filament_settings_id=data.get("filament_settings_id"),
        )

    def _detect_profile_type(self, data: dict) -> ProfileType:
        """Detect profile type from data keys."""
        # machine_model: has 'variants' key (PrusaSlicer printer model definition)
        if "variants" in data:
            return ProfileType.MACHINE_MODEL

        # machine: has printer_settings_id but no variants
        if "printer_settings_id" in data and "filament_settings_id" not in data:
            return ProfileType.MACHINE

        # print: has print_settings_id
        if "print_settings_id" in data and "filament_settings_id" not in data:
            return ProfileType.PRINT

        # filament: has filament_settings_id or filament_type, or default
        return ProfileType.FILAMENT

    def _extract_name(self, data: dict, profile_type: ProfileType, path: Path) -> str:
        """Extract the profile name based on type."""
        if profile_type == ProfileType.FILAMENT:
            return data.get("filament_settings_id", data.get("name", path.stem))
        elif profile_type == ProfileType.MACHINE:
            return data.get("printer_settings_id", data.get("name", path.stem))
        elif profile_type == ProfileType.MACHINE_MODEL:
            return data.get("name", path.stem)
        elif profile_type == ProfileType.PRINT:
            return data.get("print_settings_id", data.get("name", path.stem))
        return path.stem

    def _glob_profiles(self, vendor_dir: Path) -> Iterator[Path]:
        yield from sorted(vendor_dir.rglob("*.json"))
