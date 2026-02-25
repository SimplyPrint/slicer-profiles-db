import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

from .base import BaseParser
from ..models import SlicerType, ProfileType, ParsedProfile


# Cura fdm_material XML namespace
NS = {"m": "http://www.ultimaker.com/material"}


class CuraParser(BaseParser):
    """Parser for Cura fdm_material XML files and machine definition JSON files."""

    slicer_type = SlicerType.CURA

    def parse_file(self, path: Path) -> ParsedProfile:
        if path.suffix == ".json" or path.name.endswith(".def.json"):
            return self._parse_machine_definition(path)
        return self._parse_fdm_material(path)

    def _parse_fdm_material(self, path: Path) -> ParsedProfile:
        """Parse a Cura fdm_material XML file."""
        tree = ET.parse(path)
        root = tree.getroot()

        # Extract metadata
        metadata = root.find("m:metadata", NS)
        name_elem = metadata.find("m:name", NS) if metadata is not None else None

        brand = ""
        material = ""
        color = ""
        label = ""
        if name_elem is not None:
            brand = (name_elem.findtext("m:brand", "", NS) or "").strip()
            material = (name_elem.findtext("m:material", "", NS) or "").strip()
            color = (name_elem.findtext("m:color", "", NS) or "").strip()
            label = (name_elem.findtext("m:label", "", NS) or "").strip()

        profile_name = label or f"{brand} {material} {color}".strip()
        guid = (metadata.findtext("m:GUID", "", NS) or "").strip() if metadata is not None else ""
        color_code = (
            (metadata.findtext("m:color_code", "", NS) or "").strip()
            if metadata is not None
            else ""
        )
        description = (
            (metadata.findtext("m:description", "", NS) or "").strip()
            if metadata is not None
            else ""
        )

        # Extract properties
        properties = root.find("m:properties", NS)
        diameter = ""
        density = ""
        weight = ""
        if properties is not None:
            diameter = (properties.findtext("m:diameter", "", NS) or "").strip()
            density = (properties.findtext("m:density", "", NS) or "").strip()
            weight = (properties.findtext("m:weight", "", NS) or "").strip()

        # Extract settings
        settings: dict = {
            "brand": brand,
            "material": material,
            "color": color,
            "label": label,
            "GUID": guid,
            "color_code": color_code,
            "description": description,
            "diameter": diameter,
            "density": density,
            "weight": weight,
        }

        settings_elem = root.find("m:settings", NS)
        if settings_elem is not None:
            for setting in settings_elem.findall("m:setting", NS):
                key = setting.get("key", "")
                if key:
                    settings[key] = (setting.text or "").strip()

            # Also parse machine-specific settings
            for machine in settings_elem.findall("m:machine", NS):
                machine_id = machine.findtext("m:machine_identifier", "", NS)
                for setting in machine.findall("m:setting", NS):
                    key = setting.get("key", "")
                    if key:
                        machine_key = f"machine:{machine_id}:{key}" if machine_id else key
                        settings[machine_key] = (setting.text or "").strip()

        vendor = brand or "Generic"

        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.FILAMENT,
            name=profile_name,
            vendor=vendor,
            settings=settings,
            source_path=path,
            filament_id=guid,
            filament_type=material,
        )

    def _parse_machine_definition(self, path: Path) -> ParsedProfile:
        """Parse a Cura machine definition JSON file (*.def.json)."""
        data = json.loads(path.read_text(encoding="utf-8"))

        name = data.get("name", path.stem)
        metadata = data.get("metadata", {})
        vendor = metadata.get("manufacturer", "Unknown")

        # Flatten overrides into settings
        settings: dict = {
            "name": name,
            "version": data.get("version"),
            "inherits": data.get("inherits"),
        }
        settings.update(metadata)

        overrides = data.get("overrides", {})
        for key, override_data in overrides.items():
            if isinstance(override_data, dict):
                if "default_value" in override_data:
                    settings[key] = override_data["default_value"]
                elif "value" in override_data:
                    settings[key] = override_data["value"]
            else:
                settings[key] = override_data

        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.MACHINE_MODEL,
            name=name,
            vendor=vendor,
            settings=settings,
            source_path=path,
        )

    def parse_directory(self, directory: Path, profile_type_filter=None) -> Iterator[ParsedProfile]:
        """Cura profiles may be flat files or in vendor subdirectories."""
        seen_paths: set[Path] = set()

        # Handle flat directory of fdm_material files
        has_flat_materials = any(
            p.suffix == ".fdm_material" or p.name.endswith(".xml.fdm_material")
            for p in directory.iterdir()
            if p.is_file()
        )

        if has_flat_materials:
            for path in sorted(directory.iterdir()):
                if path.suffix == ".fdm_material" or path.name.endswith(".xml.fdm_material"):
                    if path in seen_paths:
                        continue
                    seen_paths.add(path)
                    try:
                        profile = self.parse_file(path)
                        if profile_type_filter and profile.profile_type not in profile_type_filter:
                            continue
                        yield profile
                    except Exception:
                        continue

        # Handle def.json files (may be in subdirectories or flat)
        # Collect all def.json profiles first for inheritance resolution
        def_json_profiles: list[ParsedProfile] = []
        for path in sorted(directory.rglob("*.def.json")):
            if path in seen_paths:
                continue
            seen_paths.add(path)
            try:
                profile = self.parse_file(path)
                def_json_profiles.append(profile)
            except Exception:
                continue

        # Build a lookup by stem for inheritance resolution
        stem_lookup: dict[str, ParsedProfile] = {}
        for p in def_json_profiles:
            stem = p.source_path.stem
            # .def.json files have stem like "ultimaker_s5" from "ultimaker_s5.def.json"
            if stem.endswith(".def"):
                stem = stem[:-4]
            stem_lookup[stem] = p

        # Resolve inherited manufacturer for profiles with Unknown vendor
        for p in def_json_profiles:
            if p.vendor == "Unknown":
                inherits = p.settings.get("inherits")
                visited: set[str] = set()
                while inherits and inherits not in visited:
                    visited.add(inherits)
                    parent = stem_lookup.get(inherits)
                    if parent is None:
                        break
                    if parent.vendor != "Unknown":
                        p.vendor = parent.vendor
                        break
                    inherits = parent.settings.get("inherits")

        # Yield def.json profiles, filtering invisible abstract definitions
        for p in def_json_profiles:
            if p.settings.get("visible") is False:
                continue
            if profile_type_filter and p.profile_type not in profile_type_filter:
                continue
            yield p

        # Also handle vendor subdirectory structure
        for vendor_dir in sorted(directory.iterdir()):
            if not vendor_dir.is_dir():
                continue
            for path in self._glob_profiles(vendor_dir):
                if path in seen_paths:
                    continue
                seen_paths.add(path)
                try:
                    profile = self.parse_file(path)
                    if profile_type_filter and profile.profile_type not in profile_type_filter:
                        continue
                    yield profile
                except Exception:
                    continue

    def _glob_profiles(self, vendor_dir: Path) -> Iterator[Path]:
        yield from sorted(vendor_dir.rglob("*.fdm_material"))
        yield from sorted(vendor_dir.rglob("*.def.json"))
