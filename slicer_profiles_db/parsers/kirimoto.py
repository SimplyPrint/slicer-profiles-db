"""Parse GridSpace Kiri:Moto device definitions into runtime profiles."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .base import BaseParser
from ..models import ParsedProfile, ProfileType, SlicerType


_FILAMENT_TYPES = (
    "TPU-AMS",
    "PETG-CF",
    "PLA-CF",
    "PLA-GF",
    "PA12-CF",
    "PA6-CF",
    "PA-CF",
    "PA-GF",
    "PC-ABS",
    "PCTG",
    "PETG",
    "PLA+",
    "PLA",
    "ABS",
    "ASA",
    "TPU",
    "TPE",
    "Nylon",
    "PA",
    "PC",
    "PET",
    "HIPS",
    "PVA",
    "PMMA",
    "PP",
)


def _infer_filament_type(process: dict[str, Any], process_name: str) -> str | None:
    explicit = process.get("filament_type")
    if isinstance(explicit, list):
        explicit = explicit[0] if explicit else None
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()

    normalized_name = re.sub(r"[^A-Za-z0-9+]+", " ", process_name).upper()
    padded_name = f" {normalized_name} "
    for material in _FILAMENT_TYPES:
        normalized_material = re.sub(r"[^A-Za-z0-9+]+", " ", material).upper()
        if f" {normalized_material} " in padded_name:
            return material
    return None


def _array(value: Any) -> list:
    if value in (None, ""):
        return []
    return value if isinstance(value, list) else [value]


def _split_identity(path: Path) -> tuple[str, str, str]:
    parts = path.stem.split(".")
    vendor = parts[0]
    model = " ".join(parts[1:]).replace("_", " ") if len(parts) > 1 else path.stem
    display_name = f"{vendor} {model}".strip()
    return vendor, model, display_name


def _normalize_device(source: dict[str, Any], name: str) -> dict[str, Any]:
    """Mirror Kiri's ``device_from_code`` conversion for Node-side profiles."""
    if isinstance(source.get("code"), dict):
        device = dict(source["code"])
        device.setdefault("deviceName", name)
        return device
    if isinstance(source.get("internal"), (int, float)) and source["internal"] >= 0:
        device = dict(source)
        device.setdefault("deviceName", name)
        return device

    command = source.get("cmd") if isinstance(source.get("cmd"), dict) else {}
    settings = (
        source.get("settings") if isinstance(source.get("settings"), dict) else {}
    )

    def value(key: str, default: Any, *fallbacks: str) -> Any:
        for candidate in (key, *fallbacks):
            if candidate in settings:
                return settings[candidate]
        return default

    raw_extruders = source.get("extruders")
    extruders = []
    if isinstance(raw_extruders, list):
        for raw in raw_extruders:
            if not isinstance(raw, dict):
                continue
            extruders.append(
                {
                    "extFilament": raw.get("filament", 1.75),
                    "extNozzle": raw.get("nozzle", 0.4),
                    "extSelect": ["T0"],
                    "extDeselect": [],
                    "extOffsetX": raw.get("offset_x", 0),
                    "extOffsetY": raw.get("offset_y", 0),
                }
            )
    if not extruders:
        extruders = [
            {
                "extFilament": value("filament_diameter", 1.75),
                "extNozzle": value("nozzle_size", 0.4),
                "extSelect": ["T0"],
                "extDeselect": [],
                "extOffsetX": 0,
                "extOffsetY": 0,
            }
        ]

    return {
        "noclone": source.get("no_clone", False),
        "mode": source.get("mode", "FDM"),
        "deviceName": name,
        "internal": 0,
        "imageURL": "",
        "imageScale": value("image_scale", 0.75),
        "imageAnchor": value("image_anchor", 0),
        "bedHeight": value("bed_height", 2.5),
        "bedWidth": value("bed_width", 300),
        "bedDepth": value("bed_depth", 175),
        "bedRound": value("bed_circle", False),
        "bedBelt": value("bed_belt", False),
        "maxHeight": value("build_height", value("max_height", 150)),
        "originCenter": value("origin_center", False),
        "extrudeAbs": value("extrude_abs", False),
        "spindleMax": value("spindle_max", 0),
        "gcodeFan": _array(command.get("fan_power", source.get("fan_power"))),
        "gcodeFeature": _array(command.get("feature", source.get("feature"))),
        "gcodeTrack": _array(command.get("progress", source.get("progress"))),
        "gcodeLayer": _array(command.get("layer", source.get("layer"))),
        "gcodePre": _array(source.get("pre")),
        "gcodePost": _array(source.get("post")),
        "gcodeProc": source.get("proc", ""),
        "gcodeDwell": _array(source.get("dwell")),
        "gcodeSpindle": _array(source.get("spindle", command.get("spindle"))),
        "gcodeResetA": _array(source.get("reseta", command.get("reseta"))),
        "gcodeChange": _array(source.get("tool-change")),
        "gcodeFExt": source.get("file-ext", "gcode"),
        "gcodeSpace": source.get("token-space", True),
        "gcodeStrip": source.get("strip-comments", False),
        "gcodeLaserOn": _array(source.get("laser-on")),
        "gcodeLaserOff": _array(source.get("laser-off")),
        "extruders": extruders,
    }


class KiriMotoParser(BaseParser):
    slicer_type = SlicerType.KIRIMOTO

    def parse_file(self, path: Path) -> ParsedProfile:
        """Return the concrete machine role for one device file."""
        source = json.loads(path.read_text(encoding="utf-8"))
        vendor, _model, display_name = _split_identity(path)
        device = _normalize_device(source, f"{display_name} 0.4 nozzle")
        nozzle = device["extruders"][0]["extNozzle"]
        device.update(
            {
                "type": "machine",
                "name": f"{display_name} {nozzle:g} nozzle",
                "printer_model": display_name,
                "nozzle_diameter": [nozzle],
                "extNozzle": nozzle,
            }
        )
        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=ProfileType.MACHINE,
            name=device["name"],
            vendor=vendor,
            settings=device,
            source_path=path,
            native_id=path.stem,
            context={"printer_model": display_name},
        )

    def parse_directory(
        self,
        directory: Path,
        profile_type_filter: list[ProfileType] | None = None,
        resource_version: str | None = None,
    ) -> Iterator[ParsedProfile]:
        requested = set(profile_type_filter or ProfileType)
        for path in sorted(directory.rglob("*.json")):
            try:
                source = json.loads(path.read_text(encoding="utf-8"))
                machine = self.parse_file(path)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue

            vendor, _model, display_name = _split_identity(path)
            device = machine.settings
            nozzle = device["extruders"][0]["extNozzle"]
            context: dict[str, Any] = {"display_name": display_name}
            processes = source.get("profiles")
            if isinstance(processes, list) and processes:
                first_name = processes[0].get("processName")
                if isinstance(first_name, str) and first_name:
                    context["selection_defaults"] = {
                        "process_profile": {"match": {"name": first_name}},
                        "filament_profile": {"match": {"name": first_name}},
                    }

            if ProfileType.MACHINE_MODEL in requested:
                yield ParsedProfile(
                    slicer=self.slicer_type,
                    profile_type=ProfileType.MACHINE_MODEL,
                    name=display_name,
                    vendor=vendor,
                    settings={
                        "type": "machine_model",
                        "name": display_name,
                        "printer_model": display_name,
                        "nozzle_diameter": [nozzle],
                        "variants": [str(nozzle)],
                        "bed_belt": device.get("bedBelt", False),
                        "bed_width": device.get("bedWidth"),
                        "bed_depth": device.get("bedDepth"),
                        "build_height": device.get("maxHeight"),
                    },
                    source_path=path,
                    native_id=path.stem,
                    context=context,
                )
            if ProfileType.MACHINE in requested:
                yield machine

            if not isinstance(processes, list):
                continue
            for index, raw_process in enumerate(processes):
                if not isinstance(raw_process, dict):
                    continue
                process = dict(raw_process)
                process_name = str(
                    process.get("processName") or f"{display_name} process {index + 1}"
                )
                process.update(
                    {
                        "type": "process",
                        "name": process_name,
                        "compatible_printers": [machine.name],
                    }
                )
                if ProfileType.PRINT in requested:
                    yield ParsedProfile(
                        slicer=self.slicer_type,
                        profile_type=ProfileType.PRINT,
                        name=process_name,
                        vendor=vendor,
                        settings=process,
                        source_path=path,
                        native_id=f"{path.stem}:process:{index}",
                        context={"printer_model": display_name},
                    )

                filament_type = _infer_filament_type(raw_process, process_name)
                if ProfileType.FILAMENT in requested and filament_type:
                    filament = {
                        key: raw_process[key]
                        for key in ("outputTemp", "outputBedTemp")
                        if key in raw_process
                    }
                    filament.update(
                        {
                            "type": "filament",
                            "name": process_name,
                            "filament_type": filament_type,
                            "compatible_printers": [machine.name],
                        }
                    )
                    yield ParsedProfile(
                        slicer=self.slicer_type,
                        profile_type=ProfileType.FILAMENT,
                        name=process_name,
                        vendor=vendor,
                        settings=filament,
                        source_path=path,
                        filament_type=filament_type,
                        native_id=f"{path.stem}:filament:{index}",
                        context={"printer_model": display_name},
                    )

    def _glob_profiles(self, vendor_dir: Path) -> Iterator[Path]:
        yield from sorted(vendor_dir.rglob("*.json"))
