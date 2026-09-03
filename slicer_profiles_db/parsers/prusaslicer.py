import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar

from ..models import ParsedProfile, ProfileType, SlicerType
from .base import BaseParser


class PrusaSlicerParser(BaseParser):
    """
    Parser for PrusaSlicer profiles.

    PrusaSlicer profiles are JSON files (already converted from INI bundles
    by squash.py or load_profiles.py). Values are strings rather than arrays.
    Handles all profile types: filament, machine, machine_model, print.
    """

    slicer_type = SlicerType.PRUSASLICER

    _BUNDLE_TYPES: ClassVar[dict[str, ProfileType]] = {
        "machine": ProfileType.MACHINE,
        "filament": ProfileType.FILAMENT,
    }

    def parse_directory(
        self,
        directory: Path,
        profile_type_filter: list[ProfileType] | None = None,
        resource_version: str | None = None,
    ) -> Iterator[ParsedProfile]:
        bundle = directory / "profile-bundle.json"
        if bundle.is_file():
            yield from self.parse_profile_bundle(bundle, profile_type_filter)
            return
        yield from super().parse_directory(
            directory,
            profile_type_filter=profile_type_filter,
            resource_version=resource_version,
        )

    def parse_profile_bundle(
        self,
        path: Path,
        profile_type_filter: list[ProfileType] | None = None,
    ) -> Iterator[ParsedProfile]:
        """Parse PrusaSlicer 3's evaluated, context-aware profile bundle."""
        bundle = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(bundle, dict) or bundle.get("schema_version") != 1:
            raise ValueError("unsupported PrusaSlicer profile bundle")
        if bundle.get("format") != "prusa-evaluated-profiles":
            raise ValueError("unsupported PrusaSlicer profile bundle")
        slicer_version = str(bundle.get("slicer_version", ""))
        records = {
            name: bundle.get(name, []) for name in (*self._BUNDLE_TYPES, "process")
        }
        if not profile_type_filter or ProfileType.MACHINE_MODEL in profile_type_filter:
            yield from self._machine_models(records["machine"], path, slicer_version)
        for collection, profile_type in self._BUNDLE_TYPES.items():
            if profile_type_filter and profile_type not in profile_type_filter:
                continue
            counts = Counter(
                str(record.get("native_id", "")) for record in records[collection]
            )
            for record in records[collection]:
                yield self._profile(
                    record,
                    collection,
                    profile_type,
                    path,
                    slicer_version,
                    counts[str(record.get("native_id", ""))] > 1,
                )

        if not profile_type_filter or ProfileType.PRINT in profile_type_filter:
            for record in records["process"]:
                yield from self._process_profiles(record, path, slicer_version)

    @staticmethod
    def _context(record: dict, slicer_version: str) -> dict:
        return {
            "format": "prusa-evaluated-profiles",
            "schema_version": 1,
            "slicer_version": slicer_version,
            "native_id": str(record["native_id"]),
            "root_id": record.get("root_id", ""),
        }

    def _profile(
        self,
        record: dict,
        collection: str,
        profile_type: ProfileType,
        path: Path,
        slicer_version: str,
        duplicate: bool,
    ) -> ParsedProfile:
        data = dict(record.get("data") or {})
        name = str(record["name"])
        data.setdefault(
            {
                ProfileType.MACHINE: "printer_settings_id",
                ProfileType.FILAMENT: "filament_settings_id",
            }[profile_type],
            name,
        )
        contexts = record.get("contexts", [])
        compatible = sorted(
            {item["machine_name"] for item in contexts if item.get("machine_name")}
        )
        if compatible:
            data.setdefault("compatible_printers", compatible)

        context = self._context(record, slicer_version)
        if contexts and isinstance(contexts[0].get("preset"), dict):
            context["preset"] = contexts[0]["preset"]
        if profile_type == ProfileType.MACHINE and contexts:
            context.update(
                {
                    key: contexts[0][key]
                    for key in (
                        "hardware_id",
                        "printer_model",
                        "printer_base_model",
                        "tool_count",
                    )
                    if key in contexts[0]
                }
            )
            data.setdefault("printer_model", contexts[0].get("printer_model"))

        storage_key = f"{collection}:{record['native_id']}"
        if duplicate:
            digest = hashlib.sha256(
                json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()[:12]
            storage_key += f":{digest}"
        return ParsedProfile(
            slicer=self.slicer_type,
            profile_type=profile_type,
            name=name,
            vendor=str(record["vendor_id"]),
            settings=data,
            source_path=path,
            native_id=str(record["native_id"]),
            storage_key=storage_key,
            filament_type=data.get("filament_type")
            if profile_type == ProfileType.FILAMENT
            else None,
            context={**context, "storage_key": storage_key},
        )

    def _process_profiles(
        self, record: dict, path: Path, slicer_version: str
    ) -> Iterator[ParsedProfile]:
        base = dict(record.get("data") or {})
        for machine in record.get("contexts", []) or [{}]:
            tools = machine.get("tool_processes", [])
            groups: dict[str, dict[int, dict]] = {}
            for tool in tools:
                if not isinstance(tool.get("data"), dict):
                    continue
                label = str(tool.get("name", "")).split("@", 1)[0].strip()
                groups.setdefault(label, {})[int(tool.get("tool", 0))] = tool
            if not groups:
                groups[str(record["name"])] = {}

            for label, by_tool in groups.items():
                tool_count = max(by_tool, default=-1) + 1
                tool_settings = {
                    key: [
                        by_tool.get(index, {}).get("data", {}).get(key)
                        for index in range(tool_count)
                    ]
                    for key in sorted(
                        {key for tool in by_tool.values() for key in tool["data"]}
                    )
                }
                effective = {
                    **base,
                    **(
                        tool_settings
                        if tool_count > 1
                        else {key: values[0] for key, values in tool_settings.items()}
                    ),
                }
                effective["print_settings_id"] = label
                machine_name = machine.get("machine_name")
                if isinstance(machine_name, str):
                    effective["compatible_printers"] = [machine_name]
                storage_key = ":".join(
                    filter(
                        None,
                        (
                            "process",
                            str(record["native_id"]),
                            str(machine.get("machine_id", "")),
                            label,
                        ),
                    )
                )
                context = {
                    **self._context(record, slicer_version),
                    "storage_key": storage_key,
                    "configuration": {
                        "print_settings": base,
                        "toolprint_settings": tool_settings,
                        "preset": {
                            "print": machine.get("preset", {}),
                            "tools": (
                                [
                                    by_tool[index].get("preset", {})
                                    for index in sorted(by_tool)
                                ]
                                if by_tool
                                else [tool.get("preset", {}) for tool in tools]
                            ),
                        },
                    },
                }
                yield ParsedProfile(
                    slicer=self.slicer_type,
                    profile_type=ProfileType.PRINT,
                    name=label,
                    vendor=str(record["vendor_id"]),
                    settings=effective,
                    source_path=path,
                    native_id=str(record["native_id"]),
                    storage_key=storage_key,
                    context=context,
                    setting_scopes={
                        key: "extruder.0" if key in tool_settings else "global"
                        for key in base.keys() | tool_settings.keys()
                    },
                )

    def _machine_models(
        self,
        records: list,
        source_path: Path,
        slicer_version: str,
    ) -> Iterator[ParsedProfile]:
        """Build mapping-friendly model/variant roles from evaluated machines."""
        models: dict[tuple[str, str], dict] = {}
        for record in records:
            contexts = record.get("contexts", [])
            context = contexts[0] if contexts else {}
            model_id = str(
                context.get("printer_model")
                or record.get("root_id")
                or record["native_id"]
            )
            vendor = str(record["vendor_id"])
            model = models.setdefault(
                (vendor, model_id),
                {
                    "name": self._machine_model_name(str(record["name"])),
                    "base_model": context.get("printer_base_model"),
                    "technology": str(record.get("technology", "")),
                    "variants": set(),
                },
            )
            name = self._machine_model_name(str(record["name"]))
            model["name"] = min(model["name"], name, key=len)
            variants = record.get("data", {}).get(
                "nozzle_diameter"
            ) or self._machine_name_variants(str(record["name"]))
            if not isinstance(variants, list):
                variants = [variants]
            model["variants"].update(
                f"{value:g}" if isinstance(value, float) else str(value)
                for value in variants
            )

        for (vendor, model_id), model in models.items():
            variants = sorted(model["variants"], key=lambda value: float(value))
            aliases = list(dict.fromkeys(filter(None, (model_id, model["base_model"]))))
            storage_key = f"machine_model:{model_id}"
            yield ParsedProfile(
                slicer=self.slicer_type,
                profile_type=ProfileType.MACHINE_MODEL,
                name=model["name"],
                vendor=vendor,
                settings={
                    "name": model["name"],
                    "model_id": model_id,
                    "type": "machine_model",
                    "technology": model["technology"],
                    "variants": ";".join(variants),
                },
                source_path=source_path,
                native_id=model_id,
                storage_key=storage_key,
                context={
                    **self._context({"native_id": model_id}, slicer_version),
                    "storage_key": storage_key,
                    "display_name": model["name"],
                    "compatible_printer_models": aliases,
                    "variants": [{"key": variant} for variant in variants],
                },
            )

    @staticmethod
    def _machine_model_name(machine_name: str) -> str:
        name = re.sub(
            r"\s+(?:\d+T\s+)?"
            r"\d+(?:\.\d+)?\s*(?:mm\s*)?(?:HF|high[\s_-]*flow)?"
            r"(?:\s*,\s*\d+(?:\.\d+)?\s*(?:mm\s*)?"
            r"(?:HF|high[\s_-]*flow)?)*"
            r"(?:\s+nozzle)?\s*$",
            "",
            machine_name,
            flags=re.IGNORECASE,
        )
        return name.strip() or machine_name

    @staticmethod
    def _machine_name_variants(machine_name: str) -> list[str]:
        """Extract nozzle diameters absent from 3.0's machine settings object."""
        return re.findall(
            r"(?:^|[\s,])(\d+(?:\.\d+)?)\s*(?:mm\s*)?"
            r"(?:HF|high[\s_-]*flow)?(?=\s*(?:,|(?:nozzle)?\s*$))",
            machine_name,
            flags=re.IGNORECASE,
        )

    def parse_file(self, path: Path) -> ParsedProfile:
        data = json.loads(path.read_text(encoding="utf-8"))
        vendor = path.parent.name

        # Determine profile type from data content
        profile_type = self._detect_profile_type(data)

        # Name extraction per type
        name = self._extract_name(data, profile_type, path)

        filament_type = (
            data.get("filament_type") if profile_type == ProfileType.FILAMENT else None
        )

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
