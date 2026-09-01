import hashlib
import json
import re
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
        "process": ProfileType.PRINT,
        "tool_process": ProfileType.TOOL_PRINT,
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
        if not isinstance(bundle, dict):
            raise TypeError("profile bundle root must be an object")
        if bundle.get("schema_version") != 1:
            raise ValueError("unsupported PrusaSlicer profile bundle schema")
        if bundle.get("format") != "prusa-evaluated-profiles":
            raise ValueError("unsupported PrusaSlicer profile bundle format")

        slicer_version = str(bundle.get("slicer_version", ""))
        machine_records = bundle.get("machine", [])
        if not isinstance(machine_records, list):
            raise TypeError("profile bundle 'machine' must be an array")
        tool_process_records = bundle.get("tool_process", [])
        if not isinstance(tool_process_records, list):
            raise TypeError("profile bundle 'tool_process' must be an array")
        if not profile_type_filter or ProfileType.MACHINE_MODEL in profile_type_filter:
            yield from self._derive_machine_models(
                machine_records,
                path,
                bundle["format"],
                bundle["schema_version"],
                slicer_version,
            )
        for collection, profile_type in self._BUNDLE_TYPES.items():
            if profile_type_filter and profile_type not in profile_type_filter:
                continue
            records = bundle.get(collection, [])
            if not isinstance(records, list):
                raise TypeError(f"profile bundle {collection!r} must be an array")
            native_id_counts: dict[str, int] = {}
            for record in records:
                if isinstance(record, dict):
                    native_id = str(record.get("native_id", ""))
                    native_id_counts[native_id] = native_id_counts.get(native_id, 0) + 1

            for record in records:
                if (
                    profile_type == ProfileType.TOOL_PRINT
                    and isinstance(record, dict)
                    and record.get("data") is None
                ):
                    # The evaluator emits a "no tool" sentinel for printers
                    # whose process role has no per-tool overrides.
                    continue
                if not isinstance(record, dict) or not isinstance(
                    record.get("data"), dict
                ):
                    raise TypeError(f"invalid profile bundle record in {collection!r}")
                native_id = str(record["native_id"])
                data = dict(record["data"])
                contexts = record.get("contexts", [])
                if profile_type == ProfileType.MACHINE:
                    data.setdefault("printer_settings_id", str(record["name"]))
                    if contexts:
                        data.setdefault(
                            "printer_model", contexts[0].get("printer_model")
                        )
                elif profile_type == ProfileType.PRINT:
                    data.setdefault("print_settings_id", str(record["name"]))
                elif profile_type == ProfileType.TOOL_PRINT:
                    data.setdefault("tool_print_settings_id", str(record["name"]))
                elif profile_type == ProfileType.FILAMENT:
                    data.setdefault("filament_settings_id", str(record["name"]))

                compatible_printers = sorted(
                    {
                        context["machine_name"]
                        for context in contexts
                        if isinstance(context, dict)
                        and isinstance(context.get("machine_name"), str)
                    }
                )
                if compatible_printers:
                    data.setdefault("compatible_printers", compatible_printers)
                storage_key = f"{collection}:{native_id}"
                if native_id_counts[native_id] > 1:
                    variant = hashlib.sha256(
                        json.dumps(data, sort_keys=True, separators=(",", ":")).encode(
                            "utf-8"
                        )
                    ).hexdigest()[:12]
                    storage_key = f"{storage_key}:{variant}"
                yield ParsedProfile(
                    slicer=self.slicer_type,
                    profile_type=profile_type,
                    name=str(record["name"]),
                    vendor=str(record["vendor_id"]),
                    settings=data,
                    source_path=path,
                    native_id=native_id,
                    storage_key=storage_key,
                    filament_type=(
                        data.get("filament_type")
                        if profile_type == ProfileType.FILAMENT
                        else None
                    ),
                    context={
                        "format": bundle["format"],
                        "schema_version": bundle["schema_version"],
                        "slicer_version": slicer_version,
                        "native_id": native_id,
                        "storage_key": storage_key,
                        "root_id": record.get("root_id", ""),
                        "technology": record.get("technology", ""),
                        "compatibility": contexts,
                        **(
                            {
                                "tool_process_profiles": (
                                    self._matching_tool_process_profiles(
                                        record, tool_process_records
                                    )
                                )
                            }
                            if profile_type == ProfileType.PRINT
                            else {}
                        ),
                        **(
                            {
                                key: contexts[0][key]
                                for key in (
                                    "hardware_id",
                                    "printer_model",
                                    "printer_base_model",
                                )
                                if contexts
                                and isinstance(contexts[0], dict)
                                and key in contexts[0]
                            }
                            if profile_type == ProfileType.MACHINE
                            else {}
                        ),
                    },
                )

    @staticmethod
    def _matching_tool_process_profiles(
        process: dict, tool_processes: list
    ) -> list[dict]:
        """Retain the evaluated per-tool configs required by a process role."""
        process_id = str(process["native_id"])
        process_contexts = process.get("contexts", [])
        machine_ids = {
            context.get("machine_id")
            for context in process_contexts
            if isinstance(context, dict) and context.get("machine_id") is not None
        }
        result = []
        for tool_process in tool_processes:
            if not isinstance(tool_process, dict) or not isinstance(
                tool_process.get("data"), dict
            ):
                continue
            matching_contexts = [
                context
                for context in tool_process.get("contexts", [])
                if isinstance(context, dict)
                and str(context.get("process_id", "")) == process_id
                and (not machine_ids or context.get("machine_id") in machine_ids)
            ]
            if not matching_contexts:
                continue
            result.append(
                {
                    "vendor_id": tool_process.get("vendor_id", ""),
                    "native_id": tool_process.get("native_id", ""),
                    "root_id": tool_process.get("root_id", ""),
                    "name": tool_process.get("name", ""),
                    "data": tool_process["data"],
                    "contexts": matching_contexts,
                }
            )
        return result

    def _derive_machine_models(
        self,
        records: list,
        source_path: Path,
        bundle_format: str,
        schema_version: int,
        slicer_version: str,
    ) -> Iterator[ParsedProfile]:
        """Build mapping-friendly model/variant roles from evaluated machines."""
        models: dict[tuple[str, str], dict] = {}
        for record in records:
            if not isinstance(record, dict) or not isinstance(record.get("data"), dict):
                continue
            contexts = record.get("contexts", [])
            context = contexts[0] if contexts and isinstance(contexts[0], dict) else {}
            model_id = str(
                context.get("printer_model")
                or record["data"].get("printer_model")
                or record.get("root_id")
                or record["native_id"]
            )
            vendor = str(record["vendor_id"])
            key = (vendor, model_id)
            model = models.setdefault(
                key,
                {
                    "name": self._machine_model_name(str(record["name"])),
                    "base_model": context.get("printer_base_model"),
                    "technology": str(record.get("technology", "")),
                    "variants": set(),
                },
            )
            candidate_name = self._machine_model_name(str(record["name"]))
            if len(candidate_name) < len(model["name"]):
                model["name"] = candidate_name
            nozzle_diameters = record["data"].get("nozzle_diameter", [])
            if not isinstance(nozzle_diameters, list):
                nozzle_diameters = [nozzle_diameters]
            if not nozzle_diameters:
                nozzle_diameters = self._machine_name_variants(str(record["name"]))
            model["variants"].update(
                self._format_variant(value)
                for value in nozzle_diameters
                if value not in (None, "")
            )

        for (vendor, model_id), model in models.items():
            variants = sorted(model["variants"], key=self._variant_sort_key)
            aliases = [model_id]
            if model["base_model"] and model["base_model"] not in aliases:
                aliases.append(str(model["base_model"]))
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
                    "format": bundle_format,
                    "schema_version": schema_version,
                    "slicer_version": slicer_version,
                    "native_id": model_id,
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

    @staticmethod
    def _format_variant(value) -> str:
        if isinstance(value, float):
            return f"{value:g}"
        return str(value)

    @staticmethod
    def _variant_sort_key(value: str) -> tuple[float, str]:
        try:
            return float(value), value
        except ValueError:
            return float("inf"), value

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
