"""Deterministic, deduplicated import bundle writer."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .catalog import EngineTarget
from .models import SlicerType

logger = logging.getLogger(__name__)

_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _source_id(source: Mapping[str, Any], fallback: str) -> str:
    context = source.get("context")
    if isinstance(context, Mapping):
        for key in ("source_id", "storage_key", "native_id"):
            value = context.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return fallback


def _merge_string_lists(left: Any, right: Any) -> list[str]:
    values = {
        str(value)
        for collection in (left, right)
        if isinstance(collection, list)
        for value in collection
    }
    return sorted(values)


def _merge_compatible_printers(left: Any, right: Any) -> dict[str, list[str]]:
    result: dict[str, set[str]] = {}
    for value in (left, right):
        if not isinstance(value, Mapping):
            continue
        for model, variants in value.items():
            if not isinstance(model, str):
                continue
            target = result.setdefault(model, set())
            if isinstance(variants, list):
                target.update(str(variant) for variant in variants)
    return {model: sorted(variants) for model, variants in sorted(result.items())}


def _merge_profile(existing: dict[str, Any], incoming: dict[str, Any]) -> None:
    existing["model_ids"] = sorted(
        {int(value) for value in existing["model_ids"] + incoming["model_ids"]}
    )
    left = copy.deepcopy(existing["profile"])
    right = copy.deepcopy(incoming["profile"])
    compatible = _merge_compatible_printers(
        left.pop("compatible_printers", None),
        right.pop("compatible_printers", None),
    )
    filament_ids = _merge_string_lists(
        left.pop("filament_db_ids", None), right.pop("filament_db_ids", None)
    )
    if left != right:
        raise ValueError(f"Conflicting payloads for stable profile {existing['id']}")
    if compatible:
        existing["profile"]["compatible_printers"] = compatible
    if filament_ids:
        existing["profile"]["filament_db_ids"] = filament_ids


def _read_list(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"Expected a JSON object list in {path}")
    return value


def collect_records(
    root: Path, catalog_lane: str | None = None
) -> dict[str, dict[str, Any]]:
    """Collapse the legacy per-model staging tree to one record per identity."""
    records: dict[str, dict[str, Any]] = {}
    machines: dict[str, dict[bytes, dict[str, Any]]] = {}
    for path in sorted(root.glob("models/*/*/machine_profiles.json")):
        model_id = int(path.parents[1].name)
        engine = path.parent.name
        for source in _read_list(path):
            machine = source.get("machine_model")
            if not isinstance(machine, dict):
                raise TypeError(f"Invalid machine profile in {path}")
            fallback = "/".join(
                str(value)
                for value in (
                    source.get("vendor", ""),
                    machine.get("name", ""),
                    machine.get("family", ""),
                )
            )
            source_id = str(source.get("source_id") or fallback)
            record_id = f"{engine}:machine:{source_id}"
            if catalog_lane:
                record_id += f"@{catalog_lane}"
            payload_key = _json_bytes(source)
            existing = machines.setdefault(record_id, {}).get(payload_key)
            if existing is None:
                machines[record_id][payload_key] = {
                    "engine": engine,
                    "id": record_id,
                    "kind": "machine",
                    "model_ids": [model_id],
                    "profile": source,
                    **({"catalog_lane": catalog_lane} if catalog_lane else {}),
                }
            else:
                existing["model_ids"] = sorted(set(existing["model_ids"] + [model_id]))

    for record_id, payloads in machines.items():
        for record in payloads.values():
            final_id = record_id
            if len(payloads) > 1:
                final_id += ":models:" + ",".join(
                    str(model_id) for model_id in record["model_ids"]
                )
                record["id"] = final_id
            records[final_id] = record

    profiles: dict[str, dict[bytes, dict[str, Any]]] = {}
    for kind in ("print", "filament"):
        for path in sorted(root.glob(f"models/*/*/{kind}_profiles.json")):
            model_id = int(path.parents[1].name)
            engine = path.parent.name
            for source in _read_list(path):
                fallback = f"{source.get('name') or ''}:model:{model_id}"
                source_id = _source_id(source, fallback)
                record_id = f"{engine}:{kind}:{source_id}"
                if catalog_lane:
                    record_id += f"@{catalog_lane}"
                incoming = {
                    "engine": engine,
                    "id": record_id,
                    "kind": kind,
                    "model_ids": [model_id],
                    "profile": source,
                    **({"catalog_lane": catalog_lane} if catalog_lane else {}),
                }
                payload = copy.deepcopy(source)
                payload.pop("compatible_printers", None)
                payload.pop("filament_db_ids", None)
                group = profiles.setdefault(record_id, {})
                payload_key = _json_bytes(payload)
                existing = group.get(payload_key)
                if existing is None:
                    group[payload_key] = incoming
                else:
                    _merge_profile(existing, incoming)

    for record_id, payloads in profiles.items():
        for record in payloads.values():
            final_id = record_id
            if len(payloads) > 1:
                final_id += ":models:" + ",".join(
                    str(model_id) for model_id in record["model_ids"]
                )
                record["id"] = final_id
            records[final_id] = record

    for path in sorted(root.glob("brands/*/**/generic_filament_profiles.json")):
        engine = path.parts[path.parts.index("brands") + 1]
        source_vendor = path.parent.name if path.parent.parent.name == engine else None
        for source in _read_list(path):
            fallback = "/".join(
                filter(None, (source_vendor, str(source.get("name") or "")))
            )
            source_id = _source_id(source, fallback)
            record_id = f"{engine}:filament:generic:{source_id}"
            if catalog_lane:
                record_id += f"@{catalog_lane}"
            records[record_id] = {
                "engine": engine,
                "generic": True,
                "id": record_id,
                "kind": "filament",
                "model_ids": [],
                "profile": source,
                "source_vendor": source_vendor,
                **({"catalog_lane": catalog_lane} if catalog_lane else {}),
            }
    return records


def _resource_refs(value: Any) -> Iterable[str]:
    if isinstance(value, str) and value.startswith("sha256:"):
        yield value
    elif isinstance(value, Mapping):
        for nested in value.values():
            yield from _resource_refs(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _resource_refs(nested)


def _zip_write(archive: zipfile.ZipFile, name: str, content: bytes) -> None:
    info = zipfile.ZipInfo(name, _FIXED_ZIP_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    archive.writestr(info, content)


def _validate_coverage(coverage: Mapping[str, Any]) -> None:
    """Reject partial or internally inconsistent model classifications."""
    for snapshot, report in coverage.items():
        if not isinstance(report, Mapping):
            raise TypeError(f"Invalid {snapshot} model coverage report")
        engines = report.get("engines")
        models = report.get("models")
        if not isinstance(engines, list) or not isinstance(models, list):
            raise TypeError(f"Invalid {snapshot} model coverage report")
        expected = len(engines) * len(models)
        mapped = 0
        for model in models:
            outcomes = model.get("outcomes") if isinstance(model, Mapping) else None
            if not isinstance(outcomes, Mapping) or set(outcomes) != set(engines):
                raise ValueError(f"Incomplete {snapshot} model coverage")
            for outcome in outcomes.values():
                if not isinstance(outcome, Mapping):
                    raise TypeError(f"Invalid {snapshot} model coverage outcome")
                status = outcome.get("status")
                if status == "mapped":
                    profiles = outcome.get("source_profiles")
                    if not isinstance(profiles, list) or not profiles:
                        raise ValueError(f"Invalid {snapshot} mapped outcome")
                    mapped += 1
                elif status != "unmapped" or not outcome.get("reason"):
                    raise ValueError(f"Invalid {snapshot} unmapped outcome")
        if (
            report.get("schema_version") != 1
            or report.get("total_models") != len(models)
            or report.get("classified") != expected
            or report.get("classified_percent") != 100
            or report.get("mapped") != mapped
            or report.get("unmapped") != expected - mapped
        ):
            raise ValueError(f"Inconsistent {snapshot} model coverage totals")


def write_bundle(
    path: Path,
    records: Mapping[str, Mapping[str, Any]],
    targets: Mapping[SlicerType, EngineTarget],
    resource_manifest: Mapping[str, Mapping[str, Any]],
    repo_root: Path,
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    """Write a byte-stable import bundle and return its manifest."""
    _validate_coverage(coverage)
    kind_order = {"machine": 0, "filament": 1, "print": 2}
    ordered = sorted(
        (dict(record) for record in records.values()),
        key=lambda record: (
            str(record["engine"]),
            kind_order.get(str(record["kind"]), 99),
            str(record["id"]),
        ),
    )
    for record in ordered:
        record["content_hash"] = hashlib.sha256(_json_bytes(record)).hexdigest()
    lines = [_json_bytes(record) for record in ordered]
    profiles_content = b"\n".join(lines) + (b"\n" if lines else b"")
    refs = sorted(set(_resource_refs(ordered)))
    resources: dict[str, dict[str, Any]] = {}
    resource_content: dict[str, bytes] = {}
    for ref in refs:
        metadata = resource_manifest.get(ref)
        if not isinstance(metadata, Mapping) or not isinstance(
            metadata.get("path"), str
        ):
            raise TypeError(f"Missing resource metadata for {ref}")
        source = repo_root / metadata["path"]
        content = source.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        if ref != f"sha256:{digest}":
            raise ValueError(f"Resource hash mismatch for {source}")
        suffix = source.suffix.lower()
        member = f"resources/{digest}{suffix}"
        resources[ref] = {"path": member, "size": len(content)}
        resource_content[member] = content

    engine_counts: dict[tuple[str, str | None], int] = {}
    for record in ordered:
        engine = str(record["engine"])
        lane = record.get("catalog_lane")
        key = (engine, str(lane) if lane is not None else None)
        engine_counts[key] = engine_counts.get(key, 0) + 1
    manifest = {
        "coverage": coverage,
        "engines": {
            slicer.value: {
                "gcode_abi": target.gcode_abi,
                "records": engine_counts.get((slicer.value, None), 0),
                "version": target.version,
                "lanes": {
                    lane: {
                        "format": lane_target.format,
                        "gcode_abi": lane_target.gcode_abi,
                        "records": engine_counts.get((slicer.value, lane), 0),
                        "version": lane_target.version,
                    }
                    for lane, lane_target in sorted((target.lanes or {}).items())
                },
            }
            for slicer, target in sorted(
                targets.items(), key=lambda item: item[0].value
            )
        },
        "format": "sp-profile-bundle",
        "profiles_sha256": hashlib.sha256(profiles_content).hexdigest(),
        "records": len(ordered),
        "resources": resources,
        "schema_version": 2,
    }
    manifest_content = _json_bytes(manifest) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        _zip_write(archive, "manifest.json", manifest_content)
        _zip_write(archive, "profiles.ndjson", profiles_content)
        for member in sorted(resource_content):
            _zip_write(archive, member, resource_content[member])
    logger.info(
        "Wrote %s (%d records, %d resources, sha256 %s)",
        path,
        len(ordered),
        len(resources),
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    return manifest
