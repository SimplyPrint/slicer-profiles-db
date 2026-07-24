#!/usr/bin/env python3
"""Compare whether one slicer profile /out directory is a superset of another."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

RESOURCE_KEYS = ("bed_model", "bed_texture", "hotend_model", "thumbnail")
PROFILE_FILES = (
    "machine_profiles.json",
    "filament_profiles.json",
    "print_profiles.json",
)


@dataclass
class OutIndex:
    out_dir: Path
    repo_root: Path
    model_slicers: dict[str, set[str]] = field(default_factory=dict)
    files: dict[tuple[str, str, str], Path] = field(default_factory=dict)
    names: dict[tuple[str, str, str], set[str]] = field(default_factory=dict)
    resources: dict[str, dict[str, list[dict[str, Any]]]] = field(default_factory=dict)
    invalid_resources: list[str] = field(default_factory=list)
    counts: Counter[str] = field(default_factory=Counter)


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def profile_name(profile_file: str, entry: dict[str, Any]) -> str | None:
    if profile_file == "machine_profiles.json":
        machine_model = entry.get("machine_model", {})
        return machine_model.get("name")
    return entry.get("name")


def build_index(
    out_dir: Path,
    repo_root: Path | None = None,
    slicer_filter: set[str] | None = None,
    profile_files: tuple[str, ...] = PROFILE_FILES,
) -> OutIndex:
    out_dir = out_dir.resolve()
    repo_root = (repo_root or out_dir.parent).resolve()
    index = OutIndex(out_dir=out_dir, repo_root=repo_root)

    resources = load_json(out_dir / "resources.json")
    if isinstance(resources, dict):
        index.resources = resources

    models_dir = out_dir / "models"
    if not models_dir.exists():
        return index

    for model_dir in sorted(p for p in models_dir.iterdir() if p.is_dir()):
        model_id = model_dir.name
        for slicer_dir in sorted(p for p in model_dir.iterdir() if p.is_dir()):
            slicer = slicer_dir.name
            if slicer_filter is not None and slicer not in slicer_filter:
                continue
            index.model_slicers.setdefault(model_id, set()).add(slicer)
            index.counts["model_slicer_dirs"] += 1

            for profile_file in profile_files:
                path = slicer_dir / profile_file
                if not path.exists():
                    continue
                index.files[(model_id, slicer, profile_file)] = path
                data = load_json(path)
                if not isinstance(data, list):
                    continue
                index.counts[profile_file] += 1
                names = {
                    name
                    for item in data
                    if isinstance(item, dict)
                    and (name := profile_name(profile_file, item))
                }
                index.names[(model_id, slicer, profile_file)] = names

                if profile_file == "machine_profiles.json":
                    validate_machine_resources(index, model_id, slicer, path, data)

    index.counts["models"] = len(index.model_slicers)
    return index


def validate_machine_resources(
    index: OutIndex, model_id: str, slicer: str, path: Path, data: list[Any]
) -> None:
    for item in data:
        if not isinstance(item, dict):
            continue
        machine_model = item.get("machine_model", {})
        if not isinstance(machine_model, dict):
            continue
        machine_name = machine_model.get("name", "<unknown>")
        for key in RESOURCE_KEYS:
            value = machine_model.get(key)
            if not isinstance(value, str) or not value:
                continue
            if not resolve_resource(index, path.parent, slicer, value):
                index.invalid_resources.append(
                    f"{model_id}/{slicer}: {machine_name} {key}={value}"
                )


def resolve_resource(
    index: OutIndex, local_dir: Path, slicer: str, value: str
) -> Path | None:
    local = local_dir / value
    if local.exists():
        return local

    if value.startswith("sha256:"):
        entry = (
            index.resources.get(value) if isinstance(index.resources, dict) else None
        )
        if isinstance(entry, dict) and entry.get("path"):
            candidate = index.repo_root / entry["path"]
            if candidate.exists():
                return candidate

        resource_root = index.repo_root / "profiles" / slicer / "_resources"
        if resource_root.exists():
            for candidate in resource_root.glob(f"{value[7:]}.*"):
                if candidate.is_file():
                    return candidate
        return None

    # Legacy filename manifest shape: {slicer: {filename: [{path}]}}
    resources_for_slicer = (
        index.resources.get(slicer, {}) if isinstance(index.resources, dict) else {}
    )
    entries = resources_for_slicer.get(value)
    if entries is None:
        value_lower = value.lower()
        for filename, candidate_entries in resources_for_slicer.items():
            if filename.lower() == value_lower:
                entries = candidate_entries
                break

    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict) or not entry.get("path"):
                continue
            candidate = index.repo_root / entry["path"]
            if candidate.exists():
                return candidate

    resource_root = index.repo_root / "profiles" / slicer / "_resources"
    if resource_root.exists():
        value_lower = value.lower()
        for candidate in resource_root.rglob("*"):
            if candidate.is_file() and candidate.name.lower() == value_lower:
                return candidate

    return None


def compare(base: OutIndex, candidate: OutIndex) -> dict[str, Any]:
    missing_models = sorted(set(base.model_slicers) - set(candidate.model_slicers))
    missing_slicers: dict[str, list[str]] = {}
    missing_files: list[str] = []
    missing_names: dict[str, list[str]] = {}

    for model_id, base_slicers in base.model_slicers.items():
        candidate_slicers = candidate.model_slicers.get(model_id, set())
        missing = sorted(base_slicers - candidate_slicers)
        if missing:
            missing_slicers[model_id] = missing

    for key in base.files:
        model_id, slicer, profile_file = key
        if key not in candidate.files:
            missing_files.append(f"{model_id}/{slicer}/{profile_file}")
            continue
        base_names = base.names.get(key, set())
        candidate_names = candidate.names.get(key, set())
        missing = sorted(base_names - candidate_names)
        if missing:
            missing_names[f"{model_id}/{slicer}/{profile_file}"] = missing

    missing_name_counts = Counter(
        name for names in missing_names.values() for name in names
    )

    return {
        "is_superset": not (
            missing_models
            or missing_slicers
            or missing_files
            or missing_names
            or candidate.invalid_resources
        ),
        "base_counts": dict(base.counts),
        "candidate_counts": dict(candidate.counts),
        "missing_models": missing_models,
        "missing_slicers": missing_slicers,
        "missing_files": sorted(missing_files),
        "missing_files_by_slicer": dict(
            Counter(item.split("/")[1] for item in missing_files)
        ),
        "missing_profile_names": missing_names,
        "missing_profile_name_counts": dict(missing_name_counts.most_common()),
        "candidate_invalid_resources": candidate.invalid_resources,
    }


def print_human(report: dict[str, Any], max_items: int) -> None:
    status = "PASS" if report["is_superset"] else "FAIL"
    print(f"Superset status: {status}")
    print(f"Base counts:      {report['base_counts']}")
    print(f"Candidate counts: {report['candidate_counts']}")

    if report.get("missing_files_by_slicer"):
        print(f"Missing files by slicer: {report['missing_files_by_slicer']}")

    sections = [
        ("Missing models", report["missing_models"]),
        ("Missing model slicers", report["missing_slicers"]),
        ("Missing files", report["missing_files"]),
        ("Most repeated missing profile names", report["missing_profile_name_counts"]),
        ("Missing profile names", report["missing_profile_names"]),
        ("Invalid candidate resources", report["candidate_invalid_resources"]),
    ]
    for title, value in sections:
        count = len(value)
        print(f"\n{title}: {count}")
        if not value:
            continue
        if isinstance(value, dict):
            for idx, (key, items) in enumerate(value.items()):
                if idx >= max_items:
                    print(f"  ... {count - max_items} more")
                    break
                preview = items[:10] if isinstance(items, list) else items
                print(f"  {key}: {preview}")
        else:
            for item in value[:max_items]:
                print(f"  {item}")
            if count > max_items:
                print(f"  ... {count - max_items} more")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_out", type=Path, help="Baseline /out directory")
    parser.add_argument("candidate_out", type=Path, help="Candidate /out directory")
    parser.add_argument(
        "--base-root",
        type=Path,
        default=None,
        help="Repo root for resolving baseline resources",
    )
    parser.add_argument(
        "--candidate-root",
        type=Path,
        default=None,
        help="Repo root for resolving candidate resources",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON report")
    parser.add_argument(
        "--slicer",
        action="append",
        default=None,
        help="Only compare this slicer (repeatable)",
    )
    parser.add_argument(
        "--profile-file",
        action="append",
        choices=PROFILE_FILES,
        default=None,
        help="Only compare this profile file (repeatable)",
    )
    parser.add_argument(
        "--max-items", type=int, default=25, help="Max items per human section"
    )
    args = parser.parse_args()

    slicer_filter = set(args.slicer) if args.slicer else None
    profile_files = tuple(args.profile_file) if args.profile_file else PROFILE_FILES

    base = build_index(args.base_out, args.base_root, slicer_filter, profile_files)
    candidate = build_index(
        args.candidate_out, args.candidate_root, slicer_filter, profile_files
    )
    report = compare(base, candidate)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_human(report, args.max_items)

    return 0 if report["is_superset"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
