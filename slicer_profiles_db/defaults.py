"""
Fetch slicer configuration artifacts (defaults.json).

Downloads machine.json and print_config_def.json from SimplyPrint slicer-builds,
extracts default printer option values.
"""

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen

from .models import SlicerType

# Slicers that have defaults available via SimplyPrint slicer-builds
_DEFAULTS_SLICERS = [
    SlicerType.ORCASLICER,
    SlicerType.BAMBUSTUDIO,
    SlicerType.PRUSASLICER,
]

# Map SlicerType to the artifact directory name
_SLICER_ARTIFACT_NAMES: dict[SlicerType, str] = {
    SlicerType.ORCASLICER: "OrcaSlicer",
    SlicerType.BAMBUSTUDIO: "BambuStudio",
    SlicerType.PRUSASLICER: "PrusaSlicer",
}


def _get_slicer_config_artifact(artifact_path: str) -> dict[str, Any]:
    """Download a slicer config artifact from SimplyPrint's slicer-builds repo."""
    try:
        with urlopen(
            f"https://api.github.com/repos/SimplyPrint/slicer-builds/contents/{artifact_path}"
            "?ref=slicer-config-artifacts"
        ) as f:
            data = json.load(f)
            with urlopen(data["download_url"]) as f2:
                return json.load(f2)
    except (HTTPError, json.JSONDecodeError, KeyError):
        return {}


def fetch_slicer_defaults(slicer: SlicerType) -> dict[str, Any]:
    """
    Download machine.json + print_config_def.json from SimplyPrint slicer-builds,
    extract default printer option values.

    Used by conditions.py for evaluating compatible_printers_condition.

    Args:
        slicer: Which slicer to fetch defaults for.

    Returns:
        Dict mapping option names to their default values.
    """
    artifact_name = _SLICER_ARTIFACT_NAMES.get(slicer)
    if not artifact_name:
        return {}

    printer_data = _get_slicer_config_artifact(f"{artifact_name}/machine.json")
    config_def_data = _get_slicer_config_artifact(f"{artifact_name}/print_config_def.json")

    if not printer_data:
        return {}

    # Unpack all the options from the printer data structure
    tmp_out: dict[str, Any] = {}
    for _category, page_data in printer_data.items():
        for _cat2, optgroup_data in page_data.items():
            for line in optgroup_data:
                if isinstance(line, str):
                    options = {line}
                elif isinstance(line, dict):
                    options = line.get("options", set())
                else:
                    continue

                for option in options:
                    option = option.removesuffix("#0")
                    tmp_out[option] = None

    # Get the default value for each option
    for option_key in tmp_out:
        if option_key in config_def_data and "default_value" in config_def_data[option_key]:
            tmp_out[option_key] = config_def_data[option_key]["default_value"]

    # Only return options that have defaults
    return {k: v for k, v in tmp_out.items() if v is not None}


def fetch_all_slicer_defaults(output_dir: Path | None = None) -> dict[SlicerType, dict[str, Any]]:
    """
    Fetch defaults for all supported slicers.

    Args:
        output_dir: If provided, save defaults.json files to slicer subdirectories.

    Returns:
        Dict mapping SlicerType to their defaults.
    """
    results: dict[SlicerType, dict[str, Any]] = {}

    for slicer in _DEFAULTS_SLICERS:
        defaults = fetch_slicer_defaults(slicer)
        if defaults:
            results[slicer] = defaults

            if output_dir:
                slicer_dir = output_dir / slicer.value
                slicer_dir.mkdir(parents=True, exist_ok=True)
                defaults_path = slicer_dir / "defaults.json"
                with defaults_path.open("w") as f:
                    json.dump(defaults, f, indent=4)

    return results


def load_defaults_from_file(path: Path) -> dict[str, Any]:
    """Load previously-saved defaults.json."""
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)
