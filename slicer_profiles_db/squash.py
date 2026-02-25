"""
INI bundle splitting and profile inheritance squashing.

Handles two profile formats:
- INI bundles (PrusaSlicer, SuperSlicer): split into individual JSONs,
  resolve inheritance within each type group.
- JSON profiles (BambuStudio, OrcaSlicer, ElegooSlicer): resolve "include"
  references and "inherits" chains, write only instantiable profiles.

Key design decisions:
- gcode template files (BBS/Orca "include" mechanism) have no "type" field and
  must always be loaded regardless of profile_type_filter, so that the include
  references can be resolved before inheritance squashing.
- The OrcaFilamentLibrary (shared library) is loaded separately and merged into
  each vendor's profile set as a base, so vendor profiles can inherit from library
  profiles. Library profiles themselves are also squashed.
- Vendor directories are cleared (rmtree) during squash and only instantiable
  profiles are rewritten, preventing stale files.
"""

import fileinput
import json
import logging
import re
import shutil
from pathlib import Path

import iniconfig
from iniconfig import IniConfig, ParseError

from .models import ProfileType

logger = logging.getLogger(__name__)

# Disable comment handling in iniconfig
iniconfig.COMMENTCHARS = ""

# Map INI section prefixes to ProfileType
_INI_SECTION_PREFIXES: dict[str, ProfileType] = {
    "filament:": ProfileType.FILAMENT,
    "printer_model:": ProfileType.MACHINE_MODEL,
    "printer:": ProfileType.MACHINE,
    "print:": ProfileType.PRINT,
}

# Settings ID keys per profile type (added to squashed output)
_SETTINGS_ID_KEY: dict[ProfileType, str] = {
    ProfileType.FILAMENT: "filament_settings_id",
    ProfileType.MACHINE: "printer_settings_id",
    ProfileType.MACHINE_MODEL: "name",
    ProfileType.PRINT: "print_settings_id",
}


def split_prusaslicer_bundle(
    ini_path: Path,
    output_dir: Path,
    section_types: list[str] | None = None,
) -> list[Path]:
    """
    Split a PrusaSlicer INI bundle into individual JSON config files.

    Handles ALL section types: filament:, printer_model:, printer:, print:
    Squashes inheritance within each type group.

    Args:
        ini_path: Path to the .ini bundle file.
        output_dir: Directory to write individual JSON files.
        section_types: If set, only process these section prefixes
                       (e.g. ["filament:", "printer:"]).
                       If None, process all known types.

    Returns:
        List of paths to created JSON files.
    """
    if ini_path.suffix != ".ini":
        return []

    config = _load_ini_config(ini_path)
    if config is None:
        return []

    # Determine which section prefixes to process
    if section_types:
        prefixes = {p: pt for p, pt in _INI_SECTION_PREFIXES.items() if p in section_types}
    else:
        prefixes = _INI_SECTION_PREFIXES.copy()

    # Gather profiles by type group
    profiles_by_type: dict[ProfileType, dict[str, dict[str, str]]] = {}
    for section in config:
        for prefix, profile_type in prefixes.items():
            if section.name.startswith(prefix):
                name = section.name[len(prefix):]
                profiles_by_type.setdefault(profile_type, {})[name] = dict(section.items())
                break

    output_dir.mkdir(parents=True, exist_ok=True)
    created_files: list[Path] = []

    for profile_type, profiles in profiles_by_type.items():
        squashed: dict[str, dict[str, str]] = {}

        def squash_inherits(profile_name: str) -> dict[str, str]:
            if profile_name in squashed:
                return squashed[profile_name]

            if profile_name not in profiles:
                return {}

            profile = profiles[profile_name]
            if "inherits" not in profile:
                return profile

            profile_out: dict[str, str] = {}
            inherits = [x.strip() for x in profile["inherits"].split(";")]
            for parent in inherits:
                if parent:
                    profile_out.update(squash_inherits(parent))
            profile_out.update(profile)
            del profile_out["inherits"]
            squashed[profile_name] = profile_out
            return squashed[profile_name]

        settings_id_key = _SETTINGS_ID_KEY.get(profile_type)

        for name, data in profiles.items():
            # Skip template profiles (prefixed with *)
            if name.startswith("*"):
                continue

            safe_name = name.replace("/", " ")
            out_path = output_dir / f"{safe_name}.json"
            data_out = squash_inherits(name)

            # Add the settings ID to the output
            if settings_id_key and settings_id_key != "name":
                data_out[settings_id_key] = name

            with out_path.open("w") as f:
                json.dump(data_out, f, indent=4)
            created_files.append(out_path)

    return created_files


def squash_slic3r_profiles(
    vendor_dir: Path,
    profile_type: ProfileType | None = None,
    filament_library_dir: Path | None = None,
) -> list[Path]:
    """
    Squash BBS/Orca JSON profiles for a single vendor directory.

    1. Load all profiles from vendor dir (and optionally shared library)
    2. Resolve 'include' references (BBS gcode template files)
    3. Recursively resolve 'inherits' chains
    4. Write only instantiable profiles, remove everything else

    The profile_type filter controls which profiles are written out, but
    ALL files are loaded for inheritance/include resolution (gcode templates
    have no "type" field and must always be available).

    Args:
        vendor_dir: The vendor directory (e.g. orcaslicer/BBL/).
        profile_type: If set, only write out profiles of this type.
                     None = write all types.
        filament_library_dir: Optional shared library directory
                             (e.g. OrcaFilamentLibrary/) to include
                             in inheritance resolution.

    Returns:
        List of paths to squashed JSON files.
    """
    if not vendor_dir.exists() or not vendor_dir.is_dir():
        return []

    # Load ALL JSON profiles from the vendor directory — no type filtering here,
    # because gcode templates and base profiles must be available for resolution
    profiles = _load_json_from_folder(vendor_dir, profile_type_filter=None)

    # Also load shared library profiles if provided
    if filament_library_dir and filament_library_dir.exists():
        library_profiles = _load_json_from_folder(filament_library_dir, profile_type_filter=None)
        # Library profiles serve as base; vendor profiles with same name override
        merged = library_profiles.copy()
        merged.update(profiles)
        profiles = merged

    # Resolve "include" references (BBS gcode template files)
    # Template files contain gcode fields like machine_start_gcode as separate
    # JSON files. The "include" list in a machine profile references them by name.
    # Included settings are applied only if the key doesn't already exist in the
    # profile (profile's own values take precedence).
    for name, (path, data) in profiles.items():
        if "include" not in data:
            continue
        for include_name in data["include"]:
            if include_name not in profiles:
                continue
            included_data = profiles[include_name][1]
            for key, value in included_data.items():
                if key not in data and key not in ("name", "instantiation"):
                    data[key] = value
        del data["include"]

    # Squash inheritance
    squashed: dict[str, dict] = {}

    def squash_inherits(profile_name: str) -> dict:
        if profile_name in squashed:
            return squashed[profile_name]

        entry = profiles.get(profile_name)
        if entry is None:
            return {}

        profile = entry[1]
        if "inherits" not in profile:
            return profile

        profile_out = squash_inherits(profile["inherits"]).copy()
        profile_out.update(profile)
        del profile_out["inherits"]
        squashed[profile_name] = profile_out
        return squashed[profile_name]

    # Remove existing vendor directory contents and rewrite only instantiable profiles
    # (matches original behavior: shutil.rmtree then selective rewrite)
    shutil.rmtree(vendor_dir)

    created_files: list[Path] = []

    for name, (path, data) in profiles.items():
        # Only write profiles that originally belonged to this vendor dir
        if vendor_dir.name not in path.parts:
            continue

        # Skip non-instantiable profiles (base profiles, gcode templates).
        # machine_model profiles are concrete hardware definitions and never
        # carry an "instantiation" field, so exempt them from this check.
        raw_type = data.get("type")
        if raw_type != "machine_model" and data.get("instantiation") != "true":
            continue

        # Apply profile_type filter for output
        if profile_type is not None and "type" in data:
            raw_type = data["type"]
            if raw_type == "process":
                raw_type = "print"
            if raw_type != profile_type.value:
                continue

        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            squashed_data = squash_inherits(name)
            with path.open("w") as f:
                json.dump(squashed_data, f, indent=4)
            created_files.append(path)
        except (KeyError, RecursionError) as e:
            logger.warning(
                "Failed to squash profile: vendor=%s, profile=%s: %s",
                vendor_dir.name, name, e,
            )
            continue

    return created_files


def select_latest_ini_bundle(vendor_dir: Path) -> Path | None:
    """
    Find the highest-version INI file in a vendor directory.

    Matches filenames like 2.9.3.ini or 2.5.59.0.ini (any number of
    dot-separated numeric parts).  Returns the path to the latest version,
    or None if no INI files found.
    """
    from .models import _version_key

    version_re = re.compile(r"([\d]+(?:\.[\d]+)+)\.ini$", re.IGNORECASE)
    latest_key: tuple[int, ...] = ()
    latest_path: Path | None = None

    if not vendor_dir.exists():
        return None

    for config_file in vendor_dir.iterdir():
        match = version_re.search(config_file.name)
        if not match:
            continue
        ver = _version_key(match.group(1))
        if ver > latest_key:
            latest_key = ver
            latest_path = config_file

    return latest_path


def unpack_prusaslicer_bundles(
    prusaslicer_dir: Path,
    section_types: list[str] | None = None,
) -> tuple[list[Path], str | None]:
    """
    Find and unpack all INI bundles in a directory.

    Supports two layouts:
    - **Versioned** (PrusaSlicer): vendor_dir/{version}.ini files
    - **Flat** (slic3r-profiles/SuperSlicer): root-level {VendorName}.ini files
      alongside vendor asset directories

    Args:
        prusaslicer_dir: Root profiles directory.
        section_types: If set, only process these INI section types.

    Returns:
        Tuple of (list of created JSON file paths, detected version string or None).
        The detected version is the highest version found across all INI bundle
        filenames (e.g. "2.4.9" from "2.4.9.ini").  For flat-layout repos where
        filenames are vendor names (e.g. "Creality.ini"), the version is extracted
        from the config_version field inside the INI.
    """
    from .models import _version_key

    if not prusaslicer_dir.exists():
        return [], None

    # Handle flat layout: INI files at root level (e.g. slic3r-profiles repo)
    _relocate_flat_inis(prusaslicer_dir)

    all_created: list[Path] = []
    detected_version: str | None = None
    detected_version_key: tuple[int, ...] = ()
    version_re = re.compile(r"([\d]+(?:\.[\d]+)+)\.ini$", re.IGNORECASE)

    for vendor_dir in prusaslicer_dir.iterdir():
        if not vendor_dir.is_dir():
            continue

        # Try versioned filenames first (e.g. 2.9.3.ini)
        latest_ini = select_latest_ini_bundle(vendor_dir)
        if latest_ini is not None:
            # Track the highest version across all vendors
            m = version_re.search(latest_ini.name)
            if m:
                vk = _version_key(m.group(1))
                if vk > detected_version_key:
                    detected_version_key = vk
                    detected_version = m.group(1)
        else:
            # Fall back to vendor-named INI (e.g. Creality.ini)
            vendor_inis = list(vendor_dir.glob("*.ini"))
            if vendor_inis:
                latest_ini = vendor_inis[0]
                # Extract config_version from the INI content
                cv = _read_config_version(latest_ini)
                if cv:
                    vk = _version_key(cv)
                    if vk > detected_version_key:
                        detected_version_key = vk
                        detected_version = cv
            else:
                continue

        # Remove non-latest INI files
        for config_file in vendor_dir.iterdir():
            if config_file.suffix == ".ini" and config_file != latest_ini:
                config_file.unlink()

        # Split the latest bundle
        created = split_prusaslicer_bundle(latest_ini, vendor_dir, section_types)
        all_created.extend(created)

        # Remove the INI file after splitting
        if latest_ini.exists():
            latest_ini.unlink()

    return all_created, detected_version


def _relocate_flat_inis(directory: Path) -> None:
    """Move root-level {VendorName}.ini files into {VendorName}/ subdirectories.

    This normalizes flat-layout repos (e.g. slic3r-profiles/SuperSlicer) so that
    every INI lives inside a vendor directory, matching the versioned layout.
    """
    for ini_file in list(directory.glob("*.ini")):
        vendor_name = ini_file.stem
        vendor_dir = directory / vendor_name
        vendor_dir.mkdir(exist_ok=True)
        ini_file.rename(vendor_dir / ini_file.name)


def iter_ini_bundle_versions(
    directory: Path,
    min_version: str | None = None,
) -> list[tuple[str, list[tuple[str, Path]]]]:
    """Enumerate all vendor-version pairs from INI bundles in a directory.

    Returns a list of ``(version_str, [(vendor_name, ini_path), ...])`` grouped
    by version string, sorted oldest-first by ``_version_key``.

    For versioned-filename vendors (e.g. ``PrusaResearch/2.4.9.ini``), the
    version comes from the filename.  For flat-layout vendors (e.g.
    ``Creality/Creality.ini``), the version is read from the INI's
    ``config_version`` field.

    Args:
        directory: Root directory containing vendor subdirectories with INI files.
        min_version: If set, skip versions below this threshold.
    """
    from .models import _version_key
    from .versions import enumerate_ini_versions

    _relocate_flat_inis(directory)

    # Collect all (version, vendor_name, ini_path) triples
    triples: list[tuple[str, str, Path]] = []

    for vendor_dir in sorted(directory.iterdir()):
        if not vendor_dir.is_dir():
            continue

        # Try versioned filenames first (e.g. 2.9.3.ini)
        ini_versions = enumerate_ini_versions(vendor_dir)
        if ini_versions:
            version_re = re.compile(r"([\d]+(?:\.[\d]+)+)\.ini$", re.IGNORECASE)
            for ini_file in vendor_dir.iterdir():
                m = version_re.search(ini_file.name)
                if m:
                    triples.append((m.group(1), vendor_dir.name, ini_file))
        else:
            # Flat layout: vendor-named INI (e.g. Creality/Creality.ini)
            vendor_inis = list(vendor_dir.glob("*.ini"))
            for ini_file in vendor_inis:
                cv = _read_config_version(ini_file)
                if cv:
                    triples.append((cv, vendor_dir.name, ini_file))

    # Apply min_version filter
    if min_version:
        min_key = _version_key(min_version)
        triples = [(v, vn, p) for v, vn, p in triples if _version_key(v) >= min_key]

    # Group by version string, sort groups oldest-first
    from collections import defaultdict
    groups: dict[str, list[tuple[str, Path]]] = defaultdict(list)
    for ver, vendor_name, ini_path in triples:
        groups[ver].append((vendor_name, ini_path))

    sorted_versions = sorted(groups.keys(), key=_version_key)
    return [(ver, groups[ver]) for ver in sorted_versions]


def _read_config_version(ini_path: Path) -> str | None:
    """Read config_version from a PrusaSlicer/Slic3r INI file's [vendor] section."""
    cv_re = re.compile(r"^\s*config_version\s*=\s*(.+)", re.MULTILINE)
    ver_re = re.compile(r"([\d]+(?:\.[\d]+)+)")
    try:
        text = ini_path.read_text(encoding="utf-8", errors="replace")
        m = cv_re.search(text)
        if m:
            vm = ver_re.search(m.group(1))
            if vm:
                return vm.group(1)
    except OSError:
        pass
    return None


def squash_all_slic3r_vendors(
    slicer_dir: Path,
    profile_type: ProfileType | None = None,
    filament_library_name: str | None = None,
) -> list[Path]:
    """
    Squash profiles for all vendors in a slic3r-based slicer directory.

    Handles the shared library pattern used by OrcaSlicer:
    - The library directory (e.g. OrcaFilamentLibrary/) is squashed first
      without any external library base.
    - All other vendor directories are squashed with the library profiles
      available for inheritance resolution.

    Args:
        slicer_dir: Root slicer directory (e.g. work/orcaslicer/).
        profile_type: If set, only squash this profile type.
        filament_library_name: Name of shared library directory.

    Returns:
        List of all created JSON file paths.
    """
    if not slicer_dir.exists():
        return []

    filament_library_dir = None
    if filament_library_name:
        filament_library_dir = slicer_dir / filament_library_name

    all_created: list[Path] = []

    # Squash all vendor directories (except the library) with the library
    # available for inheritance resolution. The library dir is kept intact
    # so it can be used as a base for all vendors.
    for vendor_dir in sorted(slicer_dir.iterdir()):
        if not vendor_dir.is_dir():
            continue
        # Skip the library directory — it's only used as a base for resolution,
        # not squashed/exported on its own (matches original behavior)
        if filament_library_name and vendor_dir.name == filament_library_name:
            continue

        created = squash_slic3r_profiles(vendor_dir, profile_type, filament_library_dir)
        all_created.extend(created)

    return all_created


# --- Internal helpers ---


def _load_ini_config(path: Path) -> IniConfig | None:
    """Load an INI config, handling malformed files."""
    try:
        return IniConfig(path)
    except ParseError as e:
        if e.msg == "unexpected value continuation":
            with fileinput.FileInput(path, inplace=True) as f:
                for line in f:
                    print(line.strip())
            return IniConfig(path)
        return None


def _load_json_from_folder(
    folder: Path,
    profile_type_filter: ProfileType | None = None,
) -> dict[str, tuple[Path, dict]]:
    """
    Recursively load all JSON profiles from a folder.

    When profile_type_filter is set, files WITH a "type" field that don't match
    the filter are skipped. Files WITHOUT a "type" field (gcode templates, base
    profiles) are always loaded — they're needed for include/inherits resolution.

    Args:
        folder: Directory to scan.
        profile_type_filter: If set, only load profiles of this type.
                           For BBS/Orca, checks the "type" field.

    Returns:
        Dict mapping profile name to (path, data).
    """
    profiles: dict[str, tuple[Path, dict]] = {}

    if not folder.exists():
        return profiles

    for item in folder.iterdir():
        if item.is_dir():
            profiles.update(_load_json_from_folder(item, profile_type_filter))
            continue
        if item.suffix != ".json":
            continue

        try:
            with item.open() as f:
                file_data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        # Apply type filter only when the file has an explicit "type" field.
        # Files without "type" (gcode templates, some base profiles) are always
        # loaded so they're available for include/inherits resolution.
        if profile_type_filter and "type" in file_data:
            raw_type = file_data["type"]
            # Map "process" → "print" for comparison
            if raw_type == "process":
                raw_type = "print"
            if raw_type != profile_type_filter.value:
                continue

        # Extract name
        name: str | None = file_data.get("name")
        if not name:
            name = (
                file_data.get("filament_settings_id")
                or file_data.get("printer_settings_id")
                or file_data.get("print_settings_id")
            )
        if not name:
            continue

        profiles[name] = (item, file_data)

    return profiles
