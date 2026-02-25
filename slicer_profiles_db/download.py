"""
Download and extraction for slicer profiles from GitHub repositories.

Handles ZIP download, extraction, and overlay application for all
supported slicers (BambuStudio, OrcaSlicer, PrusaSlicer, Cura,
ElegooSlicer, SuperSlicer).
"""

import logging
import re
import shutil
import tempfile
from pathlib import Path
from zipfile import ZipFile

import requests

from .models import (
    SlicerType,
    ProfileType,
    SourceConfig,
    VersionInfo,
    DownloadResult,
)
from .progress import NullProgressReporter, ProgressReporter
from .versions import normalize_version

logger = logging.getLogger(__name__)


# Default source configurations for each slicer
DEFAULT_CONFIGS: dict[SlicerType, SourceConfig] = {
    SlicerType.BAMBUSTUDIO: SourceConfig(
        slicer=SlicerType.BAMBUSTUDIO,
        github_repo="bambulab/BambuStudio",
        profile_path_in_repo="resources/profiles",
        tag_pattern=r"^v\d+",
        nightly_branch="master",
        min_version="02.04.00.70",
        profile_type_dirs={
            ProfileType.FILAMENT: "filament",
            ProfileType.MACHINE: "machine",
            ProfileType.MACHINE_MODEL: "machine",
            ProfileType.PRINT: "process",
        },
    ),
    SlicerType.ORCASLICER: SourceConfig(
        slicer=SlicerType.ORCASLICER,
        github_repo="SoftFever/OrcaSlicer",
        profile_path_in_repo="resources/profiles",
        tag_pattern=r"^v\d+",
        min_version="2.2.0",
        filament_library_name="OrcaFilamentLibrary",
        profile_type_dirs={
            ProfileType.FILAMENT: "filament",
            ProfileType.MACHINE: "machine",
            ProfileType.MACHINE_MODEL: "machine",
            ProfileType.PRINT: "process",
        },
    ),
    SlicerType.PRUSASLICER: SourceConfig(
        slicer=SlicerType.PRUSASLICER,
        github_repo="prusa3d/PrusaSlicer-settings-prusa-fff",
        branch="main",
        ini_bundle=True,
        min_version="2.4.0",
        additional_repos=["prusa3d/PrusaSlicer-settings-non-prusa-fff"],
    ),
    SlicerType.CURA: SourceConfig(
        slicer=SlicerType.CURA,
        github_repo="Ultimaker/fdm_materials",
        branch="master",
        additional_repos=["Ultimaker/cura"],
    ),
    SlicerType.ELEGOOSLICER: SourceConfig(
        slicer=SlicerType.ELEGOOSLICER,
        github_repo="ELEGOO-3D/ElegooSlicer",
        profile_path_in_repo="resources/profiles",
        tag_pattern=r"^v\d+",
        min_version="1.1.5",
        profile_type_dirs={
            ProfileType.FILAMENT: "filament",
            ProfileType.MACHINE: "machine",
            ProfileType.MACHINE_MODEL: "machine",
            ProfileType.PRINT: "process",
        },
    ),
    SlicerType.SUPERSLICER: SourceConfig(
        slicer=SlicerType.SUPERSLICER,
        github_repo="slic3r/slic3r-profiles",
        branch="main",
        ini_bundle=True,
        min_version="0.1.0",
    ),
}


def get_source_config(slicer: SlicerType) -> SourceConfig:
    """Get the default source configuration for a slicer."""
    return DEFAULT_CONFIGS[slicer]


def _looks_like_branch(version: str) -> bool:
    """Return True if the version string looks like a branch name, not a tag."""
    return version in ("main", "master", "develop", "dev")


def _build_zip_url(repo: str, version: str | None, branch: str | None) -> str:
    """Build the GitHub ZIP download URL."""
    if branch:
        return f"https://github.com/{repo}/archive/refs/heads/{branch}.zip"
    return f"https://github.com/{repo}/archive/refs/tags/{version}.zip"


def _zip_root_prefix(repo: str, version: str | None, branch: str | None) -> str:
    """Compute the expected root directory name inside the ZIP archive."""
    repo_name = repo.split("/")[-1]
    if branch:
        return f"{repo_name}-{branch}"
    # Tagged releases: strip the 'v' prefix from version for directory name
    ver = normalize_version(version) if version else ""
    return f"{repo_name}-{ver}"


def download_and_extract(
    config: SourceConfig,
    version: str,
    output_dir: Path,
    profile_types: list[ProfileType] | None = None,
    reporter: ProgressReporter | None = None,
) -> DownloadResult:
    """
    Download ZIP from GitHub, extract profile files.

    Extracts all profile types, preserving the directory structure including
    type subdirs.  Version-aware: tagged releases for BBS/Orca, branch HEAD
    for PrusaSlicer/Cura.

    Args:
        config: Source configuration for the slicer.
        version: Version string (tag name or "latest").
        output_dir: Base output directory.
        profile_types: If set, only extract these profile types.

    Returns:
        DownloadResult with extraction details.
    """
    slicer_output = output_dir / config.slicer.value
    if slicer_output.exists():
        shutil.rmtree(slicer_output)
    slicer_output.mkdir(parents=True, exist_ok=True)

    version_info = VersionInfo(
        raw=version,
        normalized=normalize_version(version),
        slicer=config.slicer,
    )

    _reporter = reporter or NullProgressReporter()
    types_found: set[ProfileType] = set()

    # Determine whether to download by tag or by branch.
    # config.branch is set for slicers that always use a branch (PrusaSlicer, Cura).
    # For tag-based slicers, the version may be a branch name when doing a nightly
    # fetch (e.g. version="main").  Detect this by checking if the version looks
    # like a branch name rather than a version tag.
    use_branch = config.branch or _looks_like_branch(version)
    tag_version = None if use_branch else version
    branch = config.branch or (version if use_branch else None)

    # Download and extract main repo
    _extract_repo(
        config.github_repo,
        tag_version,
        branch,
        config.profile_path_in_repo,
        config.profile_type_dirs,
        slicer_output,
        config,
        profile_types,
        types_found,
        _reporter,
    )

    # Download and extract additional repos
    for extra_repo in config.additional_repos:
        _extract_repo(
            extra_repo,
            tag_version,
            branch or "main",
            config.profile_path_in_repo,
            config.profile_type_dirs,
            slicer_output,
            config,
            profile_types,
            types_found,
            _reporter,
        )

    return DownloadResult(
        slicer=config.slicer,
        version=version_info,
        extracted_dir=slicer_output,
        profile_types_found=list(types_found),
    )


def _download_zip(
    url: str,
    dest_path: Path,
    reporter: ProgressReporter,
    max_retries: int = 3,
) -> None:
    """Download a ZIP file with streaming progress and retry on transient failures."""
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=120)
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            filename = url.split("/")[-1]
            bar = reporter.create_download_bar(total, f"Downloading {filename}")
            try:
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bar.update(len(chunk))
            finally:
                bar.close()
            return
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise  # Don't retry 404s
            last_error = e
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e

        if attempt < max_retries:
            logger.warning("Download attempt %d/%d failed: %s", attempt, max_retries, last_error)

    raise last_error  # type: ignore[misc]


def _extract_repo(
    repo: str,
    version: str | None,
    branch: str | None,
    profile_path_in_repo: str,
    profile_type_dirs: dict[ProfileType, str],
    slicer_output: Path,
    config: SourceConfig,
    profile_types: list[ProfileType] | None,
    types_found: set[ProfileType],
    reporter: ProgressReporter,
) -> None:
    """Download and extract a single repository."""
    url = _build_zip_url(repo, version, branch)
    zip_file_path = Path(tempfile.mktemp(suffix=".zip"))
    try:
        _download_zip(url, zip_file_path, reporter)
        zip_root = _zip_root_prefix(repo, version, branch)

        # Build the member prefix (path inside ZIP to start extracting from)
        if profile_path_in_repo:
            member = f"{zip_root}/{profile_path_in_repo}"
        else:
            member = zip_root

        if not member.endswith("/"):
            member += "/"

        # Build file patterns based on config properties
        if config.ini_bundle:
            # INI-bundle slicers (PrusaSlicer, SuperSlicer)
            pattern = re.compile(r".*\.(ini|idx|stl|svg|png|json)$")
        elif config.slicer == SlicerType.CURA:
            if "fdm_materials" in repo:
                pattern = re.compile(r".*\.fdm_material$")
            else:
                # Cura main repo — extract definitions
                member = f"{zip_root}/resources/definitions/"
                pattern = re.compile(r".*\.def\.json$")
        elif profile_type_dirs:
            # JSON-profile slicers (BambuStudio, OrcaSlicer, ElegooSlicer)
            if profile_types:
                type_dirs = [
                    profile_type_dirs[pt] for pt in profile_types if pt in profile_type_dirs
                ]
                dir_pattern = "|".join(re.escape(d) for d in type_dirs)
                pattern = re.compile(rf".*/(?:.*\.(?:stl|svg|png)|(?:{dir_pattern})/.*)")
            else:
                dirs = "|".join(re.escape(d) for d in profile_type_dirs.values())
                pattern = re.compile(rf".*/(?:.*\.(?:stl|svg|png)|(?:{dirs})/.*)")
        else:
            # Fallback: extract everything
            pattern = re.compile(r".*")

        with ZipFile(zip_file_path) as zip_f:
            # Discover actual root prefix (GitHub may use different casing)
            for _entry in zip_f.namelist():
                if "/" in _entry:
                    actual_root = _entry.split("/")[0]
                    if actual_root != zip_root:
                        member = member.replace(zip_root, actual_root, 1)
                        zip_root = actual_root
                    break

            for file in zip_f.namelist():
                if not file.startswith(member) or file.endswith("/"):
                    continue
                if not pattern.match(file):
                    continue

                rel_path = Path(file).relative_to(member)
                dest_path = slicer_output / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)

                with zip_f.open(file) as src, open(dest_path, "wb") as dst:
                    dst.write(src.read())

                # Track which profile types were found
                for pt, dir_name in profile_type_dirs.items():
                    if dir_name in rel_path.parts:
                        types_found.add(pt)
    finally:
        zip_file_path.unlink(missing_ok=True)


def apply_overlays(extracted_dir: Path, overlay_dir: Path, slicer: SlicerType) -> None:
    """
    Copy overlay profiles into the extracted directory after squashing.

    Overlay profiles are applied post-squash, so they bypass inheritance
    resolution entirely.  They must be complete (pre-squashed) profiles.

    Overlay structure: overlay_dir/{slicer}/{vendor}/...
    extracted_dir is already slicer-specific (e.g. work/bambustudio), so we
    navigate directly to overlay_dir/{slicer}/ and copy vendor dirs into
    extracted_dir without extra nesting.

    Copies all files (not just *.json) — overlays may include .stl, .png, .svg
    assets alongside profile JSONs.
    """
    slicer_overlay = overlay_dir / slicer.value
    if not slicer_overlay.exists():
        return

    for vendor_dir in slicer_overlay.iterdir():
        if not vendor_dir.is_dir():
            continue
        dest_vendor = extracted_dir / vendor_dir.name
        dest_vendor.mkdir(parents=True, exist_ok=True)
        for overlay_file in vendor_dir.rglob("*"):
            if overlay_file.is_dir():
                continue
            rel = overlay_file.relative_to(vendor_dir)
            dest = dest_vendor / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(overlay_file, dest)
