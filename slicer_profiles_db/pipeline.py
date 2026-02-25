"""
Pipeline orchestrator: download → squash → parse → store.

High-level interface that chains all the slicer profile processing steps.
"""

import json
import logging
import shutil
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

from .models import (
    SlicerType,
    ProfileType,
    IngestionReport,
)
from .progress import NullProgressReporter, ProgressReporter
from .store import ProfileStore
from .download import (
    download_and_extract,
    apply_overlays,
    get_source_config,
)
from .squash import (
    unpack_prusaslicer_bundles,
    squash_all_slic3r_vendors,
    iter_ini_bundle_versions,
    split_prusaslicer_bundle,
)
from .defaults import fetch_slicer_defaults
from .resources import (
    ResourceStore,
    collect_resources,
    rewrite_resource_refs,
    collect_referenced_hashes,
)
from .versions import (
    enumerate_github_tags,
    normalize_version,
    is_prerelease,
    sort_versions,
    version_key,
)
from .parsers import (
    BambuStudioParser,
    OrcaSlicerParser,
    PrusaSlicerParser,
    CuraParser,
    ElegooSlicerParser,
    SuperSlicerParser,
)
from .parsers.base import BaseParser

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when downloading slicer profiles from GitHub fails."""


class ParseError(Exception):
    """Raised when parsing slicer profiles fails."""


class StoreError(Exception):
    """Raised when storing profiles to disk fails."""


class ProfilePipeline:
    """
    Orchestrator that chains: download → extract → squash → parse → store.

    Usage:
        store = ProfileStore("/path/to/store")
        pipeline = ProfilePipeline(store)

        # Ingest latest version of a slicer
        report = pipeline.ingest(SlicerType.BAMBUSTUDIO, version="v02.05.00.66")

        # Ingest all tagged versions
        reports = pipeline.ingest_all_versions(SlicerType.BAMBUSTUDIO)
    """

    def __init__(
        self,
        store: ProfileStore,
        overlay_dir: Path | None = None,
        work_dir: Path | None = None,
        reporter: ProgressReporter | None = None,
    ):
        self.store = store
        self.overlay_dir = overlay_dir
        self.work_dir = work_dir
        self.reporter: ProgressReporter = reporter or NullProgressReporter()
        self._parsers: dict[SlicerType, BaseParser] = {
            SlicerType.BAMBUSTUDIO: BambuStudioParser(),
            SlicerType.ORCASLICER: OrcaSlicerParser(),
            SlicerType.PRUSASLICER: PrusaSlicerParser(),
            SlicerType.CURA: CuraParser(),
            SlicerType.ELEGOOSLICER: ElegooSlicerParser(),
            SlicerType.SUPERSLICER: SuperSlicerParser(),
        }

    def ingest(
        self,
        slicer: SlicerType,
        version: str = "latest",
        profile_types: list[ProfileType] | None = None,
        fetch_defaults: bool = False,
    ) -> IngestionReport:
        """
        Full pipeline: download → extract → squash → parse → store.

        Args:
            slicer: Which slicer to process.
            version: Version tag string, or "latest" to use the most recent tag.
            profile_types: If set, only process these profile types.
            fetch_defaults: If True, also fetch and store slicer defaults.

        Returns:
            IngestionReport with details of what was added/changed/removed.
        """
        config = get_source_config(slicer)

        # Resolve special version keywords
        is_nightly = version == "nightly"
        if version == "latest":
            self.reporter.update_status(f"Resolving latest version for {slicer.value}...")
            version = self._resolve_latest_version(slicer)
        elif is_nightly:
            version = self._resolve_nightly_version(slicer)

        # Skip immutable versions that are already in the store.
        # Mutable versions (branches, nightly) are always re-processed.
        if not self._is_version_mutable(version):
            normalized = normalize_version(version)
            existing = self.store.get_versions(slicer)
            if normalized in existing:
                self.reporter.update_status(
                    f"Skipping {slicer.value} {normalized} (already ingested)"
                )
                return IngestionReport(
                    slicer=slicer,
                    version=normalized,
                    profiles_processed=0,
                    unchanged=0,
                )

        # Create temp working directory
        created_temp = False
        if self.work_dir:
            work = self.work_dir
            work.mkdir(parents=True, exist_ok=True)
        else:
            work = Path(tempfile.mkdtemp(prefix=f"ofd-slicer-{slicer.value}-"))
            created_temp = True

        try:
            return self._run_pipeline(
                slicer, config, version, work, profile_types, fetch_defaults, is_nightly
            )
        finally:
            if created_temp and work.exists():
                shutil.rmtree(work, ignore_errors=True)

    def _run_pipeline(
        self,
        slicer: SlicerType,
        config,
        version: str,
        work: Path,
        profile_types: list[ProfileType] | None,
        fetch_defaults: bool,
        is_nightly: bool,
    ) -> IngestionReport:
        """Execute the pipeline steps (separated for clean temp dir cleanup)."""

        # Step 1: Download and extract
        self.reporter.update_status(f"Downloading {slicer.value} {version}...")
        result = download_and_extract(config, version, work, profile_types, reporter=self.reporter)
        extracted = result.extracted_dir

        # Step 2: Collect and persist resource files (STL/SVG/PNG)
        # Must happen BEFORE squash — squash_slic3r_profiles does rmtree on
        # each vendor dir, which destroys resource files alongside the JSONs.
        resource_store = ResourceStore(self.store.root / slicer.value / "_resources")
        resource_map = collect_resources(extracted, resource_store)

        # Step 3: Squash inheritance
        detected_version: str | None = None
        if config.ini_bundle:
            self.reporter.update_status(f"Squashing INI bundles for {slicer.value}...")
            _, detected_version = unpack_prusaslicer_bundles(extracted)
        elif config.profile_type_dirs:
            self.reporter.update_status(f"Resolving inheritance for {slicer.value}...")
            squash_all_slic3r_vendors(
                extracted,
                profile_type=profile_types[0] if profile_types and len(profile_types) == 1 else None,
                filament_library_name=config.filament_library_name,
            )
            # Remove the shared library dir after squashing — it was only
            # needed for inheritance resolution and should not be parsed
            if config.filament_library_name:
                lib_dir = extracted / config.filament_library_name
                if lib_dir.exists():
                    shutil.rmtree(lib_dir)

        # Step 4: Apply overlays (after squash so they bypass inheritance
        # resolution — overlay profiles should be complete/pre-squashed)
        if self.overlay_dir:
            apply_overlays(extracted, self.overlay_dir, slicer)

        # Step 5: Parse
        self.reporter.update_status(f"Parsing {slicer.value} profiles...")
        parser = self._parsers[slicer]
        parsed = list(parser.parse_directory(extracted, profile_type_filter=profile_types))

        # Step 6: Rewrite resource references in parsed profiles
        if resource_map:
            rewrite_resource_refs(parsed, resource_map)

        # Step 7: Determine storage version
        # For INI-bundle slicers, use the version detected from bundle filenames/content
        # instead of the branch name (e.g. "2.4.9" instead of "main").
        # For other branch-based slicers without version detection, use a date stamp.
        normalized_version = normalize_version(version)
        if normalized_version in ("main", "master"):
            if detected_version:
                normalized_version = detected_version
            else:
                normalized_version = date.today().isoformat()
        if is_nightly:
            normalized_version = f"nightly-{normalized_version}"

        # Step 8: Store
        self.reporter.update_status(
            f"Storing {len(parsed)} {slicer.value} profiles (version {normalized_version})..."
        )
        report = self.store.ingest_profiles(slicer, normalized_version, parsed)

        # Step 9: Garbage-collect orphaned resources
        referenced = collect_referenced_hashes(self.store.root, slicer.value)
        resource_store.gc(referenced)

        # Optional: fetch defaults
        if fetch_defaults:
            defaults = fetch_slicer_defaults(slicer)
            if defaults:
                defaults_dir = self.store.root / slicer.value
                defaults_dir.mkdir(parents=True, exist_ok=True)
                (defaults_dir / "defaults.json").write_text(
                    json.dumps(defaults, indent=4), encoding="utf-8"
                )

        return report

    def ingest_all_ini_versions(
        self,
        slicer: SlicerType,
        profile_types: list[ProfileType] | None = None,
    ) -> list[IngestionReport]:
        """
        Download an INI-bundle repo once and ingest all vendor-version combinations.

        For branch-based slicers (PrusaSlicer, SuperSlicer) whose repos contain
        multiple historical INI versions per vendor.  Downloads the branch once,
        then iterates all versions oldest-to-newest.

        Args:
            slicer: Which slicer to process.
            profile_types: If set, only process these profile types.

        Returns:
            List of IngestionReport, one per version.
        """
        config = get_source_config(slicer)

        # Create temp working directory
        created_temp = False
        if self.work_dir:
            work = self.work_dir
            work.mkdir(parents=True, exist_ok=True)
        else:
            work = Path(tempfile.mkdtemp(prefix=f"ofd-slicer-{slicer.value}-"))
            created_temp = True

        try:
            # Step 1: Download branch once
            self.reporter.update_status(f"Downloading {slicer.value} {config.branch}...")
            result = download_and_extract(config, config.branch, work, profile_types, reporter=self.reporter)
            extracted = result.extracted_dir

            # Step 2: Enumerate all version groups
            self.reporter.update_status(f"Enumerating INI bundle versions for {slicer.value}...")
            version_groups = iter_ini_bundle_versions(extracted, config.min_version)

            if not version_groups:
                self.reporter.update_status(f"No INI versions found for {slicer.value}")
                return []

            self.reporter.update_status(
                f"Found {len(version_groups)} versions for {slicer.value}"
            )

            # Step 3: Collect resources once (outside the version loop)
            resource_store = ResourceStore(self.store.root / slicer.value / "_resources")
            resource_map = collect_resources(extracted, resource_store)

            # Step 4: Process each version oldest-to-newest
            parser = self._parsers[slicer]
            reports: list[IngestionReport] = []

            for i, (ver, vendor_inis) in enumerate(version_groups, 1):
                self.reporter.step(f"{slicer.value} {ver}", i, len(version_groups))

                # Create a temp split directory for this version
                split_dir = work / f"_split_{ver}"
                split_dir.mkdir(parents=True, exist_ok=True)

                try:
                    # Split each vendor's INI bundle
                    for vendor_name, ini_path in vendor_inis:
                        vendor_out = split_dir / vendor_name
                        vendor_out.mkdir(parents=True, exist_ok=True)
                        split_prusaslicer_bundle(ini_path, vendor_out)

                    # Apply overlays after split (overlay profiles should
                    # be complete/pre-squashed)
                    if self.overlay_dir:
                        apply_overlays(split_dir, self.overlay_dir, slicer)

                    # Parse the split directory
                    parsed = list(parser.parse_directory(
                        split_dir, profile_type_filter=profile_types
                    ))

                    # Rewrite resource references
                    if resource_map:
                        rewrite_resource_refs(parsed, resource_map)

                    # Store
                    self.reporter.update_status(
                        f"Storing {len(parsed)} {slicer.value} profiles (version {ver})..."
                    )
                    report = self.store.ingest_profiles(slicer, ver, parsed)
                    reports.append(report)
                except Exception as e:
                    self.reporter.update_status(
                        f"Failed to ingest {slicer.value} {ver}: {e}"
                    )
                    continue
                finally:
                    # Clean up split dir
                    if split_dir.exists():
                        shutil.rmtree(split_dir, ignore_errors=True)

            # Garbage-collect orphaned resources
            referenced = collect_referenced_hashes(self.store.root, slicer.value)
            resource_store.gc(referenced)

            return reports

        finally:
            if created_temp and work.exists():
                shutil.rmtree(work, ignore_errors=True)

    def ingest_all_versions(
        self,
        slicer: SlicerType,
        profile_types: list[ProfileType] | None = None,
    ) -> list[IngestionReport]:
        """
        Enumerate all versions for a slicer, ingest each in order.

        For INI-bundle slicers (PrusaSlicer, SuperSlicer), downloads the branch
        once and processes all INI versions.  For tag-based slicers, enumerates
        GitHub tags and ingests each.

        Args:
            slicer: Which slicer to process.
            profile_types: If set, only process these profile types.

        Returns:
            List of IngestionReport, one per version.
        """
        config = get_source_config(slicer)

        # INI-bundle slicers: download once, process all versions from the bundle
        if config.ini_bundle:
            return self.ingest_all_ini_versions(slicer, profile_types)

        # Branch-only slicers without tag enumeration (e.g. Cura): just ingest HEAD
        if config.branch and not config.tag_pattern:
            report = self.ingest(slicer, "latest", profile_types)
            return [report]

        # Tag-based slicers: enumerate tags and ingest each
        tags = enumerate_github_tags(
            config.github_repo,
            tag_pattern=config.tag_pattern,
            slicer=slicer,
        )

        # Filter pre-releases, sort, and apply min_version filter
        stable_tags = [t for t in tags if not is_prerelease(t.raw)]
        tag_map = {t.normalized: t for t in stable_tags}
        sorted_versions = sort_versions(list(tag_map.keys()))

        if config.min_version:
            min_key = version_key(config.min_version)
            sorted_versions = [v for v in sorted_versions if version_key(v) >= min_key]

        # Pre-filter: skip tags already in the store (they're immutable)
        existing_versions = set(self.store.get_versions(slicer))
        new_versions = [v for v in sorted_versions if v not in existing_versions]
        if len(new_versions) < len(sorted_versions):
            skipped = len(sorted_versions) - len(new_versions)
            self.reporter.update_status(
                f"Skipping {skipped} already-ingested versions for {slicer.value}"
            )

        reports = []
        for i, norm_ver in enumerate(new_versions, 1):
            tag = tag_map[norm_ver]
            self.reporter.step(f"{slicer.value} {tag.raw}", i, len(new_versions))
            try:
                report = self.ingest(slicer, tag.raw, profile_types)
                reports.append(report)
            except Exception as e:
                self.reporter.update_status(f"Failed to ingest {slicer.value} {tag.raw}: {e}")
                continue

        return reports

    @staticmethod
    def _is_version_mutable(version: str) -> bool:
        """Return True if this version should always be re-processed.

        Branch names and nightly builds are mutable — their content changes
        over time.  Tagged releases are immutable and can be skipped once
        they've been ingested.
        """
        normalized = normalize_version(version)
        if normalized in ("main", "master", "develop", "dev"):
            return True
        if normalized.startswith("nightly"):
            return True
        return False

    def _resolve_latest_version(self, slicer: SlicerType) -> str:
        """Resolve 'latest' to the most recent stable version.

        For tag-based slicers, returns the highest stable tag.
        For branch-based slicers (PrusaSlicer, Cura, SuperSlicer), downloads
        from HEAD — the actual version is detected later from INI bundle
        filenames/content during the squash step.
        """
        config = get_source_config(slicer)

        if config.branch and not config.tag_pattern:
            # Branch-only slicers: download HEAD, version detected during squash
            return config.branch

        tags = enumerate_github_tags(
            config.github_repo,
            tag_pattern=config.tag_pattern,
            slicer=slicer,
        )
        if not tags:
            if config.branch:
                return config.branch
            raise ValueError(f"No tags found for {slicer.value} in {config.github_repo}")

        stable_tags = [t for t in tags if not is_prerelease(t.raw)]
        if not stable_tags:
            stable_tags = tags
        tag_map = {t.normalized: t for t in stable_tags}
        sorted_ver = sort_versions(list(tag_map.keys()))
        return tag_map[sorted_ver[-1]].raw

    def _resolve_nightly_version(self, slicer: SlicerType) -> str:
        """Resolve 'nightly' to the branch HEAD for any slicer.

        Always downloads from the default branch, giving bleeding-edge profiles.
        """
        config = get_source_config(slicer)
        if config.branch:
            return config.branch
        return config.nightly_branch
