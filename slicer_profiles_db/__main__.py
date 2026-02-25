"""
slicer_profiles_db CLI — Manage slicer profile ingestion, diffing, and mapping.

Usage:
    slicer-profiles-db <command> [options]
    python -m slicer_profiles_db <command> [options]
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from slicer_profiles_db import SlicerType, ProfileType, ProfileStore

logger = logging.getLogger(__name__)


def _find_project_root() -> Path:
    """Find the project root by looking for pyproject.toml."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path.cwd()


project_root = _find_project_root()


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        prog="slicer-profiles-db",
        description="Slicer profile ingestion, diffing, and mapping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  slicer-profiles-db ingest bambustudio --version latest
  slicer-profiles-db ingest-all --clean --version latest --overlay overlay
  slicer-profiles-db ingest-local bambustudio v02.05.00.66 profiles/bambustudio
  slicer-profiles-db diff bambustudio v02.04.00 v02.05.00.66
  slicer-profiles-db versions bambustudio
  slicer-profiles-db list bambustudio --type filament
  slicer-profiles-db evaluate bambustudio v02.05.00.66 BBL "Bambu ABS"

Environment variables:
  GITHUB_TOKEN              GitHub API token (increases rate limit)
  SP_API_URL                SimplyPrint printer model endpoint URL
  SLICER_PROFILES_STORE     Default store directory (instead of "profiles")
  SLICER_PROFILES_OVERLAY   Default overlay directory (instead of "overlay")
        """,
    )

    parser.add_argument(
        "--verbose", "-V", action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress non-error output (logging only)",
    )

    subparsers = parser.add_subparsers(
        title="commands",
        dest="command",
        required=True,
        metavar="<command>",
    )

    # --- ingest-local ---
    ingest_parser = subparsers.add_parser(
        "ingest-local",
        help="Ingest profiles from a local slicer directory into the store",
    )
    ingest_parser.add_argument(
        "slicer",
        choices=[s.value for s in SlicerType],
        help="Slicer type",
    )
    ingest_parser.add_argument("version", help="Version string (e.g. v02.05.00.66)")
    ingest_parser.add_argument(
        "profiles_dir", type=Path, help="Path to slicer profiles directory"
    )
    ingest_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    ingest_parser.add_argument(
        "--json", action="store_true", help="Output report as JSON"
    )
    ingest_parser.set_defaults(func=run_ingest_local)

    # --- ingest ---
    pipeline_parser = subparsers.add_parser(
        "ingest",
        help="Download, squash, parse, and store profiles from GitHub",
    )
    pipeline_parser.add_argument(
        "slicer",
        choices=[s.value for s in SlicerType],
        help="Slicer type",
    )
    pipeline_parser.add_argument(
        "--version",
        "-v",
        default="latest",
        help="Version tag to ingest: a tag name, 'latest' (newest stable), or 'nightly' (branch HEAD)",
    )
    pipeline_parser.add_argument(
        "--type",
        nargs="*",
        choices=[pt.value for pt in ProfileType],
        help="Profile types to process (default: all)",
    )
    pipeline_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    pipeline_parser.add_argument(
        "--overlay",
        default=None,
        help="Overlay directory path (default: $SLICER_PROFILES_OVERLAY or 'overlay')",
    )
    pipeline_parser.add_argument(
        "--fetch-defaults",
        action="store_true",
        help="Also fetch and store slicer defaults",
    )
    pipeline_parser.add_argument(
        "--all-versions",
        action="store_true",
        help="Ingest all available tagged versions (respects min_version)",
    )
    pipeline_parser.add_argument(
        "--min-version",
        default=None,
        help="Override minimum version to ingest (normalized, e.g. '02.04.00.70')",
    )
    pipeline_parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing profiles for this slicer before ingesting (clean run)",
    )
    pipeline_parser.add_argument(
        "--json", action="store_true", help="Output report as JSON"
    )
    pipeline_parser.set_defaults(func=run_ingest)

    # --- ingest-all ---
    pipeline_all_parser = subparsers.add_parser(
        "ingest-all",
        help="Ingest profiles for all slicers sequentially from GitHub",
    )
    pipeline_all_parser.add_argument(
        "--version",
        "-v",
        default="latest",
        help="Version tag to ingest (default: latest)",
    )
    pipeline_all_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    pipeline_all_parser.add_argument(
        "--overlay",
        default=None,
        help="Overlay directory path (default: $SLICER_PROFILES_OVERLAY or 'overlay')",
    )
    pipeline_all_parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete each slicer's profiles just before re-ingesting (clean run)",
    )
    pipeline_all_parser.add_argument(
        "--fetch-defaults",
        action="store_true",
        help="Also fetch and store slicer defaults",
    )
    pipeline_all_parser.add_argument(
        "--all-versions",
        action="store_true",
        help="Ingest all available versions for each slicer",
    )
    pipeline_all_parser.add_argument(
        "--skip",
        nargs="*",
        choices=[s.value for s in SlicerType],
        default=[],
        help="Slicers to skip",
    )
    pipeline_all_parser.add_argument(
        "--json", action="store_true", help="Output report as JSON"
    )
    pipeline_all_parser.set_defaults(func=run_ingest_all)

    # --- diff ---
    diff_parser = subparsers.add_parser(
        "diff",
        help="Show setting changes between two versions",
    )
    diff_parser.add_argument(
        "slicer",
        choices=[s.value for s in SlicerType],
        help="Slicer type",
    )
    diff_parser.add_argument("from_version", help="Starting version")
    diff_parser.add_argument("to_version", help="Ending version")
    diff_parser.add_argument(
        "--profile", help="Filter to a specific profile name"
    )
    diff_parser.add_argument("--vendor", help="Vendor (required with --profile)")
    diff_parser.add_argument(
        "--type",
        default="filament",
        help="Profile type (default: filament)",
    )
    diff_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    diff_parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    diff_parser.set_defaults(func=run_diff)

    # --- versions ---
    versions_parser = subparsers.add_parser(
        "versions",
        help="List ingested versions for a slicer",
    )
    versions_parser.add_argument(
        "slicer",
        choices=[s.value for s in SlicerType],
        help="Slicer type",
    )
    versions_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    versions_parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    versions_parser.set_defaults(func=run_versions)

    # --- list ---
    list_parser = subparsers.add_parser(
        "list",
        help="List stored profiles for a slicer",
    )
    list_parser.add_argument(
        "slicer",
        choices=[s.value for s in SlicerType],
        help="Slicer type",
    )
    list_parser.add_argument(
        "--type",
        default=None,
        help="Filter by profile type",
    )
    list_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    list_parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    list_parser.set_defaults(func=run_list)

    # --- evaluate ---
    eval_parser = subparsers.add_parser(
        "evaluate",
        help="Snapshot a profile's settings at a specific version",
    )
    eval_parser.add_argument(
        "slicer",
        choices=[s.value for s in SlicerType],
        help="Slicer type",
    )
    eval_parser.add_argument("version", help="Version to evaluate at")
    eval_parser.add_argument("vendor", help="Vendor name")
    eval_parser.add_argument("profile", help="Profile name")
    eval_parser.add_argument(
        "--type",
        default="filament",
        help="Profile type (default: filament)",
    )
    eval_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    eval_parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    eval_parser.set_defaults(func=run_evaluate)

    # --- map ---
    map_parser = subparsers.add_parser(
        "map",
        help="Run the full mapping pipeline: match models → map filaments → map print profiles → export",
    )
    map_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    map_parser.add_argument(
        "--output",
        "-o",
        default="out",
        help="Output directory path (default: out)",
    )
    map_parser.add_argument(
        "--skip",
        nargs="*",
        choices=[s.value for s in SlicerType],
        default=[],
        help="Slicers to skip",
    )
    map_parser.add_argument(
        "--ofd-path",
        default=None,
        help="Path to OFD repo data/ dir (resolves filament_db_ids to OFD paths)",
    )
    map_parser.add_argument(
        "--json", action="store_true", help="Output report as JSON"
    )
    map_parser.set_defaults(func=run_map)

    # --- ofd-map ---
    ofd_map_parser = subparsers.add_parser(
        "ofd-map",
        help="Run OFD forward mapping: derive slicer_settings/slicer_ids from profile store",
    )
    ofd_map_parser.add_argument(
        "--ofd-path",
        required=True,
        help="Path to OFD repo data/ dir",
    )
    ofd_map_parser.add_argument(
        "--store",
        "-s",
        default=None,
        help="Store directory path (default: $SLICER_PROFILES_STORE or 'profiles')",
    )
    ofd_map_parser.add_argument(
        "--slicer",
        nargs="*",
        choices=[s.value for s in SlicerType],
        default=None,
        help="Slicer(s) to map (default: all with profiles)",
    )
    ofd_map_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing",
    )
    ofd_map_parser.add_argument(
        "--brand",
        default=None,
        help="Filter to a single brand_id",
    )
    ofd_map_parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    ofd_map_parser.set_defaults(func=run_ofd_map)

    return parser


def run_ingest_local(args: argparse.Namespace) -> int:
    """Execute the ingest-local command — ingest from a local directory."""
    store_path = project_root / (args.store or _default_store())
    profiles_dir = project_root / args.profiles_dir

    if not profiles_dir.exists():
        logger.error("Profiles directory '%s' does not exist", profiles_dir)
        return 1

    store = ProfileStore(store_path)
    slicer = SlicerType(args.slicer)

    print(f"Ingesting {slicer.value} {args.version} from {profiles_dir}...")
    report = store.ingest(slicer, args.version, profiles_dir)

    if getattr(args, "json", False):
        print(json.dumps({
            "slicer": report.slicer.value,
            "version": report.version,
            "profiles_processed": report.profiles_processed,
            "added": report.added,
            "removed": report.removed,
            "changed": report.changed,
            "unchanged": report.unchanged,
        }, indent=2))
    else:
        print(f"\nIngestion complete:")
        print(f"  Profiles processed: {report.profiles_processed}")
        print(f"  Added:     {len(report.added)}")
        print(f"  Changed:   {len(report.changed)}")
        print(f"  Unchanged: {report.unchanged}")
        print(f"  Removed:   {len(report.removed)}")

        if report.added:
            print(f"\nNew profiles:")
            for name in report.added[:20]:
                print(f"  + {name}")
            if len(report.added) > 20:
                print(f"  ... and {len(report.added) - 20} more")

        if report.changed:
            print(f"\nChanged profiles:")
            for name, keys in list(report.changed.items())[:20]:
                print(f"  ~ {name} ({len(keys)} settings)")
            if len(report.changed) > 20:
                print(f"  ... and {len(report.changed) - 20} more")

        if report.removed:
            print(f"\nRemoved profiles:")
            for key in report.removed[:20]:
                print(f"  - {key}")

    return 0


def _default_store() -> str:
    """Return the default store path from env or fallback."""
    return os.environ.get("SLICER_PROFILES_STORE", "profiles")


def _default_overlay() -> str:
    """Return the default overlay path from env or fallback."""
    return os.environ.get("SLICER_PROFILES_OVERLAY", "overlay")


def _make_reporter(use_json: bool):
    """Create the appropriate progress reporter."""
    from slicer_profiles_db.progress import RichProgressReporter, NullProgressReporter
    return NullProgressReporter() if use_json else RichProgressReporter()


def run_ingest(args: argparse.Namespace) -> int:
    """Execute the ingest command — download, squash, parse, store from GitHub."""
    import shutil
    from slicer_profiles_db.pipeline import ProfilePipeline
    from slicer_profiles_db.versions import check_github_token

    use_json = getattr(args, "json", False)
    reporter = _make_reporter(use_json)

    # GITHUB_TOKEN check — required for --all-versions (many API calls)
    is_all_versions = getattr(args, "all_versions", False)
    check_github_token(required=is_all_versions)

    store_path = project_root / (args.store or _default_store())
    store = ProfileStore(store_path)
    slicer = SlicerType(args.slicer)

    # Clean run: delete slicer's profile directory
    if getattr(args, "clean", False):
        slicer_dir = store_path / slicer.value
        if slicer_dir.exists():
            shutil.rmtree(slicer_dir)
            reporter.update_status(f"Cleaned {slicer_dir}")

    overlay_val = args.overlay or _default_overlay()
    overlay_dir = Path(overlay_val)
    if not overlay_dir.is_absolute():
        overlay_dir = project_root / overlay_dir
    profile_types = [ProfileType(pt) for pt in args.type] if args.type else None

    pipeline = ProfilePipeline(
        store=store,
        overlay_dir=overlay_dir,
        reporter=reporter,
    )

    # Apply --min-version override if provided
    if args.min_version:
        from slicer_profiles_db.download import DEFAULT_CONFIGS
        config = DEFAULT_CONFIGS[slicer]
        DEFAULT_CONFIGS[slicer] = config.model_copy(update={"min_version": args.min_version})

    if is_all_versions:
        reporter.update_status(f"Ingesting all versions for {slicer.value}...")
        reports = pipeline.ingest_all_versions(slicer, profile_types)
        if use_json:
            print(json.dumps([{
                "slicer": r.slicer.value,
                "version": r.version,
                "profiles_processed": r.profiles_processed,
                "added": len(r.added),
                "changed": len(r.changed),
                "unchanged": r.unchanged,
            } for r in reports], indent=2))
        else:
            print(f"\nIngested {len(reports)} versions:")
            for r in reports:
                print(
                    f"  {r.version}: {r.profiles_processed} profiles "
                    f"({len(r.added)} added, {len(r.changed)} changed)"
                )
    else:
        report = pipeline.ingest(
            slicer,
            version=args.version,
            profile_types=profile_types,
            fetch_defaults=args.fetch_defaults,
        )

        if use_json:
            print(json.dumps({
                "slicer": report.slicer.value,
                "version": report.version,
                "profiles_processed": report.profiles_processed,
                "added": report.added,
                "removed": report.removed,
                "changed": report.changed,
                "unchanged": report.unchanged,
            }, indent=2))
        else:
            print(f"\nPipeline complete:")
            print(f"  Profiles processed: {report.profiles_processed}")
            print(f"  Added:     {len(report.added)}")
            print(f"  Changed:   {len(report.changed)}")
            print(f"  Unchanged: {report.unchanged}")
            print(f"  Removed:   {len(report.removed)}")

    return 0


def run_ingest_all(args: argparse.Namespace) -> int:
    """Execute the ingest-all command — ingest all slicers from GitHub."""
    import shutil
    from slicer_profiles_db.pipeline import ProfilePipeline
    from slicer_profiles_db.versions import check_github_token

    use_json = getattr(args, "json", False)
    reporter = _make_reporter(use_json)

    check_github_token(required=False)

    store_path = project_root / (args.store or _default_store())
    store = ProfileStore(store_path)
    overlay_val = args.overlay or _default_overlay()
    overlay_dir = Path(overlay_val)
    if not overlay_dir.is_absolute():
        overlay_dir = project_root / overlay_dir
    skip_set = set(args.skip) if args.skip else set()

    slicers = [s for s in SlicerType if s.value not in skip_set]

    pipeline = ProfilePipeline(
        store=store,
        overlay_dir=overlay_dir,
        reporter=reporter,
    )

    is_all_versions = getattr(args, "all_versions", False)
    if is_all_versions:
        check_github_token(required=False)

    reports = []
    errors = []
    for i, slicer in enumerate(slicers, 1):
        reporter.step(f"Processing {slicer.value}", i, len(slicers))

        # Clean run: delete this slicer's profile directory just before re-ingesting
        if args.clean:
            slicer_dir = store_path / slicer.value
            if slicer_dir.exists():
                shutil.rmtree(slicer_dir)
                reporter.update_status(f"Cleaned {slicer_dir}")

        try:
            if is_all_versions:
                sub_reports = pipeline.ingest_all_versions(slicer)
                reports.extend(sub_reports)
            else:
                report = pipeline.ingest(
                    slicer,
                    version=args.version,
                    fetch_defaults=getattr(args, "fetch_defaults", False),
                )
                reports.append(report)
        except Exception as e:
            errors.append((slicer.value, str(e)))
            reporter.update_status(f"Error processing {slicer.value}: {e}")

    # Output
    if use_json:
        print(json.dumps({
            "reports": [{
                "slicer": r.slicer.value,
                "version": r.version,
                "profiles_processed": r.profiles_processed,
                "added": len(r.added),
                "changed": len(r.changed),
                "unchanged": r.unchanged,
            } for r in reports],
            "errors": [{"slicer": s, "error": e} for s, e in errors],
        }, indent=2))
    else:
        reporter.update_status("Pipeline complete!")
        total_profiles = sum(r.profiles_processed for r in reports)
        total_added = sum(len(r.added) for r in reports)
        total_changed = sum(len(r.changed) for r in reports)
        print(f"\n  Total: {total_profiles} profiles ({total_added} added, {total_changed} changed)")
        print()
        for r in reports:
            print(
                f"  {r.slicer.value}: {r.profiles_processed} profiles "
                f"({len(r.added)} added, {len(r.changed)} changed)"
            )
        if errors:
            print(f"\n  Errors ({len(errors)}):")
            for slicer_name, err in errors:
                print(f"    {slicer_name}: {err}")

    if errors and not reports:
        return 1  # All failed
    if errors:
        return 2  # Partial success
    return 0


def run_diff(args: argparse.Namespace) -> int:
    """Execute the diff command."""
    store_path = project_root / (args.store or _default_store())
    store = ProfileStore(store_path)
    slicer = SlicerType(args.slicer)

    if args.profile:
        if not args.vendor:
            logger.error("--vendor is required when using --profile")
            return 1

        profile = store.get(slicer, args.type, args.vendor, args.profile)
        if not profile:
            logger.error("Profile not found: %s/%s", args.vendor, args.profile)
            return 1

        changes = profile.changed_settings(args.from_version, args.to_version)
        if getattr(args, "json", False):
            print(json.dumps(changes, indent=2, default=str))
        else:
            if not changes:
                print("No changes.")
            else:
                print(f"Changes in {args.vendor}/{args.profile} ({args.from_version} -> {args.to_version}):")
                for key, (old, new) in changes.items():
                    print(f"  {key}: {old} -> {new}")
    else:
        # Diff all profiles
        profiles = store.list_profiles(slicer, args.type)
        all_changes = {}
        for profile in profiles:
            changes = profile.changed_settings(args.from_version, args.to_version)
            if changes:
                all_changes[f"{profile.vendor}/{profile.name}"] = changes

        if getattr(args, "json", False):
            print(json.dumps(all_changes, indent=2, default=str))
        else:
            if not all_changes:
                print("No changes between versions.")
            else:
                print(f"Changes ({args.from_version} -> {args.to_version}):")
                for profile_key, changes in all_changes.items():
                    print(f"\n  {profile_key}:")
                    for key, (old, new) in changes.items():
                        print(f"    {key}: {old} -> {new}")

    return 0


def run_versions(args: argparse.Namespace) -> int:
    """Execute the versions command."""
    store_path = project_root / (args.store or _default_store())
    store = ProfileStore(store_path)
    slicer = SlicerType(args.slicer)

    versions = store.get_versions(slicer)

    if getattr(args, "json", False):
        print(json.dumps({"slicer": slicer.value, "versions": versions}, indent=2))
    elif not versions:
        print(f"No versions ingested for {slicer.value}")
    else:
        print(f"Ingested versions for {slicer.value}:")
        for v in versions:
            print(f"  {v}")

    return 0


def run_list(args: argparse.Namespace) -> int:
    """Execute the list command."""
    store_path = project_root / (args.store or _default_store())
    store = ProfileStore(store_path)
    slicer = SlicerType(args.slicer)

    profiles = store.list_profiles(slicer, args.type)

    if getattr(args, "json", False):
        output = []
        for p in profiles:
            output.append({
                "name": p.name,
                "vendor": p.vendor,
                "profile_type": p.profile_type,
                "first_seen": p.first_seen,
                "last_seen": p.last_seen,
                "filament_id": p.filament_id,
                "settings_count": len(p.settings),
            })
        print(json.dumps(output, indent=2))
    else:
        if not profiles:
            print(f"No profiles found for {slicer.value}")
        else:
            print(f"Profiles for {slicer.value} ({len(profiles)} total):")
            for p in sorted(profiles, key=lambda x: (x.vendor, x.name)):
                print(f"  {p.vendor}/{p.name} ({p.profile_type}, {len(p.settings)} settings)")

    return 0


def run_evaluate(args: argparse.Namespace) -> int:
    """Evaluate a profile at a specific version — snapshot all settings."""
    store_path = project_root / (args.store or _default_store())
    store = ProfileStore(store_path)
    slicer = SlicerType(args.slicer)

    profile = store.get(slicer, args.type, args.vendor, args.profile)
    if not profile:
        logger.error("Profile not found: %s/%s (%s)", args.vendor, args.profile, args.type)
        return 1

    snapshot = profile.evaluate(args.version)

    if getattr(args, "json", False):
        print(json.dumps(snapshot, indent=2, default=str))
    else:
        print(f"{args.vendor}/{profile.name} @ {args.version}")
        print(f"  Type:       {profile.profile_type}")
        print(f"  First seen: {profile.first_seen}")
        print(f"  Last seen:  {profile.last_seen}")
        print(f"  Settings:   {len(snapshot)}")
        print()
        for key, value in sorted(snapshot.items()):
            val_str = str(value)
            if len(val_str) > 100:
                val_str = val_str[:97] + "..."
            print(f"  {key}: {val_str}")

    return 0


def run_map(args: argparse.Namespace) -> int:
    """Execute the map command — full mapping pipeline."""
    from slicer_profiles_db.mapping import run_mapping_pipeline

    use_json = getattr(args, "json", False)
    reporter = _make_reporter(use_json)

    store_path = project_root / (args.store or _default_store())
    output_dir = project_root / args.output
    store = ProfileStore(store_path)

    skip_set = set(args.skip) if args.skip else set()
    slicers = [s for s in SlicerType if s.value not in skip_set] if skip_set else None

    reporter.update_status("Running mapping pipeline...")

    ofd_path_str = getattr(args, "ofd_path", None) or os.environ.get("OFD_REPO_PATH")
    ofd_path = Path(ofd_path_str) if ofd_path_str else None

    try:
        model_map = run_mapping_pipeline(store, output_dir, slicers, ofd_path=ofd_path)
    except Exception as e:
        logger.error("%s", e)
        return 1

    if use_json:
        print(json.dumps({
            "models_mapped": len(model_map.model_to_profiles),
            "failed_brands": sorted(model_map.failed_brands),
            "failed_models": sorted(model_map.failed_models),
            "output_dir": str(output_dir),
        }, indent=2))
    else:
        reporter.update_status("Mapping complete!")
        print(f"\nMapping complete:")
        print(f"  Models mapped:  {len(model_map.model_to_profiles)}")
        print(f"  Failed brands:  {len(model_map.failed_brands)}")
        print(f"  Failed models:  {len(model_map.failed_models)}")
        print(f"  Output:         {output_dir}")

        if model_map.failed_brands:
            print(f"\n  Unmatched brands:")
            for b in sorted(model_map.failed_brands):
                print(f"    - {b}")

        if model_map.failed_models:
            print(f"\n  Unmatched models:")
            for m in sorted(model_map.failed_models)[:30]:
                print(f"    - {m}")
            if len(model_map.failed_models) > 30:
                print(f"    ... and {len(model_map.failed_models) - 30} more")

    return 0


def run_ofd_map(args: argparse.Namespace) -> int:
    """Execute the ofd-map command — forward mapping from OFD to slicer profiles."""
    from slicer_profiles_db.ofd import SlicerMapper

    use_json = getattr(args, "json", False)

    store_path = project_root / (args.store or _default_store())
    ofd_path = Path(args.ofd_path)

    if not ofd_path.exists():
        logger.error("OFD data directory '%s' does not exist", ofd_path)
        return 1
    if not store_path.exists():
        logger.error("Store directory '%s' does not exist", store_path)
        return 1

    from slicer_profiles_db import ProfileIndex

    store = ProfileStore(store_path)
    index = ProfileIndex(store)

    slicer_types = [SlicerType(s) for s in args.slicer] if args.slicer else None
    index.build(slicer_types)

    mapper = SlicerMapper(index=index, data_dir=ofd_path)
    report = mapper.run(
        slicers=args.slicer,
        dry_run=args.dry_run,
        brand_filter=args.brand,
    )

    if use_json:
        output = {
            "updated": [
                {
                    "filament": str(r.filament_path),
                    "slicer": r.slicer,
                    "profile_name": r.profile_name,
                    "slicer_id": r.slicer_id,
                    "generic_id": r.generic_id,
                    "vendor": r.vendor,
                }
                for r in report.updated
            ],
            "already_correct": [
                {
                    "filament": str(r.filament_path),
                    "slicer": r.slicer,
                    "profile_name": r.profile_name,
                }
                for r in report.already_correct
            ],
            "conflicts": [
                {
                    "filament": str(c.filament_path),
                    "slicer": c.slicer,
                    "field": c.field,
                    "existing": c.existing,
                    "derived": c.derived,
                }
                for c in report.conflicts
            ],
            "skipped": [
                {"path": str(p), "reason": reason}
                for p, reason in report.skipped
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        if report.conflicts:
            print("CONFLICTS FOUND — no changes written:\n", file=sys.stderr)
            for c in report.conflicts:
                print(
                    f"  {c.filament_path} [{c.slicer}] {c.field}: "
                    f"existing={c.existing!r} vs derived={c.derived!r}",
                    file=sys.stderr,
                )
            print(
                f"\n{len(report.conflicts)} conflict(s). Fix these in filament.json "
                f"before re-running.",
                file=sys.stderr,
            )
            return 1

        action = "Would update" if args.dry_run else "Updated"

        if report.updated:
            print(f"\n{action} {len(report.updated)} mapping(s):")
            for r in report.updated:
                id_str = f" (id={r.slicer_id})" if r.slicer_id else ""
                gid_str = f" (generic_id={r.generic_id})" if r.generic_id else ""
                print(f"  {r.filament_path} [{r.slicer}] -> {r.profile_name}{id_str}{gid_str}")

        if report.already_correct:
            print(f"\nAlready correct: {len(report.already_correct)}")

        if report.skipped:
            print(f"\nSkipped: {len(report.skipped)}")
            for path, reason in report.skipped[:10]:
                print(f"  {path}: {reason}")
            if len(report.skipped) > 10:
                print(f"  ... and {len(report.skipped) - 10} more")

        total = len(report.updated) + len(report.already_correct)
        print(f"\nTotal matched: {total}")

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = create_parser()
    args = parser.parse_args(argv)

    # Configure logging
    if getattr(args, "verbose", False):
        log_level = logging.DEBUG
    elif getattr(args, "quiet", False):
        log_level = logging.ERROR
    else:
        log_level = logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    if hasattr(args, "func"):
        try:
            return args.func(args)
        except KeyboardInterrupt:
            logger.error("Interrupted")
            return 1
        except Exception as e:
            logger.error("%s", e)
            if getattr(args, "verbose", False):
                logger.debug("Traceback:", exc_info=True)
            return 1
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
