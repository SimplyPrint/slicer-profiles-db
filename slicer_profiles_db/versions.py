"""
Version normalization and GitHub tag enumeration.
"""

import logging
import os
import re
import sys
from pathlib import Path

import requests

from .models import SlicerType, VersionInfo, _version_key

logger = logging.getLogger(__name__)


# Re-export the canonical version_key so callers can do
# ``from .versions import version_key`` without knowing about models.
version_key = _version_key


def check_github_token(required: bool = False) -> bool:
    """Check if GITHUB_TOKEN is set. Warn or raise if not.

    Args:
        required: If True, exit with error instead of warning.

    Returns:
        True if token is available, False otherwise.
    """
    if os.environ.get("GITHUB_TOKEN"):
        return True

    msg = (
        "GITHUB_TOKEN is not set. GitHub API requests will be rate-limited "
        "to 60 requests/hour. Set GITHUB_TOKEN to increase the limit to 5,000/hour.\n"
        "  export GITHUB_TOKEN=ghp_..."
    )
    if required:
        raise SystemExit(f"Error: {msg}")

    logger.warning(msg)
    return False


def normalize_version(raw: str) -> str:
    """
    Strip common prefixes from version strings.

    "v02.05.00.66"  → "02.05.00.66"
    "version_2.9.3" → "2.9.3"
    "2.9.3"         → "2.9.3"
    """
    s = raw.strip()
    if s.lower().startswith("version_"):
        s = s[len("version_"):]
    elif s.lower().startswith("v"):
        s = s[1:]
    return s


def sort_versions(versions: list[str]) -> list[str]:
    """Sort version strings semantically."""
    return sorted(versions, key=version_key)


_PRERELEASE_RE = re.compile(r"(?:alpha|beta|rc|dev|pre)", re.IGNORECASE)


def is_prerelease(version: str) -> bool:
    """Return True if a version string looks like a pre-release."""
    return bool(_PRERELEASE_RE.search(version))


def enumerate_github_tags(
    repo: str,
    tag_pattern: str | None = None,
    slicer: SlicerType | None = None,
) -> list[VersionInfo]:
    """
    List tags from GitHub API, optionally filtered by regex pattern.

    Supports GITHUB_TOKEN env var for rate limiting.

    Args:
        repo: GitHub repo in "owner/repo" format.
        tag_pattern: Optional regex to filter tag names.
        slicer: SlicerType to attach to VersionInfo results.

    Returns:
        List of VersionInfo sorted by tag name.
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"

    tags: list[VersionInfo] = []
    page = 1
    pattern = re.compile(tag_pattern) if tag_pattern else None

    while True:
        resp = requests.get(
            f"https://api.github.com/repos/{repo}/tags",
            headers=headers,
            params={"per_page": 100, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break

        for tag_data in data:
            name = tag_data["name"]
            if pattern and not pattern.search(name):
                continue
            tags.append(
                VersionInfo(
                    raw=name,
                    normalized=normalize_version(name),
                    slicer=slicer or SlicerType.BAMBUSTUDIO,
                )
            )

        page += 1

    return tags


def enumerate_ini_versions(vendor_dir: Path) -> list[str]:
    """
    List version strings from INI bundle filenames.

    Example: vendor_dir contains "2.9.3.ini" → ["2.9.3"]
    """
    version_re = re.compile(r"([\d]+(?:\.[\d]+)+)\.ini$", re.IGNORECASE)
    versions = []
    if not vendor_dir.exists():
        return versions
    for f in vendor_dir.iterdir():
        match = version_re.search(f.name)
        if match:
            versions.append(match.group(1))
    return sort_versions(versions)
