#!/usr/bin/env python3
"""Check engines.lock.json, and its releases against the cloud slicer's lock.

Without an argument, only engines.lock.json is checked: every engine that keeps
release history lists its releases, and its pinned version is one of them.
With SimplyPrint/site-cloud-slicer's slicers.lock.json, the listed releases must
also be exactly the versions the cloud slicer can slice with, nightlies aside:

    gh api repos/SimplyPrint/site-cloud-slicer/contents/slicers.lock.json \
        --jq .content | base64 -d > slicers.lock.json
    uv run python tools/check_release_lock.py slicers.lock.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from slicer_profiles_db.catalog import load_engine_targets, release_lock_problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "slicers_lock", type=Path, nargs="?", help="Path to slicers.lock.json"
    )
    parser.add_argument("--engine-lock", type=Path, default=Path("engines.lock.json"))
    args = parser.parse_args()

    try:
        targets = load_engine_targets(args.engine_lock)
    except (TypeError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    if args.slicers_lock is None:
        print("engines.lock.json is valid")
        return 0

    slicers_lock = json.loads(args.slicers_lock.read_text(encoding="utf-8"))
    problems = release_lock_problems(targets, slicers_lock)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    print("engines.lock.json lists the cloud slicer's releases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
