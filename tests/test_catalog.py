import json
from pathlib import Path

from slicer_profiles_db.catalog import load_engine_targets, release_lock_problems
from slicer_profiles_db.models import SlicerType

ENGINE_LOCK = Path(__file__).resolve().parents[1] / "engines.lock.json"


def _lock(tmp_path, **bambustudio):
    document = json.loads(ENGINE_LOCK.read_text(encoding="utf-8"))
    document["engines"]["bambustudio"].update(bambustudio)
    path = tmp_path / "engines.lock.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _invalid(path, message):
    try:
        load_engine_targets(path)
    except (TypeError, ValueError) as error:
        assert message in str(error), str(error)
    else:
        raise AssertionError(f"expected an invalid lock: {message}")


def test_every_engine_with_release_history_lists_its_pinned_version():
    targets = load_engine_targets(ENGINE_LOCK)

    for slicer, target in targets.items():
        if target.release_history:
            assert target.releases, slicer
        if target.releases:
            assert target.version in target.releases, slicer
            assert target.nightly_release == target.releases[-1]


def test_a_pinned_version_that_is_not_a_listed_release_is_invalid(tmp_path):
    _invalid(_lock(tmp_path, version="02.08.04.57"), "is not one of its releases")


def test_release_history_needs_releases(tmp_path):
    _invalid(_lock(tmp_path, releases=[]), "lists no releases")


def test_releases_are_listed_oldest_first_once(tmp_path):
    newest, older = "02.09.01.70", "02.08.02.61"

    _invalid(_lock(tmp_path, releases=[newest, older]), "not oldest first")
    _invalid(_lock(tmp_path, releases=[newest, newest]), "lists a release twice")
    _invalid(_lock(tmp_path, releases=["nightly", newest]), "nightly")


def _slicers_lock(**engines):
    return {
        "schema_version": 1,
        "slicers": {
            name: {"versions": {version: {"channel": "stable"} for version in versions}}
            for name, versions in engines.items()
        },
    }


def test_releases_must_be_the_cloud_slicers_stable_versions():
    targets = {
        slicer: target
        for slicer, target in load_engine_targets(ENGINE_LOCK).items()
        if slicer == SlicerType.BAMBUSTUDIO
    }
    releases = list(targets[SlicerType.BAMBUSTUDIO].releases)
    matching = _slicers_lock(BambuStudio=[*releases, "nightly"])

    assert release_lock_problems(targets, matching) == []
    assert release_lock_problems(targets, _slicers_lock(BambuStudio=releases[1:]))
    assert release_lock_problems(
        targets, _slicers_lock(BambuStudio=[*releases, "02.10.00.10"])
    )
    missing = release_lock_problems(targets, _slicers_lock(OrcaSlicer=["2.5.0"]))
    assert missing == ["bambustudio: not in slicers.lock.json"]
