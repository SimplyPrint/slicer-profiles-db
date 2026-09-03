import hashlib
import json
import zipfile

from slicer_profiles_db.bundle import collect_records, merge_prerelease, write_bundle
from slicer_profiles_db.catalog import EngineTarget
from slicer_profiles_db.models import SlicerType


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _coverage():
    return {
        "stable": {
            "schema_version": 1,
            "engines": ["orcaslicer"],
            "total_models": 1,
            "classified": 1,
            "classified_percent": 100,
            "mapped": 1,
            "unmapped": 0,
            "models": [
                {
                    "id": 1,
                    "brand": "example",
                    "name": "printer",
                    "outcomes": {
                        "orcaslicer": {
                            "status": "mapped",
                            "source_profiles": ["Example/Printer"],
                        }
                    },
                }
            ],
        }
    }


def test_bundle_deduplicates_profiles_and_is_deterministic(tmp_path):
    staging = tmp_path / "staging"
    profile = {
        "name": "Quality",
        "data": {"speed": 50},
        "context": {"source_id": "quality"},
        "compatible_printers": {"Printer": ["0.4"]},
    }
    for model_id in (1, 2):
        _write(
            staging / f"models/{model_id}/orcaslicer/print_profiles.json",
            [profile],
        )

    records = collect_records(staging)
    assert list(records) == ["orcaslicer:print:quality"]
    assert records["orcaslicer:print:quality"]["model_ids"] == [1, 2]

    targets = {
        SlicerType.ORCASLICER: EngineTarget(stable="2.4.2"),
    }
    first = tmp_path / "first.spdb"
    second = tmp_path / "second.spdb"
    write_bundle(first, records, targets, {}, tmp_path, _coverage())
    write_bundle(second, records, targets, {}, tmp_path, _coverage())

    assert (
        hashlib.sha256(first.read_bytes()).digest()
        == hashlib.sha256(second.read_bytes()).digest()
    )
    with zipfile.ZipFile(first) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        lines = archive.read("profiles.ndjson").splitlines()
    assert manifest["records"] == 1
    assert (
        manifest["profiles_sha256"]
        == hashlib.sha256(b"\n".join(lines) + b"\n").hexdigest()
    )


def test_prerelease_catalog_lane_uses_deltas_until_topology_changes():
    stable = {
        "orcaslicer:print:quality": {
            "engine": "orcaslicer",
            "id": "orcaslicer:print:quality",
            "kind": "print",
            "model_ids": [1],
            "profile": {"name": "Quality", "data": {"speed": 50, "old": True}},
        }
    }
    prerelease = {
        "orcaslicer:print:quality": {
            **stable["orcaslicer:print:quality"],
            "profile": {"name": "Quality", "data": {"speed": 60}},
        },
        "orcaslicer:print:belt": {
            "engine": "orcaslicer",
            "id": "orcaslicer:print:belt",
            "kind": "print",
            "model_ids": [2],
            "profile": {"name": "Belt", "data": {"belt": True}},
        },
    }

    merged = merge_prerelease(stable, prerelease, {"orcaslicer": "orca-beta"})

    assert merged["orcaslicer:print:quality"]["prerelease"] == {
        "set": {"speed": 60},
        "unset": ["old"],
    }
    assert "orcaslicer:print:quality@orca-beta" not in merged
    assert merged["orcaslicer:print:belt@orca-beta"]["catalog_lane"] == "orca-beta"
    assert merged["orcaslicer:print:belt@orca-beta"]["id"] == (
        "orcaslicer:print:belt@orca-beta"
    )


def test_prerelease_topology_change_replaces_stable_profile():
    stable = {
        "orcaslicer:print:quality": {
            "engine": "orcaslicer",
            "id": "orcaslicer:print:quality",
            "kind": "print",
            "model_ids": [1],
            "profile": {"name": "Quality", "data": {"speed": 50}},
        }
    }
    prerelease = {
        "orcaslicer:print:quality": {
            **stable["orcaslicer:print:quality"],
            "model_ids": [2],
            "profile": {"name": "Quality", "data": {"speed": 60}},
        }
    }

    merged = merge_prerelease(stable, prerelease, {"orcaslicer": "orca-beta"})

    assert merged["orcaslicer:print:quality"]["prerelease"] == {"hidden": True}
    assert merged["orcaslicer:print:quality@orca-beta"]["model_ids"] == [2]


def test_prerelease_overlay_without_lane_stores_only_setting_delta():
    stable = {
        "orcaslicer:print:quality": {
            "engine": "orcaslicer",
            "id": "orcaslicer:print:quality",
            "kind": "print",
            "model_ids": [1],
            "profile": {"name": "Quality", "data": {"speed": 50, "old": True}},
        }
    }
    prerelease = {
        "orcaslicer:print:quality": {
            **stable["orcaslicer:print:quality"],
            "profile": {"name": "Quality", "data": {"speed": 60}},
        }
    }

    merged = merge_prerelease(stable, prerelease, {})

    assert merged["orcaslicer:print:quality"]["prerelease"] == {
        "set": {"speed": 60},
        "unset": ["old"],
    }


def test_bundle_rejects_incomplete_model_coverage(tmp_path):
    coverage = _coverage()
    coverage["stable"]["models"][0]["outcomes"] = {}

    try:
        write_bundle(
            tmp_path / "invalid.spdb",
            {},
            {SlicerType.ORCASLICER: EngineTarget(stable="2.4.2")},
            {},
            tmp_path,
            coverage,
        )
    except ValueError as error:
        assert "Incomplete stable model coverage" in str(error)
    else:
        raise AssertionError("incomplete coverage must fail the build")
