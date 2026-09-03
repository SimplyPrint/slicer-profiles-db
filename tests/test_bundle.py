import hashlib
import json
import zipfile

from slicer_profiles_db.bundle import collect_records, write_bundle
from slicer_profiles_db.catalog import EngineTarget, LaneTarget
from slicer_profiles_db.models import SlicerType


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _coverage():
    return {
        "base": {
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
        SlicerType.ORCASLICER: EngineTarget(version="2.4.2"),
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
        record = json.loads(lines[0])
    assert manifest["records"] == 1
    assert manifest["schema_version"] == 2
    assert manifest["engines"]["orcaslicer"]["version"] == "2.4.2"
    content_hash = record.pop("content_hash")
    assert (
        content_hash
        == hashlib.sha256(
            json.dumps(record, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
    )
    assert (
        manifest["profiles_sha256"]
        == hashlib.sha256(b"\n".join(lines) + b"\n").hexdigest()
    )


def test_catalog_lane_is_a_complete_source_format_snapshot(tmp_path):
    staging = tmp_path / "staging"
    _write(
        staging / "models/1/prusaslicer/print_profiles.json",
        [
            {
                "name": "Quality",
                "data": {"speed": 60},
                "context": {"source_id": "quality"},
            }
        ],
    )

    records = collect_records(staging, "prusaslicer-3")

    assert list(records) == ["prusaslicer:print:quality@prusaslicer-3"]
    assert (
        records["prusaslicer:print:quality@prusaslicer-3"]["catalog_lane"]
        == "prusaslicer-3"
    )

    target = EngineTarget(
        version="2.9.6",
        lanes={
            "prusaslicer-3": LaneTarget(
                version="3.0.0-alpha11",
                format="prusa-evaluated-profiles",
                gcode_abi="slic3r-profile-gcode/v1",
            )
        },
    )
    manifest = write_bundle(
        tmp_path / "lane.spdb",
        records,
        {SlicerType.PRUSASLICER: target},
        {},
        tmp_path,
        _coverage(),
    )
    assert manifest["engines"]["prusaslicer"]["records"] == 0
    assert manifest["engines"]["prusaslicer"]["lanes"]["prusaslicer-3"]["records"] == 1


def test_profiles_with_one_source_id_and_different_payloads_remain_distinct(tmp_path):
    first = tmp_path / "models" / "1" / "superslicer"
    second = tmp_path / "models" / "2" / "superslicer"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "print_profiles.json").write_text(
        json.dumps(
            [{"name": "quality", "context": {"source_id": "quality"}, "speed": 40}]
        )
    )
    (second / "print_profiles.json").write_text(
        json.dumps(
            [{"name": "quality", "context": {"source_id": "quality"}, "speed": 50}]
        )
    )

    records = collect_records(tmp_path)

    assert set(records) == {
        "superslicer:print:quality:models:1",
        "superslicer:print:quality:models:2",
    }


def test_bundle_rejects_incomplete_model_coverage(tmp_path):
    coverage = _coverage()
    coverage["base"]["models"][0]["outcomes"] = {}

    try:
        write_bundle(
            tmp_path / "invalid.spdb",
            {},
            {SlicerType.ORCASLICER: EngineTarget(version="2.4.2")},
            {},
            tmp_path,
            coverage,
        )
    except ValueError as error:
        assert "Incomplete base model coverage" in str(error)
    else:
        raise AssertionError("incomplete coverage must fail the build")
