import json

from slicer_profiles_db.catalog import EngineTarget
from slicer_profiles_db.models import SlicerType
from slicer_profiles_db.release_history import (
    apply_release_history,
    release_index,
    twin,
)
from slicer_profiles_db.store import ProfileStore

# The same fixture as web's CatalogVersionsTest, which reads what this module writes.
RELEASES = [
    "02.05.01.52",
    "02.06.01.55",
    "02.07.00.55",
    "02.07.01.62",
    "02.08.00.50",
    "02.08.02.60",
]
HISTORY = {
    "name": {"02.05.01.52": "Bambu PLA Basic @BBL P1S 0.4 nozzle"},
    "filament_id": {"02.05.01.52": "GFA00"},
    "filament_cost": {"02.05.01.52": ["24.99"], "02.08.00.50": ["19.99"]},
    "filament_flow_ratio": {"02.06.01.55": ["0.98"]},
    "filament_scarf_seam_type": {"02.05.01.52": ["none"], "02.07.01.62": None},
    "filament_ramming_travel_time": {"nightly-2026-08-01": ["0"]},
}
EXPORTED = {
    "name": "Bambu PLA Basic @BBL P1S 0.4 nozzle",
    "filament_id": "GFA00",
    "filament_cost": ["24.99"],
    "filament_flow_ratio": ["0.98"],
}


def test_indexes_the_export_against_the_newest_release_that_gives_it():
    index = release_index(HISTORY, EXPORTED, RELEASES)

    assert index["base"] == "02.07.01.62"
    assert index["versions"]["02.07.01.62"] == {}
    assert index["versions"]["02.08.02.60"] == {"filament_cost": ["19.99"]}
    assert index["versions"]["02.05.01.52"] == {
        "filament_scarf_seam_type": ["none"],
        "filament_flow_ratio": None,
    }


def test_an_export_no_release_gives_has_no_index():
    assert (
        release_index(HISTORY, {**EXPORTED, "filament_cost": ["9.99"]}, RELEASES)
        is None
    )


def test_values_compare_strictly_like_php():
    history = {"retraction_length": {"02.05.01.52": [1]}}

    assert release_index(history, {"retraction_length": [1.0]}, RELEASES) is None


def test_a_high_flow_twin_takes_its_standard_changes_on_the_settings_it_keeps():
    standard = {
        "printer_model": "Bambu Lab P1S",
        "nozzle_volume_type": ["Standard"],
        "max_volumetric_speed": ["20"],
    }
    history = {
        "printer_model": {"02.05.01.52": "Bambu Lab P1S"},
        "nozzle_volume_type": {"02.05.01.52": ["Standard"]},
        "max_volumetric_speed": {"02.05.01.52": ["20"]},
        "best_object_pos": {"02.08.00.50": "0.5,0.5"},
    }
    index = release_index(history, standard, RELEASES)

    copy = twin(index, standard, {**standard, "nozzle_volume_type": ["High Flow"]})

    assert copy["versions"]["02.08.02.60"] == {"best_object_pos": "0.5,0.5"}
    assert copy["versions"]["02.05.01.52"] == {}


def _store(tmp_path):
    root = tmp_path / "profiles"
    engine = root / "bambustudio"
    engine.mkdir(parents=True)
    (engine / "_meta.json").write_text(json.dumps({"versions": RELEASES}))
    stored = {
        "slicer": "bambustudio",
        "profile_type": "filament",
        "name": EXPORTED["name"],
        "vendor": "BBL",
        "first_seen": "02.05.01.52",
        "last_seen": "02.08.02.60",
        "renamed_from": "Bambu PLA Basic (old)",
        "settings": HISTORY,
    }
    path = engine / "BBL" / "filament" / "profile.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(stored))
    return ProfileStore(root)


def _record(data, engine="bambustudio"):
    return {
        "engine": engine,
        "kind": "filament",
        "id": "record",
        "profile": {
            "data": data,
            "context": {"source_id": f"BBL/filament/{EXPORTED['name']}"},
        },
    }


def test_records_carry_their_release_history_and_former_name(tmp_path):
    records = {"exact": _record(EXPORTED), "changed": _record({**EXPORTED, "x": 1})}

    apply_release_history(
        records,
        _store(tmp_path),
        {
            SlicerType.BAMBUSTUDIO: EngineTarget(
                version="02.07.01.62", release_history=True
            )
        },
    )

    assert records["exact"]["profile"]["version_settings"] == {
        **release_index(HISTORY, EXPORTED, RELEASES),
        "renamed_from": "Bambu PLA Basic (old)",
    }
    assert records["changed"]["profile"]["version_settings"] == {
        "renamed_from": "Bambu PLA Basic (old)"
    }


def test_engines_without_release_history_are_left_alone(tmp_path):
    records = {"exact": _record(EXPORTED)}

    apply_release_history(
        records,
        _store(tmp_path),
        {SlicerType.BAMBUSTUDIO: EngineTarget(version="02.07.01.62")},
    )

    assert "version_settings" not in records["exact"]["profile"]
