import json
from copy import deepcopy

from slicer_profiles_db.catalog import EngineTarget
from slicer_profiles_db.models import ParsedProfile, ProfileType, SlicerType
from slicer_profiles_db.release_history import (
    ReleaseHistoryError,
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


BAMBU = SlicerType.BAMBUSTUDIO
INGESTED = ("02.07.01.62", "02.08.00.50", "02.08.02.61", "02.09.01.70")
TARGET = EngineTarget(version="02.09.01.70", release_history=True)
MACHINE = "Bambu Lab X1 Carbon 0.4 nozzle"
FILAMENT = "Bambu PLA Basic @BBL X1C"
TIME_LAPSE = ";TIMELAPSE\nM971 S11 C10 O0\n"
# The machine as each release ships it: its start G-code changes in every one.
MACHINES = {
    "02.07.01.62": {
        "name": MACHINE,
        "machine_start_gcode": "M104 S[nozzle_temperature_initial_layer]\nG28\n",
        "time_lapse_gcode": TIME_LAPSE,
    },
    "02.08.00.50": {
        "name": MACHINE,
        "machine_start_gcode": "M104 S{nozzle_temperature_initial_layer[0]}\nG28\n",
        "time_lapse_gcode": TIME_LAPSE,
    },
    "02.08.02.61": {
        "name": MACHINE,
        "machine_start_gcode": "M190 S[bed_temperature_initial_layer_single]\n",
        "time_lapse_gcode": TIME_LAPSE,
        "machine_pause_gcode": "M400 U1\n",
    },
    "02.09.01.70": {
        "name": MACHINE,
        "machine_start_gcode": "M1002 set_filament_type:{filament_type[0]}\n",
        "machine_pause_gcode": "M400 U1\n",
    },
}
# A filament the catalogue gained in 02.08.02.61.
FILAMENTS = {
    "02.08.02.61": {"name": FILAMENT, "filament_start_gcode": "; filament start\n"},
    "02.09.01.70": {"name": FILAMENT, "filament_start_gcode": "; filament start\n"},
}


def _profile(profile_type, name, settings):
    return ParsedProfile(
        slicer=BAMBU,
        profile_type=profile_type,
        name=name,
        vendor="BBL",
        settings=deepcopy(settings),
    )


def _parsed(release):
    profiles = [_profile(ProfileType.MACHINE, MACHINE, MACHINES[release])]
    if release in FILAMENTS:
        profiles.append(_profile(ProfileType.FILAMENT, FILAMENT, FILAMENTS[release]))
    return profiles


def _store(path, releases=INGESTED):
    store = ProfileStore(path)
    for release in releases:
        store.ingest_profiles(BAMBU, release, _parsed(release), complete_release=True)
    return store


def _records():
    machine = {
        "data": deepcopy(MACHINES["02.09.01.70"]),
        "context": {"source_id": f"BBL/machine/{MACHINE}"},
    }
    filament = {
        "data": deepcopy(FILAMENTS["02.09.01.70"]),
        "context": {"source_id": f"BBL/filament/{FILAMENT}"},
    }
    return {
        "machine": {
            "engine": "bambustudio",
            "kind": "machine",
            "id": "machine",
            "profile": {"variants": {"0.4": machine}},
        },
        "filament": {
            "engine": "bambustudio",
            "kind": "filament",
            "id": "filament",
            "profile": filament,
        },
    }


def _machine_index(records):
    return records["machine"]["profile"]["variants"]["0.4"]["version_settings"]


def _read(baseline, index, release):
    """Web's CatalogVersions::at() for a release the index describes."""
    changes = index["versions"][release]
    if changes is None:
        return None
    settings = dict(baseline)
    for key, value in changes.items():
        if value is None:
            settings.pop(key, None)
        else:
            settings[key] = value
    return settings


def _fails(records, store, target, message):
    try:
        apply_release_history(records, store, {BAMBU: target})
    except ReleaseHistoryError as error:
        assert message in str(error), str(error)
    else:
        raise AssertionError(f"expected a failure: {message}")


def test_each_ingested_release_gets_exactly_its_own_gcode(tmp_path):
    records = _records()

    apply_release_history(records, _store(tmp_path), {BAMBU: TARGET})

    index = _machine_index(records)
    exported = MACHINES["02.09.01.70"]
    starts = {MACHINES[release]["machine_start_gcode"] for release in INGESTED}
    assert len(starts) == len(INGESTED)
    assert index["base"] == "02.09.01.70"
    for release in INGESTED:
        assert _read(exported, index, release) == MACHINES[release]
    # 02.07.01.62 has no pause G-code yet; 02.09.01.70 dropped the time-lapse one.
    assert index["versions"]["02.07.01.62"]["machine_pause_gcode"] is None
    assert "time_lapse_gcode" not in exported
    # The filament did not exist before 02.08.02.61.
    filament = records["filament"]["profile"]
    index = filament["version_settings"]
    assert index["versions"]["02.08.00.50"] is None
    assert _read(filament["data"], index, "02.08.02.61") == FILAMENTS["02.08.02.61"]


def test_every_ingested_release_is_described_oldest_first(tmp_path):
    records = _records()

    apply_release_history(records, _store(tmp_path), {BAMBU: TARGET})

    filament = records["filament"]["profile"]["version_settings"]
    assert list(_machine_index(records)["versions"]) == list(INGESTED)
    assert list(filament["versions"]) == list(INGESTED)


def test_a_release_ingested_in_part_fails(tmp_path):
    store = _store(tmp_path, INGESTED[:2])
    store.ingest_profiles(BAMBU, "02.08.02.61", _parsed("02.08.02.61")[:1])
    store.ingest_profiles(
        BAMBU, "02.09.01.70", _parsed("02.09.01.70"), complete_release=True
    )

    _fails(_records(), store, TARGET, "has 02.08.02.61 but no complete ingest")


def test_a_release_without_a_manifest_fails(tmp_path):
    store = _store(tmp_path)
    (tmp_path / "bambustudio" / "_releases" / "02.08.00.50.json").unlink()

    _fails(_records(), store, TARGET, "ingest-releases bambustudio")


def test_a_pinned_version_that_was_not_ingested_fails(tmp_path):
    target = EngineTarget(version="02.10.00.10", release_history=True)

    _fails(_records(), _store(tmp_path), target, "not an ingested release")


def test_a_store_that_no_longer_reproduces_a_release_fails(tmp_path):
    store = _store(tmp_path)
    stored = store.get(BAMBU, "machine", "BBL", MACHINE)
    stored.settings["machine_start_gcode"]["02.08.02.61"] += "; edited\n"
    store._save(stored)

    _fails(_records(), store, TARGET, "does not reproduce 02.08.02.61")


def test_an_export_no_ingested_release_gives_fails(tmp_path):
    records = _records()
    records["filament"]["profile"]["data"]["filament_start_gcode"] = "; changed\n"

    _fails(records, _store(tmp_path), TARGET, "filament is exported")


def test_the_same_releases_give_the_same_settings(tmp_path):
    outputs = []
    for path in (tmp_path / "first", tmp_path / "second"):
        records = _records()
        apply_release_history(records, _store(path), {BAMBU: TARGET})
        outputs.append(json.dumps(records, sort_keys=True))

    assert outputs[0] == outputs[1]


def test_a_renamed_profile_keeps_its_former_releases(tmp_path):
    store = ProfileStore(tmp_path)
    old = {"name": "Old X1C", "filament_start_gcode": "; old\n"}
    new = {"name": FILAMENT, "filament_start_gcode": "; new\n"}
    renamed = {**new, "renamed_from": "Old X1C"}
    for release, profile in (
        ("02.08.02.61", _profile(ProfileType.FILAMENT, "Old X1C", old)),
        ("02.09.01.70", _profile(ProfileType.FILAMENT, FILAMENT, renamed)),
    ):
        store.ingest_profiles(BAMBU, release, [profile], complete_release=True)
    records = {"filament": _records()["filament"]}
    records["filament"]["profile"]["data"] = new

    apply_release_history(records, store, {BAMBU: TARGET})

    index = records["filament"]["profile"]["version_settings"]
    assert index["renamed_from"] == "Old X1C"
    assert _read(new, index, "02.08.02.61") == old


def test_engines_without_release_history_are_left_alone(tmp_path):
    records = _records()
    target = EngineTarget(version="02.09.01.70")

    apply_release_history(records, _store(tmp_path), {BAMBU: target})

    assert "version_settings" not in records["filament"]["profile"]
