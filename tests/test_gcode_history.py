from slicer_profiles_db.catalog import EngineTarget, GcodeTarget
from slicer_profiles_db.gcode_history import build_gcode_history
from slicer_profiles_db.models import StoredProfile


def _profile(settings):
    return StoredProfile(
        slicer="bambustudio",
        profile_type="machine",
        name="Printer 0.4 nozzle",
        vendor="Vendor",
        first_seen="2.6.0",
        last_seen="3.0.0",
        settings=settings,
    )


def _target():
    return EngineTarget(
        version="3.0.0",
        gcode_settings=("machine_start_gcode",),
        gcode_targets=(
            GcodeTarget(version="2.8.0", gcode_abi="engine/2.8"),
            GcodeTarget(version="2.7.0", gcode_abi="engine/2.7"),
            GcodeTarget(version="2.6.0", gcode_abi="engine/2.6"),
        ),
    )


def test_unchanged_gcode_has_no_history():
    profile = _profile({"machine_start_gcode": {"2.6.0": "G28"}})

    assert build_gcode_history(profile, profile.evaluate("3.0.0"), _target()) == {}


def test_equal_historical_gcode_is_stored_once_for_multiple_abis():
    profile = _profile({"machine_start_gcode": {"2.6.0": "G28", "3.0.0": "G28 X"}})

    assert build_gcode_history(profile, profile.evaluate("3.0.0"), _target()) == {
        "machine_start_gcode": [
            {
                "abis": ["engine/2.8", "engine/2.7", "engine/2.6"],
                "value": "G28",
            }
        ]
    }


def test_non_gcode_changes_are_never_materialized():
    profile = _profile(
        {
            "machine_start_gcode": {"2.6.0": "G28"},
            "new_setting": {"2.6.0": None, "3.0.0": True},
        }
    )

    assert build_gcode_history(profile, profile.evaluate("3.0.0"), _target()) == {}


def test_profile_specific_gcode_absence_is_derived_not_materialized():
    profile = _profile({"machine_start_gcode": {"3.0.0": "G28"}})
    profile.first_seen = "2.6.0"

    assert build_gcode_history(profile, profile.evaluate("3.0.0"), _target()) == {}
