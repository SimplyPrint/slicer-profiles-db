from slicer_profiles_db.catalog import EngineTarget, ProfileTarget
from slicer_profiles_db.models import StoredProfile
from slicer_profiles_db.profile_overrides import build_profile_overrides


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
        profile_override_settings=("machine_start_gcode",),
        profile_targets=(
            ProfileTarget(version="2.8.0", profile_abi="engine/2.8"),
            ProfileTarget(version="2.7.0", profile_abi="engine/2.7"),
            ProfileTarget(version="2.6.0", profile_abi="engine/2.6"),
        ),
    )


def test_unchanged_gcode_has_no_history():
    profile = _profile({"machine_start_gcode": {"2.6.0": "G28"}})

    assert build_profile_overrides(profile, profile.evaluate("3.0.0"), _target()) == []


def test_equal_historical_gcode_is_stored_once_for_multiple_abis():
    profile = _profile({"machine_start_gcode": {"2.6.0": "G28", "3.0.0": "G28 X"}})

    assert build_profile_overrides(profile, profile.evaluate("3.0.0"), _target()) == [
        {
            "targets": ["engine/2.8", "engine/2.7", "engine/2.6"],
            "settings": {"machine_start_gcode": "G28"},
        }
    ]


def test_settings_with_the_same_targets_share_one_override_block():
    profile = _profile(
        {
            "machine_start_gcode": {"2.6.0": "start-old", "3.0.0": "start-new"},
            "machine_end_gcode": {"2.6.0": "end-old", "3.0.0": "end-new"},
        }
    )
    target = EngineTarget(
        version="3.0.0",
        profile_override_settings=("machine_start_gcode", "machine_end_gcode"),
        profile_targets=_target().profile_targets,
    )

    assert build_profile_overrides(profile, profile.evaluate("3.0.0"), target) == [
        {
            "targets": ["engine/2.8", "engine/2.7", "engine/2.6"],
            "settings": {
                "machine_start_gcode": "start-old",
                "machine_end_gcode": "end-old",
            },
        }
    ]


def test_non_gcode_changes_are_never_materialized():
    profile = _profile(
        {
            "machine_start_gcode": {"2.6.0": "G28"},
            "new_setting": {"2.6.0": None, "3.0.0": True},
        }
    )

    assert build_profile_overrides(profile, profile.evaluate("3.0.0"), _target()) == []


def test_profile_specific_gcode_absence_is_derived_not_materialized():
    profile = _profile({"machine_start_gcode": {"3.0.0": "G28"}})
    profile.first_seen = "2.6.0"

    assert build_profile_overrides(profile, profile.evaluate("3.0.0"), _target()) == []
