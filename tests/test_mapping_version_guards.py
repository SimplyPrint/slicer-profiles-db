import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from slicer_profiles_db.mapping import (
    _evaluate_stable,
    _write_import_manifest,
    fetch_sp_slicer_versions,
)
from slicer_profiles_db.models import SlicerType, StoredProfile


class MappingVersionGuardTests(unittest.TestCase):
    def test_evaluate_stable_caps_profiles_at_simplyprint_latest(self) -> None:
        profile = StoredProfile(
            slicer=SlicerType.BAMBUSTUDIO.value,
            profile_type="machine",
            name="Example printer",
            vendor="Example",
            first_seen="02.07.01.62",
            last_seen="02.08.00.00",
            settings={
                "gcode": {
                    "02.07.01.62": "safe gcode",
                    "02.08.00.00": "incompatible gcode",
                }
            },
        )

        snapshot = _evaluate_stable(
            profile, {SlicerType.BAMBUSTUDIO: "02.07.01.62"}
        )

        self.assertEqual(snapshot, {"gcode": "safe gcode"})

    def test_evaluate_stable_keeps_the_newest_local_profile_when_supported(self) -> None:
        profile = StoredProfile(
            slicer=SlicerType.BAMBUSTUDIO.value,
            profile_type="machine",
            name="Example printer",
            vendor="Example",
            first_seen="02.06.01.55",
            last_seen="02.07.00.55",
            settings={"gcode": {"02.07.00.55": "safe gcode"}},
        )

        snapshot = _evaluate_stable(
            profile, {SlicerType.BAMBUSTUDIO: "02.07.01.62"}
        )

        self.assertEqual(snapshot, {"gcode": "safe gcode"})

    def test_external_profile_versions_are_not_compared_to_runtime_versions(self) -> None:
        for slicer in (
            SlicerType.PRUSASLICER,
            SlicerType.SUPERSLICER,
            SlicerType.CURA,
        ):
            with self.subTest(slicer=slicer):
                profile = StoredProfile(
                    slicer=slicer.value,
                    profile_type="machine",
                    name="External profile",
                    vendor="Example",
                    first_seen="3.0.0",
                    last_seen="3.0.0",
                    settings={"gcode": {"3.0.0": "external gcode"}},
                )

                snapshot = _evaluate_stable(profile, {slicer: "2.9.6"})

                self.assertEqual(snapshot, {"gcode": "external gcode"})

    def test_import_manifest_rejects_missing_required_slicer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            artifact = output_dir / "models/1/bambustudio/machine_profiles.json"
            artifact.parent.mkdir(parents=True)
            artifact.write_text("[]", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "prusaslicer"):
                _write_import_manifest(
                    output_dir,
                    [SlicerType.BAMBUSTUDIO, SlicerType.PRUSASLICER],
                )

    @patch.dict(
        os.environ,
        {},
        clear=True,
    )
    @patch("slicer_profiles_db.mapping.requests.get")
    def test_fetch_slicer_versions_uses_the_default_simplyprint_endpoint(
        self, get: Mock
    ) -> None:
        response = Mock()
        response.json.return_value = {
            "slicers": [
                {"name": "BambuStudio", "latest": "02.07.01.62"},
                {"name": "PrusaSlicer", "latest": "2.9.6"},
                {"name": "UnsupportedSlicer", "latest": "1.0.0"},
            ]
        }
        get.return_value = response

        versions = fetch_sp_slicer_versions()

        self.assertEqual(versions, {SlicerType.BAMBUSTUDIO: "02.07.01.62"})
        get.assert_called_once_with(
            "https://slicing-test.simplyprint.io/api/v1/slicers/versions", timeout=30
        )
        response.raise_for_status.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
