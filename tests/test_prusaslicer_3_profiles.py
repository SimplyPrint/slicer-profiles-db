import json

import pytest

from slicer_profiles_db.mapping import _evaluate_stable
from slicer_profiles_db.models import (
    ParsedProfile,
    ProfileType,
    SlicerType,
    _version_key,
)
from slicer_profiles_db.parsers.prusaslicer import PrusaSlicerParser
from slicer_profiles_db.pipeline import ProfilePipeline
from slicer_profiles_db.store import ProfileStore


def _record(native_id, name, value, contexts):
    return {
        "vendor_id": "PrusaResearch",
        "native_id": native_id,
        "root_id": native_id,
        "name": name,
        "technology": "FFF",
        "data": {"example_setting": value},
        "contexts": contexts,
    }


def test_evaluated_bundle_reduces_tool_profiles_to_standard_process_profiles(tmp_path):
    bundle = {
        "schema_version": 1,
        "format": "prusa-evaluated-profiles",
        "slicer_version": "3.0.0-alpha11",
        "machine": [],
        "process": [
            _record(
                "quality",
                "0.20mm",
                1,
                [
                    {
                        "machine_id": "mk4",
                        "machine_name": "Original Prusa MK4",
                        "preset": {"id": "quality"},
                        "tool_processes": [
                            {
                                "tool": 0,
                                "native_id": "speed",
                                "root_id": "speed",
                                "name": "0.20mm SPEED @MK4 0.4",
                                "data": {"example_setting": 2},
                                "preset": {"id": "speed"},
                            },
                            {
                                "tool": 0,
                                "native_id": "structural",
                                "root_id": "structural",
                                "name": "0.20mm STRUCTURAL @MK4 0.4",
                                "data": {"example_setting": 3},
                            },
                        ],
                    }
                ],
            ),
            _record(
                "quality",
                "0.20mm",
                4,
                [{"machine_id": "xl", "machine_name": "Original Prusa XL"}],
            ),
        ],
        "filament": [],
    }
    source = tmp_path / "profile-bundle.json"
    source.write_text(json.dumps(bundle), encoding="utf-8")

    parsed = list(PrusaSlicerParser().parse_directory(tmp_path))

    assert [profile.profile_type for profile in parsed] == [ProfileType.PRINT] * 3
    assert {profile.name for profile in parsed} == {
        "0.20mm SPEED",
        "0.20mm STRUCTURAL",
        "0.20mm",
    }
    speed = next(profile for profile in parsed if profile.name == "0.20mm SPEED")
    assert speed.settings["example_setting"] == 2
    assert speed.settings["compatible_printers"] == ["Original Prusa MK4"]
    assert speed.context["configuration"]["print_settings"]["example_setting"] == 1
    assert speed.context["configuration"]["toolprint_settings"] == {
        "example_setting": [2]
    }
    assert speed.context["configuration"]["preset"] == {
        "print": {"id": "quality"},
        "tools": [{"id": "speed"}],
    }
    assert speed.setting_scopes["example_setting"] == "extruder.0"

    store = ProfileStore(tmp_path / "store")
    report = store.ingest_profiles(SlicerType.PRUSASLICER, "3.0.0-alpha11", parsed)

    assert report.profiles_processed == 3
    assert len(store.list_profiles(SlicerType.PRUSASLICER, "print")) == 3


def test_prerelease_versions_sort_numerically():
    assert _version_key("3.0.0-alpha2") < _version_key("3.0.0-alpha11")
    assert _version_key("3.0.0-alpha11") < _version_key("3.0.0-beta1")
    assert _version_key("3.0.0-rc1") < _version_key("3.0.0")


def test_prerelease_only_profile_is_not_exported_as_stable():
    parsed = ParsedProfile(
        slicer=SlicerType.PRUSASLICER,
        profile_type=ProfileType.PRINT,
        name="Alpha profile",
        vendor="Prusa",
        settings={"new_setting": True},
    )
    stored = ProfileStore("unused")._create_stored(parsed, "3.0.0-alpha11")

    assert _evaluate_stable(stored) == {}


def test_same_version_snapshot_is_immutable(tmp_path):
    store = ProfileStore(tmp_path / "store")
    profile = ParsedProfile(
        slicer=SlicerType.PRUSASLICER,
        profile_type=ProfileType.PRINT,
        name="Profile",
        vendor="Prusa",
        settings={"value": 1},
    )
    store.ingest_profiles(SlicerType.PRUSASLICER, "3.0.0-alpha11", [profile])

    profile.settings["value"] = 2
    with pytest.raises(ValueError, match="immutable"):
        store.ingest_profiles(SlicerType.PRUSASLICER, "3.0.0-alpha11", [profile])


def test_machine_name_nozzle_variants_support_native_3_names():
    parser = PrusaSlicerParser()

    assert parser._machine_model_name("Prusa MK4S 0.4 HF") == "Prusa MK4S"
    assert parser._machine_model_name("Prusa XL+ 2T 0.4 HF, 0.6 HF") == "Prusa XL+"
    assert parser._machine_name_variants("Prusa XL+ 2T 0.4 HF, 0.6 HF") == [
        "0.4",
        "0.6",
    ]


def test_latest_uses_prerelease_profile_channel(tmp_path, monkeypatch):
    bundle = {
        "schema_version": 1,
        "format": "prusa-evaluated-profiles",
        "slicer_version": "3.0.0-alpha11",
        "machine": [
            {
                **_record(
                    "mk4-04",
                    "Original Prusa MK4 0.4 nozzle",
                    1,
                    [{"printer_model": "MK4", "printer_base_model": "MK4"}],
                ),
                "data": {"nozzle_diameter": [0.4]},
            }
        ],
        "process": [],
        "filament": [],
    }
    index = {
        "latest": "version_2.9.6",
        "channels": {"prerelease": "version_3.0.0-alpha11"},
        "versions": {"version_3.0.0-alpha11": {"config": {"sha256": "test"}}},
    }

    class Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload
            self.content = json.dumps(payload).encode("utf-8")

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    requested_urls = []

    def fake_get(url, timeout):
        requested_urls.append(url)
        return Response(bundle if url.endswith("profile-bundle.json") else index)

    monkeypatch.setattr("slicer_profiles_db.pipeline.requests.get", fake_get)
    store = ProfileStore(tmp_path / "store")
    report = ProfilePipeline(store).ingest(SlicerType.PRUSASLICER, "latest")

    assert report.version == "3.0.0-alpha11"
    assert report.profiles_processed == 2
    models = store.list_profiles(SlicerType.PRUSASLICER, "machine_model")
    assert len(models) == 1
    assert models[0].name == "Original Prusa MK4"
    assert models[0].context["variants"] == [{"key": "0.4"}]
    assert len(requested_urls) == 2
    assert all("PrusaSlicer-settings" not in url for url in requested_urls)
