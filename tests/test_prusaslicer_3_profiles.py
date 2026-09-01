import json

from slicer_profiles_db.models import ProfileType, SlicerType, _version_key
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


def test_evaluated_bundle_preserves_context_and_same_named_variants(tmp_path):
    bundle = {
        "schema_version": 1,
        "format": "prusa-evaluated-profiles",
        "slicer_version": "3.0.0-alpha11",
        "machine": [],
        "process": [
            _record(
                "quality",
                "0.20mm QUALITY",
                1,
                [{"machine_id": "mk4", "machine_name": "Original Prusa MK4"}],
            ),
            _record("quality", "0.20mm QUALITY", 2, [{"machine_id": "xl"}]),
        ],
        "tool_process": [
            _record(
                "tool-quality",
                "Quality tool",
                3,
                [{"machine_id": "mk4", "process_id": "quality", "tool": 0}],
            ),
            {
                **_record(
                    "no-tool",
                    "no tool",
                    0,
                    [{"machine_id": "xl", "process_id": "quality", "tool": 0}],
                ),
                "data": None,
            },
        ],
        "filament": [],
    }
    source = tmp_path / "profile-bundle.json"
    source.write_text(json.dumps(bundle), encoding="utf-8")

    parsed = list(PrusaSlicerParser().parse_directory(tmp_path))

    assert [profile.profile_type for profile in parsed] == [
        ProfileType.PRINT,
        ProfileType.PRINT,
        ProfileType.TOOL_PRINT,
    ]
    assert parsed[0].storage_key != parsed[1].storage_key
    assert parsed[0].settings["print_settings_id"] == "0.20mm QUALITY"
    assert parsed[0].settings["compatible_printers"] == ["Original Prusa MK4"]
    assert parsed[0].context["tool_process_profiles"][0]["native_id"] == (
        "tool-quality"
    )

    store = ProfileStore(tmp_path / "store")
    report = store.ingest_profiles(SlicerType.PRUSASLICER, "3.0.0-alpha11", parsed)

    assert report.profiles_processed == 3
    assert len(store.list_profiles(SlicerType.PRUSASLICER, "print")) == 2
    assert len(store.list_profiles(SlicerType.PRUSASLICER, "tool_print")) == 1


def test_prerelease_versions_sort_numerically():
    assert _version_key("3.0.0-alpha2") < _version_key("3.0.0-alpha11")
    assert _version_key("3.0.0-alpha11") < _version_key("3.0.0-beta1")
    assert _version_key("3.0.0-rc1") < _version_key("3.0.0")


def test_machine_name_nozzle_variants_support_native_3_names():
    parser = PrusaSlicerParser()

    assert parser._machine_model_name("Prusa MK4S 0.4 HF") == "Prusa MK4S"
    assert parser._machine_model_name("Prusa XL+ 2T 0.4 HF, 0.6 HF") == "Prusa XL+"
    assert parser._machine_name_variants("Prusa XL+ 2T 0.4 HF, 0.6 HF") == [
        "0.4",
        "0.6",
    ]


def test_explicit_3_version_routes_directly_to_evaluated_bundle(tmp_path, monkeypatch):
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
        "tool_process": [],
        "filament": [],
    }
    index = {
        "latest": "version_3.0.0-alpha11",
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
    report = ProfilePipeline(store).ingest(
        SlicerType.PRUSASLICER, "version_3.0.0-alpha11"
    )

    assert report.version == "3.0.0-alpha11"
    assert report.profiles_processed == 2
    models = store.list_profiles(SlicerType.PRUSASLICER, "machine_model")
    assert len(models) == 1
    assert models[0].name == "Original Prusa MK4"
    assert models[0].context["variants"] == [{"key": "0.4"}]
    assert len(requested_urls) == 2
    assert all("PrusaSlicer-settings" not in url for url in requested_urls)
