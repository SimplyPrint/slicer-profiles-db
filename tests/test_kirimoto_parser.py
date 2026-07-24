import json

from slicer_profiles_db.models import ProfileType, SlicerType
from slicer_profiles_db.parsers.kirimoto import KiriMotoParser


def test_parses_belt_device_and_embedded_processes(tmp_path):
    source = {
        "pre": ["G28"],
        "post": ["M84"],
        "extruders": [{"nozzle": 0.4, "filament": 1.75}],
        "cmd": {"fan_power": "M106 S{fan_speed}"},
        "settings": {
            "bed_belt": True,
            "bed_width": 220,
            "bed_depth": 350,
            "build_height": 170,
        },
        "profiles": [
            {"processName": "CR30 PLA", "sliceHeight": 0.2, "outputTemp": 210},
            {"processName": "CR30 PETG", "sliceHeight": 0.2, "outputTemp": 240},
        ],
    }
    path = tmp_path / "Creality.CR-30.json"
    path.write_text(json.dumps(source))

    profiles = list(
        KiriMotoParser().parse_directory(tmp_path, resource_version="4.7.1")
    )

    assert [profile.profile_type for profile in profiles] == [
        ProfileType.MACHINE_MODEL,
        ProfileType.MACHINE,
        ProfileType.PRINT,
        ProfileType.FILAMENT,
        ProfileType.PRINT,
        ProfileType.FILAMENT,
    ]
    machine_model, machine, pla, pla_filament, petg, petg_filament = profiles
    assert machine_model.slicer == SlicerType.KIRIMOTO
    assert machine_model.settings["bed_belt"] is True
    assert machine.context["printer_model"] == "Creality CR-30"
    assert machine.settings["bedBelt"] is True
    assert machine.settings["gcodeFan"] == ["M106 S{fan_speed}"]
    assert machine.settings["extruders"][0]["extNozzle"] == 0.4
    assert pla.settings["compatible_printers"] == [machine.name]
    assert {pla.name, petg.name} == {"CR30 PLA", "CR30 PETG"}
    assert pla_filament.filament_type == "PLA"
    assert pla_filament.settings["outputTemp"] == 210
    assert pla_filament.settings["type"] == "filament"
    assert petg_filament.filament_type == "PETG"
    assert machine_model.context["selection_defaults"] == {
        "process_profile": {"match": {"name": "CR30 PLA"}},
        "filament_profile": {"match": {"name": "CR30 PLA"}},
    }


def test_profile_type_filter_only_emits_machine_models(tmp_path):
    path = tmp_path / "SainSmart.Infi-20.json"
    path.write_text(
        json.dumps(
            {
                "extruders": [{"nozzle": 0.4}],
                "settings": {"bed_belt": True},
                "profiles": [{"processName": "SSI20 PLA"}],
            }
        )
    )

    profiles = list(
        KiriMotoParser().parse_directory(
            tmp_path, profile_type_filter=[ProfileType.MACHINE_MODEL]
        )
    )

    assert len(profiles) == 1
    assert profiles[0].profile_type == ProfileType.MACHINE_MODEL
