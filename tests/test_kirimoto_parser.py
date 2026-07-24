import json

from slicer_profiles_db.brands import BRAND_MAPS
from slicer_profiles_db.matching import match_printer_model
from slicer_profiles_db.models import ProfileType, SlicerType
from slicer_profiles_db.parsers.kirimoto import KiriMotoParser
from slicer_profiles_db.store import ProfileStore


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
    assert {pla.name, petg.name} == {
        "CR30 PLA @Creality CR-30",
        "CR30 PETG @Creality CR-30",
    }
    assert pla.settings["name"] == "CR30 PLA"
    assert pla.context == {
        "native_id": "Creality.CR-30:process:0",
        "printer_model": "Creality CR-30",
    }
    assert pla_filament.filament_type == "PLA"
    assert pla_filament.settings["outputTemp"] == 210
    assert pla_filament.settings["type"] == "filament"
    assert pla_filament.context == {
        "native_id": "Creality.CR-30:filament:0",
        "printer_model": "Creality CR-30",
    }
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


def test_same_named_embedded_profiles_remain_distinct_per_device(tmp_path):
    for model, temperature in (("A1", 220), ("P1S", 230)):
        path = tmp_path / f"Bambu.{model}.json"
        path.write_text(
            json.dumps(
                {
                    "extruders": [{"nozzle": 0.4}],
                    "profiles": [
                        {
                            "processName": "Bambu PLA",
                            "outputTemp": temperature,
                        }
                    ],
                }
            )
        )

    parsed = list(KiriMotoParser().parse_directory(tmp_path))
    print_profiles = [
        profile for profile in parsed if profile.profile_type == ProfileType.PRINT
    ]
    filament_profiles = [
        profile for profile in parsed if profile.profile_type == ProfileType.FILAMENT
    ]

    assert {profile.name for profile in print_profiles} == {
        "Bambu PLA @Bambu A1",
        "Bambu PLA @Bambu P1S",
    }
    assert {profile.name for profile in filament_profiles} == {
        "Bambu PLA @Bambu A1",
        "Bambu PLA @Bambu P1S",
    }
    assert {profile.settings["outputTemp"] for profile in print_profiles} == {
        220,
        230,
    }

    store = ProfileStore(tmp_path / "store")
    report = store.ingest_profiles(SlicerType.KIRIMOTO, "test", parsed)

    assert report.profiles_processed == len(parsed)
    assert len(store.list_profiles(SlicerType.KIRIMOTO, "print")) == 2
    assert len(store.list_profiles(SlicerType.KIRIMOTO, "filament")) == 2


def test_ender_3_profile_declares_ender_3_pro_compatibility(tmp_path):
    path = tmp_path / "Creality.Ender.3.json"
    path.write_text(
        json.dumps(
            {
                "extruders": [{"nozzle": 0.4}],
                "profiles": [{"processName": "Ender 3 PLA"}],
            }
        )
    )

    machine_model = next(
        profile
        for profile in KiriMotoParser().parse_directory(tmp_path)
        if profile.profile_type == ProfileType.MACHINE_MODEL
    )

    assert machine_model.context["compatible_printer_models"] == [
        "Creality Ender-3 Pro"
    ]


def test_kirimoto_vendor_aliases_match_simplyprint_brands():
    models = [
        {"id": 19, "brand": "Folger Tech", "name": "FT-5"},
        {"id": 431, "brand": "Bambu Lab", "name": "P1S"},
        {"id": 476, "brand": "Bambu Lab", "name": "A1"},
        {"id": 89, "brand": "Ultimaker", "name": "2"},
    ]
    brands = ["Bambu Lab", "Folger Tech", "Ultimaker"]
    aliases: dict[int, list[str]] = {}
    brand_map = BRAND_MAPS[SlicerType.KIRIMOTO]

    assert match_printer_model(
        models,
        brands,
        aliases,
        "Bambu",
        "Bambu A1",
        brand_map,
    ) == {476}
    assert match_printer_model(
        models,
        brands,
        aliases,
        "Bambu",
        "Bambu P1S",
        brand_map,
    ) == {431}
    assert match_printer_model(
        models,
        brands,
        aliases,
        "FolgerTech",
        "FolgerTech FT5",
        brand_map,
    ) == {19}
    assert match_printer_model(
        models,
        brands,
        aliases,
        "Ultimaker",
        "Ultimaker Ultimaker2",
        brand_map,
    ) == {89}
