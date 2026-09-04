from slicer_profiles_db.compatibility import backwards_delta


def test_backwards_delta_keeps_ordinary_latest_values():
    current = {"speed": 80, "mode": "quality"}
    previous = {"speed": 50, "mode": "draft"}
    schema = {
        "speed": {"type": "int"},
        "mode": {"type": "string"},
    }

    assert backwards_delta(current, previous, schema, schema) is None


def test_backwards_delta_handles_schema_gcode_and_enum_boundaries():
    current = {
        "new_setting": True,
        "machine_start_gcode": "{new_placeholder}",
        "mode": "new_mode",
        "typed": [1],
    }
    previous = {
        "legacy_setting": 7,
        "machine_start_gcode": "[old_placeholder]",
        "mode": "old_mode",
        "typed": "1",
    }
    current_schema = {
        "new_setting": {"type": "bool"},
        "machine_start_gcode": {"type": "string"},
        "mode": {"type": "enum", "enum_values": ["old_mode", "new_mode"]},
        "typed": {"type": "ints"},
    }
    target_schema = {
        "legacy_setting": {"type": "int"},
        "machine_start_gcode": {"type": "string"},
        "mode": {"type": "enum", "enum_values": ["old_mode"]},
        "typed": {"type": "string"},
    }

    assert backwards_delta(current, previous, current_schema, target_schema) == {
        "set": {
            "legacy_setting": 7,
            "machine_start_gcode": "[old_placeholder]",
            "mode": "old_mode",
            "typed": "1",
        },
        "unset": ["new_setting"],
    }


def test_backwards_delta_restores_historical_enum_value():
    assert backwards_delta(
        {"mode": "new_mode"},
        {"mode": "old_mode"},
        {"mode": {"type": "enum", "enum_values": ["new_mode"]}},
        {
            "mode": {
                "type": "enum",
                "enum_values": ["old_mode"],
            }
        },
    ) == {"set": {"mode": "old_mode"}}
