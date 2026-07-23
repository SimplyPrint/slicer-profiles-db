from slicer_profiles_db.mapping import _prepare_sp_data
from slicer_profiles_db.matching import match_printer_model


def test_child_variant_inherits_parent_model_and_alias_names() -> None:
    brands, models, names = _prepare_sp_data(
        {
            "brands": ["Creality"],
            "models": [
                {
                    "id": 6,
                    "brand": "Creality",
                    "name": "Ender-3",
                    "parent": None,
                    "slicerProfileNames": ["ENDER3"],
                },
                {
                    "id": 42,
                    "brand": "Creality",
                    "name": "Ender-3 Pro",
                    "parent": 6,
                    "slicerProfileNames": None,
                },
            ],
        }
    )

    assert names[42] == ["ender-3", "ENDER3"]
    assert match_printer_model(
        models,
        brands,
        names,
        brand="Creality",
        printer_name="Creality Ender-3",
        brand_map={},
    ) == {6, 42}


def test_parent_names_are_inherited_transitively_without_duplicates() -> None:
    _, _, names = _prepare_sp_data(
        {
            "brands": ["Bambu Lab"],
            "models": [
                {
                    "id": 1,
                    "brand": "Bambu Lab",
                    "name": "P1S",
                    "parent": None,
                    "slicerProfileNames": ["Bambu Lab P1S"],
                },
                {
                    "id": 2,
                    "brand": "Bambu Lab",
                    "name": "P1S Combo",
                    "parent": 1,
                    "slicerProfileNames": ["Bambu Lab P1S"],
                },
                {
                    "id": 3,
                    "brand": "Bambu Lab",
                    "name": "P1S FarmLoop Combo",
                    "parent": 2,
                    "slicerProfileNames": None,
                },
            ],
        }
    )

    assert names[3] == ["p1s combo", "Bambu Lab P1S", "p1s"]


def test_invalid_parent_relationship_does_not_leak_names_across_brands() -> None:
    _, _, names = _prepare_sp_data(
        {
            "brands": ["Creality", "Anycubic"],
            "models": [
                {
                    "id": 1,
                    "brand": "Anycubic",
                    "name": "Kobra",
                    "parent": None,
                    "slicerProfileNames": ["Anycubic Kobra"],
                },
                {
                    "id": 2,
                    "brand": "Creality",
                    "name": "Ender-3",
                    "parent": 1,
                    "slicerProfileNames": None,
                },
            ],
        }
    )

    assert 2 not in names


def test_parent_cycle_is_bounded() -> None:
    _, _, names = _prepare_sp_data(
        {
            "brands": ["Creality"],
            "models": [
                {
                    "id": 1,
                    "brand": "Creality",
                    "name": "Ender-3",
                    "parent": 2,
                    "slicerProfileNames": None,
                },
                {
                    "id": 2,
                    "brand": "Creality",
                    "name": "Ender-3 Pro",
                    "parent": 1,
                    "slicerProfileNames": None,
                },
            ],
        }
    )

    assert names[1] == ["ender-3 pro"]
    assert names[2] == ["ender-3"]
