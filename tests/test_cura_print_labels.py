import unittest
from pathlib import Path

from slicer_profiles_db.models import ParsedProfile, ProfileType, SlicerType
from slicer_profiles_db.parsers.cura import _InstanceResource, _print_label


def material(native_id: str, name: str, brand: str, material_type: str, color: str):
    return ParsedProfile(
        slicer=SlicerType.CURA,
        profile_type=ProfileType.FILAMENT,
        name=name,
        vendor=brand,
        settings={},
        native_id=native_id,
        context={"brand": brand, "material_type": material_type, "color": color},
    )


def instance(source_kind: str, name: str, quality_type: str, **values):
    return _InstanceResource(
        path=Path("x.inst.cfg"),
        source_kind=source_kind,
        native_id=f"{source_kind}:{name}",
        definition="dremel_3d45",
        name=name,
        metadata={"quality_type": quality_type},
        values=values,
    )


class CuraPrintLabelTests(unittest.TestCase):
    def test_reads_like_a_person_wrote_it(self) -> None:
        pla = material("dremel_pla", "Dremel PLA Blue", "Dremel", "PLA", "Blue")
        self.assertEqual(
            _print_label(
                instance("quality", "Low Quality", "draft"),
                None,
                0.3,
                "",
                pla,
                "dremel_pla",
            ),
            "Low Quality (0.3 mm) · Dremel PLA",
        )

    def test_names_a_quality_after_its_global_quality(self) -> None:
        tough = material(
            "ultimaker_tough_pla",
            "Any color Tough PLA",
            "Ultimaker",
            "Tough PLA",
            "Generic",
        )
        self.assertEqual(
            _print_label(
                instance("quality", "pla_noz0.40_lay0.25", "normal"),
                instance("quality", "Standard Quality", "normal"),
                0.25,
                "AA 0.4",
                tough,
                "ultimaker_tough_pla",
            ),
            "Standard Quality (0.25 mm) · AA 0.4 · Ultimaker Tough PLA",
        )

    def test_keeps_a_material_label_and_falls_back_to_the_id(self) -> None:
        silk = material("goofoo_esilk_pla", "eSilk", "GOOFOO", "PLA", "Generic")
        quality = instance("quality", "Fine", "fine")
        self.assertEqual(
            _print_label(quality, None, None, "0.4mm Nozzle", silk, "goofoo_esilk_pla"),
            "Fine · 0.4mm Nozzle · GOOFOO eSilk",
        )
        self.assertEqual(
            _print_label(quality, None, None, "", None, "unknown_pla"),
            "Fine · unknown_pla",
        )

    def test_names_an_intent_with_its_quality_level(self) -> None:
        fast = instance("quality", "Fast", "fast", layer_height="0.2")
        intent = instance("intent", "Annealing", "fast")
        self.assertEqual(
            _print_label(intent, fast, None, "CC 0.4", None, ""),
            "Annealing · Fast (0.2 mm) · CC 0.4",
        )


if __name__ == "__main__":
    unittest.main()
