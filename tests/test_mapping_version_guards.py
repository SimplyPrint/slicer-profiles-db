import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from slicer_profiles_db.mapping import (
    _apply_bed_visual_fallback,
    _bambu_runtime_variants,
    _cura_material_compatibility_aliases,
    _evaluate_stable,
    _find_variant_lookup,
    _machine_model_export,
    _model_variants,
    _parse_variant_from_name,
    _profile_matches_printer,
    _propagate_bed_visual_donors_by_profile,
    _public_variant_payload,
    _same_variant,
    _write_import_manifest,
    ModelMap,
    fetch_sp_slicer_versions,
    map_printer_models,
)
from slicer_profiles_db.models import (
    ParsedProfile,
    ProfileType,
    SlicerType,
    StoredProfile,
)
from slicer_profiles_db.parsers.cura import _material_compatibility_aliases


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

        snapshot = _evaluate_stable(profile, {SlicerType.BAMBUSTUDIO: "02.07.01.62"})

        self.assertEqual(snapshot, {"gcode": "safe gcode"})

    def test_evaluate_stable_keeps_the_newest_local_profile_when_supported(
        self,
    ) -> None:
        profile = StoredProfile(
            slicer=SlicerType.BAMBUSTUDIO.value,
            profile_type="machine",
            name="Example printer",
            vendor="Example",
            first_seen="02.06.01.55",
            last_seen="02.07.00.55",
            settings={"gcode": {"02.07.00.55": "safe gcode"}},
        )

        snapshot = _evaluate_stable(profile, {SlicerType.BAMBUSTUDIO: "02.07.01.62"})

        self.assertEqual(snapshot, {"gcode": "safe gcode"})

    def test_external_profile_versions_are_not_compared_to_runtime_versions(
        self,
    ) -> None:
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

    def test_cura_machine_export_preserves_custom_bed_resources(self) -> None:
        profile = StoredProfile(
            slicer=SlicerType.CURA.value,
            profile_type="machine_model",
            name="Cura printer",
            vendor="Example",
            first_seen="5.13.0",
            last_seen="5.13.0",
            context={
                "bed_assets": {
                    "model": {"url": "sha256:model", "format": "obj"},
                    "texture": {"url": "sha256:texture"},
                },
                "bed_model": "sha256:model",
                "bed_texture": "sha256:texture",
            },
            settings={},
        )

        exported = _machine_model_export(
            profile,
            {
                "machine_width": 220,
                "machine_depth": 220,
                "bed_assets": {"texture": {"url": "sha256:texture"}},
                "bed_model": "legacy-model.stl",
                "bed_texture": "legacy-texture.png",
            },
        )

        self.assertEqual(exported["bed_model"], "sha256:model")
        self.assertEqual(exported["bed_texture"], "sha256:texture")
        self.assertEqual(exported["bed_assets"]["model"]["url"], "sha256:model")
        self.assertNotIn(
            "mesh_selection",
            exported["bed_assets"]["model"],
        )
        self.assertEqual(exported["bed_assets"]["texture"]["target"], "model")
        self.assertEqual(exported["bed_assets"]["texture"]["mapping"], "uv")
        self.assertTrue(exported["bed_assets"]["texture"]["flip_y"])

    def test_cura_machine_export_selects_largest_3mf_mesh(self) -> None:
        profile = StoredProfile(
            slicer=SlicerType.CURA.value,
            profile_type="machine_model",
            name="Cura printer",
            vendor="Example",
            first_seen="5.13.0",
            last_seen="5.13.0",
            context={
                "bed_assets": {
                    "model": {"url": "sha256:model", "format": "3mf"},
                }
            },
            settings={},
        )

        exported = _machine_model_export(profile, {})

        self.assertEqual(
            exported["bed_assets"]["model"]["mesh_selection"],
            "largest_face_count",
        )
        self.assertEqual(
            exported["bed_assets"]["model"]["geometry_space"],
            "raw_mesh",
        )
        self.assertEqual(
            exported["bed_assets"]["model"]["transform"]["rotation"],
            {
                "euler": [90, 0, 0],
                "unit": "deg",
                "order": "XYZ",
            },
        )

    def test_hardware_variant_parser_preserves_high_flow_identity(self) -> None:
        self.assertEqual(
            _parse_variant_from_name("Prusa MK4S HF0.4 nozzle"),
            "HF0.4",
        )
        self.assertEqual(
            _parse_variant_from_name("Prusa CORE One HF 0.6 nozzle"),
            "HF0.6",
        )
        self.assertEqual(
            _parse_variant_from_name("Flashforge Guider4 0.8 HF nozzle"),
            "HF0.8",
        )
        self.assertTrue(_same_variant("HF0.4", "0.4HF"))
        self.assertFalse(_same_variant("HF0.4", "0.4"))

    def test_machine_model_discovers_omitted_high_flow_sibling_roles(self) -> None:
        profile = StoredProfile(
            slicer=SlicerType.ORCASLICER.value,
            profile_type=ProfileType.MACHINE_MODEL.value,
            name="Prusa MK4S",
            vendor="Prusa",
            first_seen="2.4.0",
            last_seen="2.4.0",
            settings={},
        )
        variants = _model_variants(
            profile,
            {
                "name": "Prusa MK4S",
                "nozzle_diameter": "0.4;0.6",
            },
            {
                "standard": {"name": "Prusa MK4S 0.4 nozzle"},
                "high-flow": {"name": "Prusa MK4S HF0.4 nozzle"},
                "child-model": {"name": "Prusa MK4S MMU3 HF0.4 nozzle"},
            },
        )

        self.assertEqual(variants, ["0.4", "0.6", "HF0.4"])

    def test_high_flow_variant_payload_has_explicit_portable_attributes(
        self,
    ) -> None:
        payload = _public_variant_payload(
            {
                "name": "Flashforge Guider4 0.4 HF nozzle",
                "data": {"printer_variant": "0.4HF"},
            },
            variant_key="0.4HF",
        )

        self.assertEqual(
            payload["attributes"],
            {
                "nozzle_diameter": 0.4,
                "nozzle_volume_type": "high_flow",
            },
        )

    def test_bambu_embedded_high_flow_choice_becomes_runtime_variant(self) -> None:
        variants = _bambu_runtime_variants(
            "0.4",
            {
                "name": "Bambu Lab X1 Carbon 0.4 nozzle",
                "data": {
                    "name": "Bambu Lab X1 Carbon 0.4 nozzle",
                    "nozzle_diameter": ["0.4"],
                    "default_nozzle_volume_type": ["Standard"],
                    "extruder_variant_list": [
                        "Direct Drive Standard,Direct Drive High Flow",
                    ],
                },
                "context": {"variant_key": "0.4"},
            },
        )

        self.assertEqual([key for key, _ in variants], ["0.4", "HF0.4"])
        standard = variants[0][1]
        high_flow = variants[1][1]
        self.assertEqual(
            standard["attributes"],
            {
                "nozzle_diameter": 0.4,
                "nozzle_volume_type": "standard",
            },
        )
        self.assertEqual(
            high_flow["attributes"],
            {
                "nozzle_diameter": 0.4,
                "nozzle_volume_type": "high_flow",
            },
        )
        self.assertEqual(
            high_flow["data"]["default_nozzle_volume_type"],
            ["High Flow"],
        )
        self.assertEqual(
            high_flow["data"]["name"],
            "Bambu Lab X1 Carbon 0.4 nozzle",
        )
        self.assertEqual(
            high_flow["name"],
            "Bambu Lab X1 Carbon HF0.4 nozzle",
        )

    def test_bambu_does_not_invent_hf_when_one_tool_lacks_it(self) -> None:
        variants = _bambu_runtime_variants(
            "0.4",
            {
                "name": "Dual tool printer 0.4 nozzle",
                "data": {
                    "nozzle_diameter": ["0.4", "0.4"],
                    "extruder_variant_list": [
                        "Direct Drive Standard,Direct Drive High Flow",
                        "Bowden Standard",
                    ],
                },
            },
        )

        self.assertEqual([key for key, _ in variants], ["0.4"])

    def test_orca_embedded_high_flow_choice_maps_profiles_and_runtime(self) -> None:
        profile = StoredProfile(
            slicer=SlicerType.ORCASLICER.value,
            profile_type=ProfileType.MACHINE_MODEL.value,
            name="Bambu Lab X1 Carbon",
            vendor="BBL",
            first_seen="2.3.2",
            last_seen="2.3.2",
            settings={},
        )
        machine_data = {
            "name": "Bambu Lab X1 Carbon",
            "nozzle_diameter": "0.4",
        }
        standard_role = {
            "name": "Bambu Lab X1 Carbon 0.4 nozzle",
            "data": {
                "name": "Bambu Lab X1 Carbon 0.4 nozzle",
                "nozzle_diameter": ["0.4"],
                "default_nozzle_volume_type": ["Standard"],
                "extruder_variant_list": [
                    "Direct Drive Standard,Direct Drive High Flow",
                ],
            },
        }
        lookup = {"Bambu Lab X1 Carbon0.4": standard_role}

        self.assertEqual(
            _model_variants(profile, machine_data, lookup),
            ["0.4", "HF0.4"],
        )
        high_flow = _find_variant_lookup(
            profile,
            machine_data,
            profile.name,
            "HF0.4",
            lookup,
        )

        self.assertIsNotNone(high_flow)
        self.assertEqual(
            high_flow["data"]["default_nozzle_volume_type"],
            ["High Flow"],
        )
        self.assertEqual(
            high_flow["attributes"]["nozzle_volume_type"],
            "high_flow",
        )

    def test_profile_condition_overrides_broad_printer_compatibility(self) -> None:
        shared = {
            "compat": ["Prusa MK4S HF0.4 nozzle"],
            "printer_identities": {"Prusa MK4S HF0.4 nozzle"},
            "printer_name": "Prusa MK4S HF0.4 nozzle",
            "model_name": "Prusa MK4S",
            "variant": "HF0.4",
            "variant_data": {
                "nozzle_diameter": ["0.4"],
                "printer_notes": ["PRINTER_MODEL_MK4S\nHF_NOZZLE"],
            },
            "slicer": SlicerType.ORCASLICER.value,
        }

        self.assertFalse(
            _profile_matches_printer(
                **shared,
                condition=(
                    "nozzle_diameter[0]==0.4 "
                    "and printer_notes!~/.*HF_NOZZLE.*/"
                ),
            )
        )
        self.assertTrue(
            _profile_matches_printer(
                **shared,
                condition=(
                    "nozzle_diameter[0]==0.4 "
                    "and printer_notes=~/.*HF_NOZZLE.*/"
                ),
            )
        )

    def test_bed_visual_fallback_only_fills_missing_hardware_assets(self) -> None:
        target = {"bed_model": "sha256:engine-owned"}
        fallback = {
            "bed_assets": {"model": {"ref": "sha256:donor"}},
            "bed_model": "sha256:donor",
            "bed_texture": "sha256:texture",
        }

        _apply_bed_visual_fallback(target, fallback)

        self.assertEqual(target["bed_model"], "sha256:engine-owned")
        self.assertEqual(target["bed_texture"], "sha256:texture")
        self.assertEqual(
            target["bed_assets"],
            {"model": {"ref": "sha256:donor"}},
        )

    def test_cura_generic_material_diameter_ids_are_compatibility_aliases(
        self,
    ) -> None:
        self.assertEqual(
            _cura_material_compatibility_aliases(["generic_petg"]),
            ["generic_petg", "generic_petg_175"],
        )
        self.assertEqual(
            _cura_material_compatibility_aliases(["generic_petg_175"]),
            ["generic_petg_175", "generic_petg"],
        )

    def test_cura_parser_groups_material_ids_by_upstream_display_name(
        self,
    ) -> None:
        def material(native_id: str) -> ParsedProfile:
            return ParsedProfile(
                slicer=SlicerType.CURA,
                profile_type=ProfileType.FILAMENT,
                name=f"Generic PETG Generic · {native_id}",
                vendor="Generic",
                settings={},
                native_id=native_id,
                filament_type="PETG",
                context={
                    "brand": "Generic",
                    "material_type": "PETG",
                    "color": "Generic",
                    "display_name": "Generic PETG Generic",
                },
            )

        aliases = _material_compatibility_aliases(
            [material("generic_petg"), material("generic_petg_175")]
        )

        self.assertEqual(
            aliases,
            {
                "generic_petg": ("generic_petg", "generic_petg_175"),
                "generic_petg_175": ("generic_petg_175", "generic_petg"),
            },
        )

    def test_kiri_bed_visual_donor_is_shared_across_same_machine_profile(
        self,
    ) -> None:
        model_map = ModelMap(
            model_to_profiles={
                model_id: {
                    SlicerType.KIRIMOTO.value: ["Creality/Creality Ender 3"]
                }
                for model_id in (6, 42, 43)
            }
        )
        donor = {
            "bed_assets": {
                "model": {
                    "ref": "sha256:donor",
                    "geometry_space": "raw_mesh",
                    "transform": {"rotation": {"euler": [90, 0, 0]}},
                }
            }
        }

        propagated = _propagate_bed_visual_donors_by_profile(
            model_map,
            {42: donor},
        )

        self.assertEqual(propagated, {6: donor, 42: donor, 43: donor})
        self.assertIsNot(propagated[6], propagated[42])

    def test_machine_model_compatibility_alias_maps_kiri_to_ender_3_pro(
        self,
    ) -> None:
        profile = StoredProfile(
            slicer=SlicerType.KIRIMOTO.value,
            profile_type=ProfileType.MACHINE_MODEL.value,
            name="Creality Ender 3",
            vendor="Creality",
            first_seen="4.7.1",
            last_seen="4.7.1",
            context={
                "display_name": "Creality Ender 3",
                "compatible_printer_models": ["Creality Ender-3 Pro"],
            },
            settings={"name": {"4.7.1": "Creality Ender 3"}},
        )
        index = Mock()
        index.find_by_type.side_effect = (
            lambda _slicer, profile_type, *_args: (
                [profile] if profile_type == ProfileType.MACHINE_MODEL else []
            )
        )

        result = map_printer_models(
            Mock(),
            index,
            {
                "brands": ["Creality"],
                "models": [
                    {"id": 43, "brand": "Creality", "name": "Ender-3"},
                    {"id": 42, "brand": "Creality", "name": "Ender-3 Pro"},
                ],
            },
            slicers=[SlicerType.KIRIMOTO],
        )

        self.assertIn(SlicerType.KIRIMOTO.value, result.model_to_profiles[43])
        self.assertIn(SlicerType.KIRIMOTO.value, result.model_to_profiles[42])

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
