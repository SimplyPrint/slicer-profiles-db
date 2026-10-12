import json

from slicer_profiles_db.download import apply_overlays
from slicer_profiles_db.models import SlicerType
from slicer_profiles_db.resources import ResourceStore, collect_resources


def test_overlay_meshes_are_content_addressed(tmp_path):
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    vendor = tmp_path / "overlay" / "cura" / "Dremel"
    (vendor / "meshes").mkdir(parents=True)
    (vendor / "meshes" / "Dremel_3D45_platform.stl").write_bytes(b"solid bed")
    (vendor / "definitions").mkdir()
    (vendor / "definitions" / "dremel_3d45.def.json").write_text("{}")
    store = ResourceStore(tmp_path / "_resources")

    copied = apply_overlays(extracted, tmp_path / "overlay", SlicerType.CURA)
    resource_map = collect_resources(extracted, store, copied)

    assert sorted(path.name for path in copied) == [
        "Dremel_3D45_platform.stl",
        "dremel_3d45.def.json",
    ]
    hash_hex = resource_map["Dremel_3D45_platform.stl"]
    assert store.get_path(hash_hex).read_bytes() == b"solid bed"
    assert "dremel_3d45.def.json" not in resource_map


def test_a_patch_overlay_sets_keys_on_the_upstream_profile(tmp_path):
    machine = tmp_path / "extracted" / "Dremel" / "machine"
    machine.mkdir(parents=True)
    upstream = machine / "Dremel 3D45 0.4 nozzle.json"
    upstream.write_text(
        json.dumps(
            {
                "name": "Dremel 3D45 0.4 nozzle",
                "use_relative_e_distances": "0",
                "before_layer_change_gcode": ";BEFORE_LAYER_CHANGE\nG92 E0\n",
            }
        )
    )
    patches = tmp_path / "overlay" / "orcaslicer" / "Dremel" / "machine"
    patches.mkdir(parents=True)
    (patches / "Dremel 3D45 0.4 nozzle.patch.json").write_text(
        json.dumps({"before_layer_change_gcode": ";BEFORE_LAYER_CHANGE\n"})
    )
    (patches / "Dremel 3D40 0.4 nozzle.patch.json").write_text("{}")

    copied = apply_overlays(
        tmp_path / "extracted", tmp_path / "overlay", SlicerType.ORCASLICER
    )

    assert copied == []
    assert json.loads(upstream.read_text()) == {
        "name": "Dremel 3D45 0.4 nozzle",
        "use_relative_e_distances": "0",
        "before_layer_change_gcode": ";BEFORE_LAYER_CHANGE\n",
    }
    assert sorted(path.name for path in machine.iterdir()) == [upstream.name]
