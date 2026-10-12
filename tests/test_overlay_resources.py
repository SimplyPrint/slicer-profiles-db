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
