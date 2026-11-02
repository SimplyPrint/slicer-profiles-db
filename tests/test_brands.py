import pytest

from slicer_profiles_db.brands import normalize_brand, strip_brand_from_name
from slicer_profiles_db.models import SlicerType


@pytest.mark.parametrize(
    ("slicer", "vendor"),
    [
        (SlicerType.ORCASLICER, "Qidi"),
        (SlicerType.BAMBUSTUDIO, "Qidi"),
        (SlicerType.CREALITYPRINT, "Qidi"),
        (SlicerType.ELEGOOSLICER, "Qidi"),
        (SlicerType.ANYCUBICSLICER, "Qidi"),
        (SlicerType.PRUSASLICER, "QIDITechnology"),
        (SlicerType.SUPERSLICER, "QIDITechnology"),
    ],
)
def test_qidi_vendors_map_to_simplyprints_brand(slicer, vendor):
    # SimplyPrint's brand has been "Qidi", not "Qidi Tech", since October 2026.
    assert normalize_brand(slicer, vendor) == "qidi"


def test_qidi_models_lose_the_brand_prefix():
    assert strip_brand_from_name("qidi x-max 3", "qidi") == "x-max 3"
