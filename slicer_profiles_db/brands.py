"""
Brand name normalization.

Maps slicer-specific vendor/brand names to SimplyPrint brand names.
"""

import re

from .models import SlicerType

# Shared brand mappings used across multiple slicers.
# All keys MUST be lowercase.
_SHARED_BRAND_MAP: dict[str, str] = {
    "ratrig": "rat rig",
    "biqu": "bigtreetech",
    "artillery": "artillery 3d",
    "anker": "ankermake",
}

# Per-slicer brand name overrides.  Merged on top of the shared map.
_SLICER_OVERRIDES: dict[SlicerType, dict[str, str]] = {
    SlicerType.PRUSASLICER: {
        "prusaresearch": "prusa",
        "qiditechnology": "qidi tech",
    },
    SlicerType.ORCASLICER: {
        "qidi": "qidi tech",
        "bbl": "bambu lab",
        "twotrees": "two trees",
        "positron3d": "positron 3d",
        "folgertech": "folger tech",
        "flyingbear": "flying bear",
        "custom": "any generic printer",
    },
    SlicerType.BAMBUSTUDIO: {
        "qidi": "qidi tech",
        "bbl": "bambu lab",
        "twotrees": "two trees",
        "positron3d": "positron 3d",
        "folgertech": "folger tech",
        "flyingbear": "flying bear",
    },
    SlicerType.ELEGOOSLICER: {
        "qidi": "qidi tech",
        "bbl": "bambu lab",
        "twotrees": "two trees",
        "positron3d": "positron 3d",
        "folgertech": "folger tech",
        "flyingbear": "flying bear",
    },
    SlicerType.CURA: {
        "prusa3d": "prusa",
        "vivedino, formbot": "vivedino",
        "zav co., ltd.": "zav",
        "velleman n.v.": "velleman",
        "creality3d": "creality",
        "jgaurora": "JGMaker\\/JGAurora",
        "sovol 3d": "sovol",
        "ultimaker b.v.": "ultimaker",
        "german reprap": "reprap",
        "vorondesign": "voron",
        "nwa 3d llc": "nwa3d",
        "unknown": "any generic printer",
    },
    SlicerType.SUPERSLICER: {
        "prusaresearch": "prusa",
        "qiditechnology": "qidi tech",
    },
}

# Pre-built per-slicer brand maps (shared + overrides merged).
BRAND_MAPS: dict[SlicerType, dict[str, str]] = {}
for _slicer in SlicerType:
    _merged = dict(_SHARED_BRAND_MAP)
    _merged.update(_SLICER_OVERRIDES.get(_slicer, {}))
    BRAND_MAPS[_slicer] = _merged


def normalize_brand(slicer: SlicerType, vendor: str) -> str:
    """
    Return the SimplyPrint-normalized brand name for a slicer vendor string.

    Looks up the lowercased vendor in the per-slicer brand map.  If no mapping
    exists, returns the vendor lowercased as-is.
    """
    brand_map = BRAND_MAPS.get(slicer, {})
    key = vendor.lower()
    return brand_map.get(key, key)


def strip_brand_from_name(
    name: str, brand: str, original_brand: str | None = None
) -> str:
    """
    Remove the brand prefix from a printer model name.

    Tries the SimplyPrint brand first, then the original slicer brand.
    All comparisons are case-insensitive.

    Args:
        name: The printer name (already lowercased).
        brand: The SimplyPrint-normalized brand (lowercased).
        original_brand: The original slicer vendor name (lowercased), if it
            differs from *brand* after mapping.

    Returns:
        The name with the brand prefix stripped, or the original name if
        the brand wasn't found in it.
    """
    name_lower = name.lower()

    idx = name_lower.find(brand)
    if idx != -1:
        return name_lower[idx + len(brand):].strip()

    if original_brand and original_brand != brand:
        idx = name_lower.find(original_brand)
        if idx != -1:
            return name_lower[idx + len(original_brand):].strip()

    return name_lower
