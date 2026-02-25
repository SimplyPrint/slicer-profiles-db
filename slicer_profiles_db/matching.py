"""
Printer model fuzzy matching algorithms.

13 algorithms that try progressively looser transformations to match
slicer printer names against SimplyPrint model names.
"""

import re
from typing import Callable

from .brands import normalize_brand, strip_brand_from_name, BRAND_MAPS

# Pre-compiled patterns used by multiple algorithms.
_MMU_RE = re.compile(r"mmu[0-9]s?")
_BED_SIZE_RE = re.compile(r"[0-9]+mm3?")
_VORON_VERSION_RE = re.compile(r"v([0-9])")


def direct_comparison(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == slicer_name


def remove_dashes(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name.replace("-", " ") == slicer_name.replace("-", " ")


def remove_spaces(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name.replace(" ", "") == slicer_name.replace(" ", "")


def remove_parentheses(sp_name: str, slicer_name: str, brand: str) -> bool:
    return (
        sp_name.replace("(", "").replace(")", "")
        == slicer_name.replace("(", "").replace(")", "")
    )


def remove_bltouch(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == slicer_name.replace("bltouch", "").strip()


def remove_mmu(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == re.sub(_MMU_RE, "", slicer_name).strip()


def remove_input_shaper(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == slicer_name.replace("input shaper", "").strip()


def remove_bed_size(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == re.sub(_BED_SIZE_RE, "", slicer_name).strip()


def voron_version_convert(sp_name: str, slicer_name: str, brand: str) -> bool:
    if brand != "voron":
        return False
    slicer_name = re.sub(_BED_SIZE_RE, "", slicer_name).strip()
    slicer_name = slicer_name.replace("zero", "v0")
    match = re.match(_VORON_VERSION_RE, slicer_name)
    if match:
        ver_num = match.group(1)
        if ver_num == "1":
            ver_num = f"v{ver_num}"
        slicer_name = re.sub(_VORON_VERSION_RE, f"{ver_num}.x", slicer_name)
        return sp_name.replace("voron", "").strip() == slicer_name
    return False


def prusa_split_model_names(sp_name: str, slicer_name: str, brand: str) -> bool:
    if brand != "prusa" or "&&" not in slicer_name:
        return False
    i3 = slicer_name.startswith("i3")
    if i3:
        slicer_name = slicer_name.removeprefix("i3").strip()
    slicer_name = re.sub(_MMU_RE, "", slicer_name).strip()
    slicer_name = slicer_name.replace("input shaper", "").strip()
    split_names = slicer_name.split("&&")
    for part in split_names:
        candidate = part.strip()
        if i3:
            candidate = "i3 " + candidate
        if sp_name == candidate:
            return True
    return False


def sovol_split_model_names(sp_name: str, slicer_name: str, brand: str) -> bool:
    if brand != "sovol" or "/" not in slicer_name:
        return False
    slicer_name = slicer_name.replace("bltouch", "").strip()
    for part in slicer_name.split("/"):
        if sp_name == part.strip():
            return True
    return False


def ratrig_vcore(sp_name: str, slicer_name: str, brand: str) -> bool:
    if brand != "rat rig" or not slicer_name.startswith("v-core"):
        return False
    sp_name = sp_name.replace("(", "").replace(")", "")
    slicer_name = slicer_name.replace("corexy ", "").replace("hybrid ", "")
    slicer_name = re.sub(r"-(?=[0-9])", " ", slicer_name)
    slicer_name = re.sub(r"3\.[0-9]", "3", slicer_name)
    slicer_name = re.sub(r"4\.[0-9]", "4", slicer_name)
    sp_name = re.sub(r"4\.[0-9]", "4", sp_name)
    slicer_name = re.sub(r"(?<=[0-9])mm", "", slicer_name)
    sp_name = re.sub(r"(?<=[0-9])mm", "", sp_name)
    slicer_name = slicer_name.replace(" copy mode", "").replace(" mirror mode", "")
    return sp_name == slicer_name


def alternate_remove_bed_size(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == re.sub(r" [0-9]{3,}$", "", slicer_name)


# Ordered list of all matching algorithms, tried in sequence.
CHECK_MODEL_ALGOS: list[Callable[[str, str, str], bool]] = [
    direct_comparison,
    remove_dashes,
    remove_spaces,
    remove_parentheses,
    remove_bltouch,
    remove_mmu,
    remove_input_shaper,
    remove_bed_size,
    voron_version_convert,
    prusa_split_model_names,
    sovol_split_model_names,
    ratrig_vcore,
    alternate_remove_bed_size,
]


def match_printer_model(
    sp_models: list[dict],
    sp_brands: list[str],
    sp_slicer_names: dict[int, list[str]],
    brand: str,
    printer_name: str,
    brand_map: dict[str, str],
) -> set[int]:
    """
    Run all matching algorithms against SimplyPrint models, return matching IDs.

    Args:
        sp_models: SimplyPrint model list (each with 'id', 'brand', 'name').
        sp_brands: Lowercased list of SimplyPrint brand names.
        sp_slicer_names: {model_id: [lowered slicer profile names]} from SP data.
        brand: Slicer vendor/brand name (original, not yet normalized).
        printer_name: Printer model name from the slicer profile.
        brand_map: Per-slicer brand mapping dict (from BRAND_MAPS).

    Returns:
        Set of matched SimplyPrint model IDs (may be empty).
    """
    printer_name = printer_name.strip().lower()
    brand = brand.lower()
    old_brand: str | None = None

    # Map slicer brand → SimplyPrint brand
    if brand in brand_map:
        old_brand = brand
        brand = brand_map[brand]

    if brand not in sp_brands:
        return set()

    # Strip brand prefix from printer name
    printer_name = strip_brand_from_name(printer_name, brand, old_brand)

    ids: set[int] = set()

    # Try all 13 algorithms against each SP model of the same brand
    for model in sp_models:
        if model["brand"] != brand:
            continue
        model_name = model["name"]
        # Strip brand from SP model name too
        if brand in model_name or (old_brand and old_brand in model_name):
            pattern = re.escape(brand)
            if old_brand:
                pattern += "|" + re.escape(old_brand)
            model_name = re.sub(pattern, "", model_name).strip()
        for algo in CHECK_MODEL_ALGOS:
            if algo(model_name, printer_name, brand):
                ids.add(model["id"])
                break

    # Fallback: check SimplyPrint's slicerProfileNames field
    for model_id, name_list in sp_slicer_names.items():
        for spn in name_list:
            cleaned = spn
            if brand in spn or (old_brand and old_brand in spn):
                pattern = re.escape(brand)
                if old_brand:
                    pattern += "|" + re.escape(old_brand)
                cleaned = re.sub(pattern, "", spn).strip()
            if printer_name == cleaned:
                ids.add(model_id)

    return ids
