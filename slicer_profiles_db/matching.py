"""Printer model fuzzy matching algorithms.

Canonical model names and their slicer aliases are treated as names owned by
the model's brand.  Every name follows the same normalisation and matching
pipeline so an alias can benefit from the same fuzzy matching as a canonical
name without leaking into another brand.
"""

import re
import unicodedata
from typing import Callable

from .brands import strip_brand_from_name

# Pre-compiled patterns used by multiple algorithms.
_MMU_RE = re.compile(r"mmu[0-9]s?")
_BED_SIZE_RE = re.compile(r"[0-9]+mm3?")
_VORON_VERSION_RE = re.compile(r"v([0-9])")
_WHITESPACE_RE = re.compile(r"\s+")


def _normalise_name(value: str) -> str:
    """Return a stable form while retaining separators used by fuzzy rules."""
    value = unicodedata.normalize("NFKC", value).casefold().replace("_", " ")
    return _WHITESPACE_RE.sub(" ", value).strip()


def _comparison_key(value: str) -> str:
    """Collapse separators while preserving identity-bearing symbols.

    Unicode assigns mathematical, currency, modifier, and other symbols to
    the ``S*`` categories.  Keeping those categories means names such as
    ``Model`` and ``Model+`` cannot collapse to the same key, while ordinary
    punctuation, whitespace, and underscores remain interchangeable.
    """
    return "".join(
        character
        for character in value
        if character.isalnum() or unicodedata.category(character).startswith(("M", "S"))
    )


def direct_comparison(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name == slicer_name


def normalised_comparison(sp_name: str, slicer_name: str, brand: str) -> bool:
    """Compare names without case or separator-only differences."""
    sp_key = _comparison_key(sp_name)
    return bool(sp_key) and sp_key == _comparison_key(slicer_name)


def remove_dashes(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name.replace("-", " ") == slicer_name.replace("-", " ")


def remove_spaces(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name.replace(" ", "") == slicer_name.replace(" ", "")


def remove_parentheses(sp_name: str, slicer_name: str, brand: str) -> bool:
    return sp_name.replace("(", "").replace(")", "") == slicer_name.replace(
        "(", ""
    ).replace(")", "")


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
    normalised_comparison,
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
        sp_brands: List of SimplyPrint brand names.
        sp_slicer_names: ``{model_id: [slicer profile names]}`` from SP data.
        brand: Slicer vendor/brand name (original, not yet normalized).
        printer_name: Printer model name from the slicer profile.
        brand_map: Per-slicer brand mapping dict (from BRAND_MAPS).

    Returns:
        Set of matched SimplyPrint model IDs (may be empty).
    """
    printer_name = _normalise_name(printer_name)
    brand = _normalise_name(brand)
    old_brand: str | None = None

    # Map slicer brand → SimplyPrint brand
    normalised_brand_map = {
        _normalise_name(source): _normalise_name(target)
        for source, target in brand_map.items()
    }
    if brand in normalised_brand_map:
        old_brand = brand
        brand = normalised_brand_map[brand]

    if brand not in {_normalise_name(item) for item in sp_brands}:
        return set()

    # Strip brand prefix from printer name
    printer_name = strip_brand_from_name(printer_name, brand, old_brand)

    ids: set[int] = set()

    # Canonical names and aliases are both brand-owned candidates.  Keeping
    # them inside this loop prevents an alias belonging to another brand from
    # being returned merely because its text happens to match.
    for model in sp_models:
        if _normalise_name(str(model.get("brand", ""))) != brand:
            continue

        model_id = model["id"]
        candidates = [model.get("name", ""), *sp_slicer_names.get(model_id, [])]
        for candidate in candidates:
            if not isinstance(candidate, str):
                continue
            candidate_name = strip_brand_from_name(
                _normalise_name(candidate), brand, old_brand
            )
            if any(
                algo(candidate_name, printer_name, brand) for algo in CHECK_MODEL_ALGOS
            ):
                ids.add(model_id)
                break

    return ids
