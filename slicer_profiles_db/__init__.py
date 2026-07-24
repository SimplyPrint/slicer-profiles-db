"""
slicer_profiles_db — Slicer Profile Database

Parses multiple slicer formats into one unified intermediate representation,
persists them to disk with versioned settings, and tracks changes across
slicer versions.
"""

from .brands import BRAND_MAPS, normalize_brand
from .conditions import evaluate_printer_condition
from .index import ProfileIndex, is_profile_generic, is_profile_model_specific
from .mapping import (
    export_output,
    map_filament_profiles,
    map_print_profiles,
    map_printer_models,
    run_mapping_pipeline,
)
from .matching import match_printer_model
from .models import (
    DownloadResult,
    IngestionReport,
    ParsedProfile,
    ProfileType,
    SlicerType,
    SourceConfig,
    StoredProfile,
    VersionInfo,
)
from .pipeline import DownloadError, ParseError, StoreError
from .store import ProfileStore

__all__ = [
    "BRAND_MAPS",
    "DownloadError",
    "DownloadResult",
    "IngestionReport",
    "ParseError",
    "ParsedProfile",
    "ProfileIndex",
    "ProfileStore",
    "ProfileType",
    "SlicerType",
    "SourceConfig",
    "StoreError",
    "StoredProfile",
    "VersionInfo",
    "evaluate_printer_condition",
    "export_output",
    "is_profile_generic",
    "is_profile_model_specific",
    "map_filament_profiles",
    "map_print_profiles",
    "map_printer_models",
    "match_printer_model",
    "normalize_brand",
    "run_mapping_pipeline",
]
