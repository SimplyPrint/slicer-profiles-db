"""
slicer_profiles_db — Slicer Profile Database

Parses multiple slicer formats into one unified intermediate representation,
persists them to disk with versioned settings, and tracks changes across
slicer versions.
"""

from .models import (
    SlicerType,
    ProfileType,
    ParsedProfile,
    StoredProfile,
    IngestionReport,
    SourceConfig,
    VersionInfo,
    DownloadResult,
)
from .store import ProfileStore
from .index import ProfileIndex, is_profile_generic, is_profile_model_specific
from .conditions import evaluate_printer_condition
from .brands import normalize_brand, BRAND_MAPS
from .matching import match_printer_model
from .mapping import (
    map_printer_models,
    map_filament_profiles,
    map_print_profiles,
    export_output,
    run_mapping_pipeline,
)
from .pipeline import DownloadError, ParseError, StoreError

__all__ = [
    # Enums
    "SlicerType",
    "ProfileType",
    # Models
    "ParsedProfile",
    "StoredProfile",
    "IngestionReport",
    "SourceConfig",
    "VersionInfo",
    "DownloadResult",
    # Store & Index
    "ProfileStore",
    "ProfileIndex",
    # Helpers
    "evaluate_printer_condition",
    "is_profile_generic",
    "is_profile_model_specific",
    # Brands & Matching
    "normalize_brand",
    "BRAND_MAPS",
    "match_printer_model",
    # Mapping Pipeline
    "map_printer_models",
    "map_filament_profiles",
    "map_print_profiles",
    "export_output",
    "run_mapping_pipeline",
    # Exceptions
    "DownloadError",
    "ParseError",
    "StoreError",
]
