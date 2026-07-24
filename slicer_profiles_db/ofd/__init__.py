"""
slicer_profiles_db.ofd — OFD (Open Filament Database) integration.

Provides OFD repo reading, filament indexing, and slicer profile mapping.
"""

from .index import OFDFilamentIndex
from .mapper import MappingReport, MappingResult, SlicerMapper
from .repo import OFDFilament, OFDRepo
from .vendor_map import BRAND_PREFIX_OVERRIDES, get_profile_prefixes

__all__ = [
    "BRAND_PREFIX_OVERRIDES",
    "MappingReport",
    "MappingResult",
    "OFDFilament",
    "OFDFilamentIndex",
    "OFDRepo",
    "SlicerMapper",
    "get_profile_prefixes",
]
