"""
slicer_profiles_db.ofd — OFD (Open Filament Database) integration.

Provides OFD repo reading, filament indexing, and slicer profile mapping.
"""

from .repo import OFDRepo, OFDFilament
from .index import OFDFilamentIndex
from .mapper import SlicerMapper, MappingResult, MappingConflict, MappingReport
from .vendor_map import get_profile_prefixes, BRAND_PREFIX_OVERRIDES

__all__ = [
    "OFDRepo",
    "OFDFilament",
    "OFDFilamentIndex",
    "SlicerMapper",
    "MappingResult",
    "MappingConflict",
    "MappingReport",
    "get_profile_prefixes",
    "BRAND_PREFIX_OVERRIDES",
]
