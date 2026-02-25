from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from ..models import SlicerType, ProfileType, ParsedProfile


class BaseParser(ABC):
    @property
    @abstractmethod
    def slicer_type(self) -> SlicerType: ...

    @abstractmethod
    def parse_file(self, path: Path) -> ParsedProfile: ...

    def parse_directory(
        self,
        directory: Path,
        profile_type_filter: list[ProfileType] | None = None,
    ) -> Iterator[ParsedProfile]:
        """Parse all profile files from a slicer's profile directory.

        Args:
            directory: Root directory containing vendor subdirectories.
            profile_type_filter: If set, only yield profiles matching these types.
        """
        for vendor_dir in sorted(directory.iterdir()):
            if not vendor_dir.is_dir():
                continue
            for path in self._glob_profiles(vendor_dir):
                try:
                    profile = self.parse_file(path)
                    if profile_type_filter and profile.profile_type not in profile_type_filter:
                        continue
                    yield profile
                except Exception:
                    continue

    @abstractmethod
    def _glob_profiles(self, vendor_dir: Path) -> Iterator[Path]:
        """Yield profile file paths within a vendor directory."""
        ...
