"""
OFDRepo: reads OFD filament data from a local checkout of the data/ directory.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class OFDFilament:
    """A single filament entry from the OFD repository."""

    brand_id: str       # "bambu_lab"
    brand_name: str     # "Bambu Lab"
    material: str       # "PLA"
    filament_id: str    # "aero"
    filament_name: str  # "Aero"
    fs_path: str        # "bambu_lab/PLA/aero"
    slicer_settings: dict = field(default_factory=dict)
    slicer_ids: dict = field(default_factory=dict)


class OFDRepo:
    """Read-only access to an OFD data/ directory.

    Walks the standard layout::

        data/{brand_id}/brand.json
        data/{brand_id}/{material}/{filament_id}/filament.json

    and builds a list of :class:`OFDFilament` instances.
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.filaments: list[OFDFilament] = []
        self._load()

    def _load(self) -> None:
        if not self.data_dir.is_dir():
            raise FileNotFoundError(f"OFD data directory not found: {self.data_dir}")

        for brand_dir in sorted(self.data_dir.iterdir()):
            if not brand_dir.is_dir():
                continue

            brand_json = brand_dir / "brand.json"
            if not brand_json.exists():
                continue

            brand_data = json.loads(brand_json.read_text(encoding="utf-8"))
            brand_id = brand_dir.name
            brand_name = brand_data.get("name", "")

            for material_dir in sorted(brand_dir.iterdir()):
                if not material_dir.is_dir():
                    continue

                material = material_dir.name

                for filament_dir in sorted(material_dir.iterdir()):
                    if not filament_dir.is_dir():
                        continue

                    filament_path = filament_dir / "filament.json"
                    if not filament_path.exists():
                        continue

                    filament_data = json.loads(
                        filament_path.read_text(encoding="utf-8")
                    )
                    filament_id = filament_dir.name
                    filament_name = filament_data.get("name", filament_id)
                    fs_path = f"{brand_id}/{material}/{filament_id}"

                    self.filaments.append(OFDFilament(
                        brand_id=brand_id,
                        brand_name=brand_name,
                        material=material,
                        filament_id=filament_id,
                        filament_name=filament_name,
                        fs_path=fs_path,
                        slicer_settings=filament_data.get("slicer_settings", {}),
                        slicer_ids=filament_data.get("slicer_ids", {}),
                    ))

        logger.info("Loaded %d filaments from OFD", len(self.filaments))
