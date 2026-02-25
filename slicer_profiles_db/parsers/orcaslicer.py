from ..models import SlicerType
from .slic3r_json import Slic3rJsonParser


class OrcaSlicerParser(Slic3rJsonParser):
    slicer_type = SlicerType.ORCASLICER
