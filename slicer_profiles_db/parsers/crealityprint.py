from ..models import SlicerType
from .slic3r_json import Slic3rJsonParser


class CrealityPrintParser(Slic3rJsonParser):
    slicer_type = SlicerType.CREALITYPRINT
