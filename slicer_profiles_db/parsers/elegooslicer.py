from ..models import SlicerType
from .slic3r_json import Slic3rJsonParser


class ElegooSlicerParser(Slic3rJsonParser):
    slicer_type = SlicerType.ELEGOOSLICER
