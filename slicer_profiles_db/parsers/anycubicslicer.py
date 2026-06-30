from ..models import SlicerType
from .slic3r_json import Slic3rJsonParser


class AnycubicSlicerParser(Slic3rJsonParser):
    slicer_type = SlicerType.ANYCUBICSLICER
