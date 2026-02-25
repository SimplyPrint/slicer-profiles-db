from ..models import SlicerType
from .slic3r_json import Slic3rJsonParser


class BambuStudioParser(Slic3rJsonParser):
    slicer_type = SlicerType.BAMBUSTUDIO
