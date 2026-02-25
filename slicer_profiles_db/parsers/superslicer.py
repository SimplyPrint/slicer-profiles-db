from ..models import SlicerType
from .prusaslicer import PrusaSlicerParser


class SuperSlicerParser(PrusaSlicerParser):
    slicer_type = SlicerType.SUPERSLICER
