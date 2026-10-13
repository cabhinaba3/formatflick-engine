from pathlib import Path
from typing import Any


class engine:
    def __init__(self,
                 source: Path,
                 mode: str,
                 *args, **kwargs
                 ):
        self.source = source
        self.args = args
        self.kwargs = kwargs
        self.mode = mode

        self.obj = None

    def read_file(self) -> Any:
        """reads the file and sends the object"""
        pass

    @staticmethod
    def to_csv(obj):
        pass

    @staticmethod
    def to_tsv(obj):
        pass

    @staticmethod
    def to_xlsx(obj):
        pass

    @staticmethod
    def to_json(obj):
        pass

    @staticmethod
    def to_xml(obj, root=None):
        pass


class dest_engine:
    def __init__(self, obj, destination, *args, **kwargs):
        self.obj = obj
        self.destination = destination
        self.kwargs = kwargs

    def to_destination(self):
        """
        Occasionally used when we have to convert that into a destination file
        Also depends on different file formats
        """
        pass
