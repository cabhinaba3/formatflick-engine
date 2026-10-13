import os
from typing import Any
from .flick import flick
from .global_var import CSV, TSV, XLSX, JSON, XML, VALID_EXTENSIONS, RESULT_FILE, FILE_MODE, NFILE_MODE
from .engine.xEngine import xEngine, dest_xEngine
from .engine.jEngine import jEngine, dest_jEngine
from .engine.cEngine import cEngine, dest_cEngine

STREAMING_THRESHOLD_BYTES = 50 * 1024 * 1024  # 50 MB default threshold
VALID_CONVERSION_KEYS = frozenset((s, d) for s in VALID_EXTENSIONS for d in VALID_EXTENSIONS if s != d)
SRC_ENGINE_MAP = {CSV: cEngine, TSV: cEngine, XLSX: cEngine, JSON: jEngine, XML: xEngine}
DEST_ENGINE_MAP = {CSV: dest_cEngine, TSV: dest_cEngine, XLSX: dest_cEngine, JSON: dest_jEngine, XML: dest_xEngine}


class formatflick(flick):
    def __init__(self,
                 source,
                 destination=None,
                 destination_extension: str = None,
                 verbosity: int = None,
                 *args, **kwargs):
        """
        Initialises as instance of formatflick class with the provided source and some optional parameters:
        Right now this module operates two modes => 'file' and 'nfile'
        - 'file' mode will produce a destination file (which will may or may nor be provided by user)
        - 'nfile' mode will produce a destination object (which should be provided by the user)

        Inputs are as follows (an Extensive list)
        1. source: source file path. Ensure that you have entered path in accordance to the os
        2. destination:
            By default, it is set as None
        3. destination_extension:
            By default, it is set as None
            Remember to you have to put the number in such a way that module should be able to extract the extention to which you want to conver
            You either have to provide destination or destination extention.
            Below are some combinations of destination and destination_extension that will work,
                - destination = None and destination_extension = <dot> <any valid extension>
                - destination = <file path with a valid extension> and destination_extension (will be ignored)
        4. mode:
            There are two file modes for formatflick module(optional field)
            The value of mode is set to 'file' mode
                - 'file' outputs the resultant data into destination file
                - 'nfile' outputs the resultant data into destination file and also returns a dataframe
        5. verbosity:
            verbosity defines the level of logging user want while executing the module.
            The values are consistent with the python logging module;
            By default, it is set to 3
        """
        super().__init__(source=source,
                         destination=destination,
                         destination_extension=destination_extension,
                         verbosity=verbosity,
                         *args, **kwargs)

        # for formatflick the default behavior is FILE_MODE

        if self.mode is None:
            self.mode = FILE_MODE

        self.kwargs = kwargs
        self.streaming_threshold_bytes = kwargs.get(
            "streaming_threshold_bytes",
            kwargs.get("streaming_threshold", kwargs.get("threshold", STREAMING_THRESHOLD_BYTES))
        )
        self.chunk_size = kwargs.get("chunk_size", 50000)
        self.force_streaming = kwargs.get("force_streaming", False)

        # Check the source file is valid or not
        self.check_source_validity()

        # check the destination file, extensions are valid or not
        self.check_destination_validity()

        # check the validity of conversion
        self.is_valid_conversion()

    def check_source_validity(self) -> None:
        """
        Utility function to check if the source is valid or not
        In this function the followings are checked
        - source file path exists or not
        - If exists check is the source extension is valid extension or not

        """
        # Check if the Source is valid or not
        #
        if not os.path.exists(self.source):
            raise Exception("The source path dont exits")
        self.source_extension = super().get_file_extension(self.source)
        super().is_valid_file_extension(self.source_extension)

    def check_destination_validity(self) -> None:
        """
        Utility function to check if the destination file is valid or not
        Depending on the file_mode we decide what to do
        If file_mode:
            In this function, the following logic is implemented,
            - If destination and destination path both are none, raise an exception
            - If destination is None
                - check for destination_extension. If it is not a valid extension, raise an exception
                - if it is a valid exception, destination file name would be "result" and form the destination file path

        If nfile_mode:
            If the destination_extension is None we will raise Exception.
            Destination Path will be ignored
        """
        if self.is_file_mode():
            if self.destination is None and self.destination_extension is None:
                raise Exception("destination and destination_extension both cannot be None.")
            elif self.destination is None:
                super().is_valid_file_extension(self.destination_extension)
                self.destination = os.path.join(os.getcwd(), RESULT_FILE + self.destination_extension)
            else:
                self.destination_extension = super().get_file_extension(self.destination)
                super().is_valid_file_extension(self.destination_extension)
        else:
            if self.destination_extension is None:
                raise Exception("destination_extension cannot be None for nfile mode")
            else:
                super().is_valid_file_extension(self.destination_extension)

    def is_valid_conversion(self):
        conversion_key = (self.source_extension, self.destination_extension)
        if conversion_key not in VALID_CONVERSION_KEYS:
            raise Exception(f"The following conversion between {self.source_extension} and {self.destination_extension} is not allowed")
        return conversion_key

    def should_stream(self) -> bool:
        """
        Determines whether to route to chunked streaming pipeline
        based on source file size and configuration.
        """
        if self.force_streaming:
            return True
        try:
            if os.path.exists(self.source):
                return os.path.getsize(self.source) >= self.streaming_threshold_bytes
        except OSError:
            pass
        return False

    def convert(self):
        """
        Convert method for formatflick. Selects execution path adaptively:
        - Datasets < 50MB: In-memory vectorized pipeline (Polars/PyArrow/lxml/orjson)
          for maximum throughput.
        - Datasets >= 50MB (or force_streaming=True): Chunked streaming coordinator
          guaranteeing constant bounded memory (<100MB RSS) with zero OOM errors.
        """
        if self.should_stream():
            return self._convert_streaming()
        return self._convert_in_memory()

    def _convert_in_memory(self):
        engine_cls = SRC_ENGINE_MAP[self.source_extension]
        _engine_ = engine_cls(self.source, mode=self.mode, source_extension=self.source_extension)

        obj = _engine_.read_file()
        if self.source_extension == JSON:
            obj = obj[1]

        convert_method = getattr(_engine_, f"to_{self.destination_extension[1:]}")
        d_obj = convert_method(obj)

        if self.mode == FILE_MODE:
            dest_cls = DEST_ENGINE_MAP[self.destination_extension]
            d_engine = dest_cls(d_obj, self.destination, destination_extension=self.destination_extension)
            d_engine.to_destination()

        return d_obj

    def _convert_streaming(self):
        from .engine.streaming import stream_convert, stream_read_memory_efficient

        stream_kwargs = {k: v for k, v in self.kwargs.items() if k != "chunk_size"}
        if self.is_file_mode():
            res = stream_convert(
                source=self.source,
                destination=self.destination,
                src_ext=self.source_extension,
                dest_ext=self.destination_extension,
                chunk_size=self.chunk_size,
                **stream_kwargs
            )
            return res
        else:
            return stream_read_memory_efficient(
                source=self.source,
                src_ext=self.source_extension,
                dest_ext=self.destination_extension,
                chunk_size=self.chunk_size,
                **stream_kwargs
            )


Formatflick = formatflick
formatflick.Formatflick = formatflick
formatflick.formatflick = formatflick
Formatflick.STREAMING_THRESHOLD_BYTES = STREAMING_THRESHOLD_BYTES
