import logging
import sys

log_level_map = {
    0: logging.DEBUG,
    1: logging.INFO,
    2: logging.WARNING,
    3: logging.ERROR,
    4: logging.CRITICAL
}


class logger:
    """
    Logger class used in the entire class of formatflick.It defined the logger level and several methods for logging
    the message.
    """
    def __init__(self,
                 verb: int
                 , *args, **kwargs):
        """
        Initializes the logger class for the entire module
        Input:
        - verb:
            Mandatory input. Defines the level of logging. Consistent with python logger module.
            Mapping of verb variable Logger level mapping,
                0. debug
                1. info
                2. warnings
                3. error
                4. critical

        - args: optional
        - kwargs: optional

        The logger also defines the logging format
            - {asctime} {levelname} {message}
            the dateformat is Y-m-d h-m-s

        Be sure to put verb as 0 to 3. Failing to do so with raise an exception
        """
        self.logger = logging.getLogger('project_logger')

        if log_level_map.get(verb, None) is None:
            raise Exception(f"{verb} should be between 0 to 3")
        self.level = log_level_map.get(verb, None)
        self.logger.setLevel(self.level)
        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

    # Utility function for getting log messages
    def log_unsupported_file_conversion_error(self, source, destination):
        """
        log unsupported file conversion error
        Input:
        - source path
        - destination path
        """
        self.logger.error(f"{source} to {destination} is unsupported file conversion by default")

    # log processing time
    def log_time(self, start_time, end_time):
        """
        log the entire processing time
        Input:
        - start_time
        - end_time
        """
        self.logger.info(f"Time Taken: {end_time - start_time}")

    # log validation of files
    def log_validating_file_encoding(self, file_path):
        """
        log validating the file encoding(source or destination)
        Input:
        - file_path: source/destination file path
        """
        self.logger.info(f"Validating {file_path} Encoding")

    def log_validated_file_encoding(self, file_path):
        """
        log validated file encoding
        Input:
        - file_path: source/destination file path
        """
        self.logger.info(f"{file_path} encoding is validated")

    def log_invalid_file_encoding(self, file_path):
        """
        log invalid file encoding
        Input:
        - file_path: source/destination file path
        """
        self.logger.error(f"Invalid file encoding for {file_path}")

    # log file name handling
    def log_filename_extraction_error(self, file_path):
        """
        log file name extraction error: error occurred during file name extraction
        Input:
        - file_path: source/destination file path
        """
        self.logger.error(f"Error occurred while extracting File Name: {file_path}", exc_info=False)

    # log file extension handling
    def log_extension_extraction_msg(self, src):
        """
        log file extension extraction: validation
        Input:
            - src: source/destination file path
        """
        self.logger.info(f"Validating the {src} file extension")

    def log_extension_extraction_error(self, extension):
        """
        log error log during extension extraction
        Input:
        - extension: source/destination file extension
        """
        self.logger.error(f"Error occurred while getting File Extension: {extension}", exc_info=False)

    def log_valid_extension_msg(self):
        """
        log that the file extension is valid.
        No input
        """
        self.logger.info("Valid file extension")

    def log_invalid_extension_error(self, extension):
        """
        log that the file extension is invalid
        Input:
        - extension: source/destination file extension
        """
        self.logger.error(f"{extension} is not a valid File Extension")

    def log_invalid_file_path(self, path):
        """
        log invalid file path
        Input:
        - path: source/destination file path
        """
        self.logger.error(f"{path} does not exist")

    def log_path_exists_waring(self, path):
        """
        log that a file is created with that name
        Input:
        - path: source/destination path
        """
        self.logger.warning(f"Path: {path} already exists")

    def log_create_file(self):
        """
        log destination file creation status=> successful
        """
        self.logger.info("Destination File created successfully")

    def log_create_file_error(self):
        """
        log destination file creation status=> unsuccessful
        """
        self.logger.error("Error in creating Destination File")

    # Initiating specific engine
    def log_initiating_engine(self, engine):
        """
        log initiation of the core engine
        """
        self.logger.info(f"Initiating {engine} engine...")

    def log_process_initialization(self, source, destination):
        """log process initiation of file format conversion"""
        self.logger.info(f"Converting from {source} to {destination}")
        self.logger.info(f"Reading the {source} file")

    def log_process_completion(self, destination):
        """
        log process completion messages
        Input:
        - destination: destination file path
        """
        self.logger.info("Conversion Complete")
        self.logger.info(f"Resultant file can be seen at {destination}")

    # custom log
    def log_custom_message(self, *args, **kwargs):
        """
        Custom log message. This function can be changed for users own need.
        """
        msg = kwargs.get("msg", None)
        self.logger.info(msg)
