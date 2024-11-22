import os
import logging

class Logging:

    def __init__(self, log_dir='logs', log_file='logs.log', log_level='INFO'):
        """
        Initialize the logger setup.

        :param log_dir: Directory to storee the log files
        :param log_file: Log file name.
        :param log_level: Min level for logging.
        """
        self.log_dir = log_dir
        self.log_file = log_file
        self.log_level = self._get_log_level(log_level)
        self.log_save_path = f"{self.log_dir}/{self.log_file}"
        self.logger = logging.getLogger('RadioSongAnalysis')

    def _set_formatter(self, handler):
        """Applies a formatter to the provided handler"""
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

    def _get_log_level(self, level_str):
        """
        Maps the log level string to the corresponding logging module constant.

        :param level_str: Log level as a string (e.g., 'DEBUG', 'INFO').
        :return: Corresponding logging level (e.g., logging.DEBUG, logging.INFO).
        """
        levels = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL,
        }
        return levels.get(level_str.upper(), logging.INFO) # Default to INFO if not specified

    def setup_logging(self):
        """Sets up logging configuration based on the environment"""

        # Create logs directory if it doesn't exist
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
                
        # Check if the logger already has handlers to avoid adding duplicate handlers
        if not self.logger.hasHandlers():
            self.logger.setLevel(self.log_level)

            # Create handlers
            file_handler = logging.FileHandler(self.log_save_path)
            stream_handler = logging.StreamHandler()

            # Set formatter
            self._set_formatter(file_handler)
            self.logger.addHandler(file_handler)

            # Add handlers to logger
            self._set_formatter(stream_handler)
            self.logger.addHandler(stream_handler)

        return self.logger