import os
import logging

def setup_logging():
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Get the root logger
    logger = logging.getLogger()
    
    # Check if the logger already has handlers to avoid adding duplicate handlers
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)

        # Create handlers
        file_handler = logging.FileHandler("logs/full_log.log")
        stream_handler = logging.StreamHandler()

        # Set formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Apply formatter to handlers
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger