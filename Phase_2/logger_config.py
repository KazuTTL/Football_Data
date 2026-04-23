import logging
import os
import sys

# Tao thu muc logs
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name):
    """
    Khoi tao vaf tra ve logger voi Console Handler va File Handler.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Chi tao handlers neu logger chua co, tranh bi ghi log dup
    if not logger.handlers:
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # In ra Terminal
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Ghi ra file
        file_handler = logging.FileHandler(
            os.path.join(LOG_DIR, "phase2_pipeline.log"), 
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger
