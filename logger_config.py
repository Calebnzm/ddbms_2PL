import logging
import sys

def setup_logger(name=None):
    """
    Configure and return a logger instance.
    If name is None, configures the root logger.
    """
    logger = logging.getLogger(name)
    
    # Check if handlers are already configured to avoid duplicates
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File Handler
        file_handler = logging.FileHandler('system.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger
