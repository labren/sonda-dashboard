import logging
import os

def setup_logger():
    """
    Configures a logger to log errors to a file named 'error_log.txt' in the parent directory of the script.
    The log file will contain error messages with timestamps.
    """

    logger = logging.getLogger('sonda_translator')
    logger.setLevel(logging.ERROR)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir.endswith('sdt'):
        script_dir = os.path.dirname(script_dir)
    log_file = os.path.join(script_dir, 'error_log.txt')
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('----------------------------------------\nDATA DO PROCESSAMENTO: %(asctime)s\n%(message)s\n')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger