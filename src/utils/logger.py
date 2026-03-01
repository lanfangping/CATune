import os
import logging

class MyLogger:
    def __init__(self, logger_name, save_path='.', level='INFO'):
        self.logger = self._get_logger(logger_name, save_path, level)

    def _get_logger(self, name, save_path, level):
        """
        create logger function
        """
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level))

        # Custom format to include source (filename, function, line number)
        log_format = logging.Formatter(
            '[%(asctime)s %(levelname)s] [%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
        )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        logger.addHandler(console_handler)

        # File handler (optional)
        if save_path is not None:
            os.makedirs(save_path, exist_ok=True)
            file_handler = logging.FileHandler(os.path.join(save_path, f'{name}_log.txt'))
            file_handler.setFormatter(log_format)
            logger.addHandler(file_handler)

        return logger