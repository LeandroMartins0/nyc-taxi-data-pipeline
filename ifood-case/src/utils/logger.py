import logging
from typing import Optional
from pathlib import Path


class LoggerFactory:
    """
    Factory for creating and configuring loggers across the application.
    Ensures no duplicate handlers and supports custom formatting and levels.
    """

    _configured_loggers = {}

    @classmethod
    def get_logger(cls, name: str, level: int = logging.INFO, to_file: Optional[str] = None) -> logging.Logger:
        """
        Returns a logger with the specified name and configures it if not already done.

        Args:
            name (str): Logger name (usually __name__ from the module).
            level (int): Logging level (default: logging.INFO).
            to_file (str, optional): If set, logs will also be written to the specified file.

        Returns:
            logging.Logger: Configured logger instance.
        """
        if name in cls._configured_loggers:
            return cls._configured_loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if to_file:
            log_path = Path(to_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        cls._configured_loggers[name] = logger
        return logger
