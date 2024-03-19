import os

from loguru import logger

from inesdata_mov_datasets.settings import Settings


def instantiate_logger(settings: Settings, source: str, method: str):
    """Log configuration."""
    logger.remove()  # disable all previous handlers

    logger.configure(extra={"source": source})  # add new formatted variables

    log_folder = settings.storage.logs.path
    log_path = os.path.join(log_folder, method, "inesdata_mov_{time:YYYY_MM_DD}.log")
    log_level = settings.storage.logs.level
    log_format = "[{time:YYYY-MM-DD HH:mm:ss}] - [{process}] - [{level}] - [{extra[source]}] - {name}.{function} - {message}"
    log_rotation = "00:00"  # each rotation a new log file is created
    log_compression = "tar.gz"  # each rotation the files are compressed

    logger.add(
        log_path,
        level=log_level,
        format=log_format,
        rotation=log_rotation,
        compression=log_compression,
    )
