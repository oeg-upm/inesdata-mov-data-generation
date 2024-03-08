import os

from loguru import logger

from inesdata_mov_datasets.settings import Settings


def import_extract_logger(settings: Settings, source: str):
    """Log configuration."""
    logger.remove()  # disable all previous handlers

    logger.configure(extra={"source": source})  # add new formatted variables

    log_folder = settings.storage.logs.path
    log_path = os.path.join(log_folder, "extract", "inesdata_mov_{time:YYYY_MM_DD}.log")
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


def import_create_logger(settings: Settings, source: str):
    """Log configuration."""
    logger.remove()  # disable all previous handlers

    logger.configure(extra={"source": source})  # add new formatted variables

    log_folder = settings.storage.logs.path
    log_path = os.path.join(log_folder, "create", "inesdata_mov_{time:YYYY_MM_DD}.log")
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
