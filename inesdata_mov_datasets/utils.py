from inesdata_mov_datasets.settings import Settings
import yaml

def read_settings(path: str) -> Settings:
    """Read settings from yaml file

    Args:
        path (str): path to configuration file.

    Returns:
        Settings: Pydantic object to manage paramters.
    """
    with open(path, "r") as file:
        return Settings(**yaml.safe_load(file))
