import yaml
from inesdata_mov_datasets.settings import SourceEmtSettings, StorageSettings
import pytest

yaml_config = """
sources:
  emt:
    credentials:
      email: juag
      password: temporal01
      x_client_id: xx
      passkey: xx
    stops: [1,2]
"""

def test_wrong_emt_credentiales():
    yaml_config = """
        sources:
            emt:
                credentials:
                    email: null
                    password: null
                    x_client_id: null
                    passkey: null
                stops: [1,2]
        """


    settings = yaml.safe_load(yaml_config)

    # if no credentials are provided, an error is expected
    with pytest.raises(ValueError):
        SourceEmtSettings(**settings["sources"]["emt"])

def test_wrong_storage():
    yaml_config = """
        storage:
            default: null
            config:
                minio:
                    access_key: my_access_key
                    secret_key: my_secret_key
                    endpoint: minio-endpoint
                    secure: True
                    bucket: my_bucket
                local:
                    path: /path/to/save/datasets
    """

    settings = yaml.safe_load(yaml_config)
    with pytest.raises(ValueError):
        StorageSettings(**settings["storage"])

