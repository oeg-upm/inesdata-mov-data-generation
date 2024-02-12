from typing import List, Optional

from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings

# Sources settings


class SourceEmtCredentialsSettings(BaseModel):
    email: Optional[str] = None
    password: Optional[str] = None
    x_client_id: Optional[str] = None
    passkey: Optional[str] = None

    @model_validator(mode="after")
    def check_passwords_match(self) -> "EmtCredentialsSettings":
        email = self.email
        password = self.password
        x_client_id = self.x_client_id
        passkey = self.passkey

        # validate if are provided
        if email is None and password is None and x_client_id is None and passkey is None:
            raise ValueError("Provide email and password OR x_client_id and passkey")

        # if all fields are provided, we use x_client_id and passkey
        if (
            email is not None
            and password is not None
            and x_client_id is not None
            and passkey is not None
        ):
            self.email = None
            self.password = None
        return self


class SourceEmtSettings(BaseModel):
    credentials: SourceEmtCredentialsSettings
    stops: List[int]


class SourceAemetCredentialsSettings(BaseModel):
    api_key: str = None


class SourceAemetSettings(BaseModel):
    credentials: SourceAemetCredentialsSettings


class SourcesSettings(BaseSettings):
    emt: Optional[SourceEmtSettings] = None
    aemet: Optional[SourceAemetSettings] = None


# Storage


class StorageMinioSettings(BaseModel):
    access_key: str
    secret_key: str
    endpoint: str
    secure: bool
    bucket: str


class StorageLocalSettings(BaseModel):
    path: str


class StorageConfigSettings(BaseModel):
    minio: Optional[StorageMinioSettings]
    local: Optional[StorageLocalSettings]


class StorageSettings(BaseModel):
    default: Optional[str] = "local"
    config: StorageConfigSettings

    @model_validator(mode="after")
    def check_storage_config(self) -> "StorageSettings":
        if self.default not in ["minio", "local"]:
            raise ValueError("Provide a valid default storage: minio or local")
        return self


# General settings


class Settings(BaseSettings):
    sources: SourcesSettings
    storage: StorageSettings
