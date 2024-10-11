import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import requests
import datetime
from pathlib import Path
import pytz
from inesdata_mov_datasets.sources.extract.aemet import get_aemet, save_aemet  # Cambia esto por el nombre real de tu módulo

###################### get_aemet
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.sources = MagicMock()
    settings.sources.aemet.credentials = MagicMock()
    settings.sources.aemet.credentials.api_key = "test-api-key"
    return settings

@patch('inesdata_mov_datasets.sources.extract.aemet.instantiate_logger')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.logger.info')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.logger.debug')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.logger.error')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.requests.get')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.save_aemet')  # Cambia esto por el nombre real de tu módulo
@pytest.mark.asyncio
async def test_get_aemet(mock_save_aemet, mock_requests_get, mock_error, mock_debug, mock_info, mock_instantiate_logger, mock_settings):
    """Test para verificar la extracción de datos de AEMET."""
    
    # Configura la respuesta simulada de requests.get
    mock_requests_get.side_effect = [
        MagicMock(status_code=200, json=lambda: {"datos": "https://example.com/aemet_data.json"}),  # Primera llamada para la URL de AEMET
        MagicMock(status_code=200, json=lambda: {"temperature": 22})  # Segunda llamada para los datos
    ]

    # Ejecutar la función
    await get_aemet(mock_settings)

    # Verificar que se llama a instantiate_logger
    mock_instantiate_logger.assert_called_once_with(mock_settings, "AEMET", "extract")

    # Verificar que se llama a logger.info para iniciar la extracción
    mock_info.assert_any_call("Extracting AEMET")
    mock_info.assert_any_call("Extracted AEMET")

    # Verificar que se llama a requests.get dos veces
    assert mock_requests_get.call_count == 2

    # Verificar que se llama a save_aemet con los datos obtenidos
    mock_save_aemet.assert_called_once_with(mock_settings, {"temperature": 22})

    # Verificar que se llama a logger.debug
    mock_debug.assert_called()

###################### save_aemet
@pytest.fixture
def mock_settings_minio():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"
    settings.storage.default = "minio"  # Cambia esto a "local" para otro test
    return settings

@pytest.fixture
def mock_settings_local():
    """Fixture para simular la configuración de settings con almacenamiento local."""
    settings = MagicMock()
    settings.storage.default = "local"
    settings.storage.config.local.path = "/tmp"
    return settings

@patch('inesdata_mov_datasets.sources.extract.aemet.upload_objs')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.check_s3_file_exists')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.logger.debug')  # Cambia esto por el nombre real de tu módulo
@pytest.mark.asyncio
async def test_save_aemet_minio(mock_debug, mock_check_s3_file_exists, mock_upload_objs, mock_settings_minio):
    """Test para verificar la funcionalidad de guardar datos de AEMET en MinIO."""
    mock_check_s3_file_exists.return_value = False  
    data = {"temperature": 22}  # Datos de prueba

    # Ejecutar la función
    await save_aemet(mock_settings_minio, data)

    europe_timezone = pytz.timezone("Europe/Madrid")
    current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
    formatted_date_day = current_datetime.strftime(
        "%Y%m%d"
    )  # formatted date year|month|day all together
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )
    # Verificar que check_s3_file_exists fue llamado con los parámetros correctos
    mock_check_s3_file_exists.assert_called_once_with(
        endpoint_url=mock_settings_minio.storage.config.minio.endpoint,
        aws_secret_access_key=mock_settings_minio.storage.config.minio.secret_key,
        aws_access_key_id=mock_settings_minio.storage.config.minio.access_key,
        bucket_name=mock_settings_minio.storage.config.minio.bucket,
        object_name=f"raw/aemet/{formatted_date_slash}/aemet_{formatted_date_day}.json"  # Aquí se puede especificar el objeto esperado, si es necesario
    )

    # Verificar que se llama a upload_objs
    mock_upload_objs.assert_called_once()

@patch('inesdata_mov_datasets.sources.extract.aemet.upload_objs')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.check_local_file_exists')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.extract.aemet.logger.debug')  # Cambia esto por el nombre real de tu módulo
@pytest.mark.asyncio
async def test_save_aemet_local(mock_debug, mock_check_local_file_exists, mock_upload_objs, mock_settings_local):
    """Test para verificar la funcionalidad de guardar datos de AEMET localmente."""
    mock_check_local_file_exists.return_value = False  # Simulamos que el archivo no existe
    data = {"temperature": 22}  # Datos de prueba

    # Ejecutar la función
    await save_aemet(mock_settings_local, data)
    europe_timezone = pytz.timezone("Europe/Madrid")
    current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
    formatted_date_day = current_datetime.strftime(
        "%Y%m%d"
    )  # formatted date year|month|day all together
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    )

    # Verificar que check_local_file_exists fue llamado con los parámetros correctos
    mock_check_local_file_exists.assert_called_once_with(
        Path(f"/tmp/raw/aemet/{formatted_date_slash}"),
        f"aemet_{formatted_date_day}.json"  # Aquí se puede especificar el objeto esperado, si es necesario
    )

    # Verificar que no se llama a upload_objs, ya que se guarda localmente
    mock_upload_objs.assert_not_called()

