import pytest
import asyncio
from unittest.mock import MagicMock,patch, mock_open
from inesdata_mov_datasets.sources.extract.informo import get_informo, save_informo
import xmltodict
from pathlib import Path
from loguru import logger
import json
import pytz
import datetime


###################### get_informo
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.sources = MagicMock()
    settings.sources.informo.credentials = MagicMock()
    settings.sources.informo.credentials.api_key = "test-api-key"
    return settings

# Parcheamos las dependencias de la función get_informo
@patch('inesdata_mov_datasets.sources.extract.informo.instantiate_logger') 
@patch('inesdata_mov_datasets.sources.extract.informo.logger.info')  
@patch('inesdata_mov_datasets.sources.extract.informo.logger.debug')  
@patch('inesdata_mov_datasets.sources.extract.informo.logger.error')  
@patch('inesdata_mov_datasets.sources.extract.informo.requests.get')  
@patch('inesdata_mov_datasets.sources.extract.informo.save_informo')  
@pytest.mark.asyncio
async def test_get_informo(mock_save_informo, mock_requests_get, mock_error, mock_debug, mock_info, mock_instantiate_logger, mock_settings):
    """Test para verificar la extracción de datos de INFORMO."""
    
    # Configurar la respuesta simulada de requests.get
    mock_response_content = "<xml><data>Datos de ejemplo</data></xml>"
    mock_requests_get.return_value = MagicMock(status_code=200, content=mock_response_content)

    # Ejecutar la función
    await get_informo(mock_settings)

    parsed_data = xmltodict.parse(mock_response_content)

    # Verificar que se llama a instantiate_logger
    mock_instantiate_logger.assert_called_once_with(mock_settings, "INFORMO", "extract")

    # Verificar que se llama a logger.info para iniciar la extracción
    mock_info.assert_any_call("Extracting INFORMO")
    mock_info.assert_any_call("Extracted INFORMO")

    # Verificar que se llama a requests.get una vez
    mock_requests_get.assert_called_once_with("https://informo.madrid.es/informo/tmadrid/pm.xml")
    # Verificar que se llama a save_informo con los datos obtenidos
    mock_save_informo.assert_called_once_with(mock_settings, parsed_data)

    # Verificar que se llama a logger.debug
    mock_debug.assert_called()

# Fixture para simular la configuración de settings
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.sources = MagicMock()
    settings.sources.informo.credentials = MagicMock()
    settings.sources.informo.credentials.api_key = "test-api-key"
    return settings

# Test para manejar un error HTTP en la solicitud
@patch('inesdata_mov_datasets.sources.extract.informo.instantiate_logger')   
@patch('inesdata_mov_datasets.sources.extract.informo.logger.error')   
@patch('inesdata_mov_datasets.sources.extract.informo.requests.get')   
@pytest.mark.asyncio
async def test_get_informo_http_error(mock_requests_get, mock_error_logger, mock_instantiate_logger, mock_settings):
    """Test para verificar que se captura un error HTTP."""
    
    # Simular una excepción en requests.get
    mock_requests_get.side_effect = Exception("Error en la conexión")

    # Ejecutar la función
    await get_informo(mock_settings)

    # Verificar que se llama a instantiate_logger
    mock_instantiate_logger.assert_called_once_with(mock_settings, "INFORMO", "extract")

    # Verificar que se capturó un error en los logs
    assert mock_error_logger.call_count == 2

# Test para manejar un error de parsing XML
@patch('inesdata_mov_datasets.sources.extract.informo.instantiate_logger')   
@patch('inesdata_mov_datasets.sources.extract.informo.logger.error')   
@patch('inesdata_mov_datasets.sources.extract.informo.requests.get')   
@patch('inesdata_mov_datasets.sources.extract.informo.save_informo')  
@pytest.mark.asyncio
async def test_get_informo_xml_parsing(mock_save_informo, mock_requests_get, mock_error_logger, mock_instantiate_logger, mock_settings):
    """Test para verificar que se captura un error de parsing XML."""
    
    # Simular una respuesta XML no válida
    mock_response = MagicMock(status_code=200, content="Invalid XML")
    mock_requests_get.return_value = mock_response

    # Ejecutar la función
    await get_informo(mock_settings)

    # Verificar que se llama a instantiate_logger
    mock_instantiate_logger.assert_called_once_with(mock_settings, "INFORMO", "extract")

    # Verificar que se capturó un error en los logs de parsing
    assert mock_error_logger.call_count == 2

    # Asegurarse de que save_informo no se llamó debido al error
    mock_save_informo.assert_not_called()

###################### save_informo
# Fixture para simular la configuración de settings
@pytest.fixture
def mock_settings_minio():
    """Fixture para simular la configuración de settings con almacenamiento Minio."""
    settings = MagicMock()
    settings.storage.default = "minio"
    settings.storage.config.minio.endpoint = "http://minio.local"
    settings.storage.config.minio.access_key = "minio_access_key"
    settings.storage.config.minio.secret_key = "minio_secret_key"
    settings.storage.config.minio.bucket = "test-bucket"
    return settings

@pytest.fixture
def mock_settings_local():
    """Fixture para simular la configuración de settings con almacenamiento local."""
    settings = MagicMock()
    settings.storage.default = "local"
    settings.storage.config.local.path = "/tmp"
    return settings

# Simular los datos que se pasarán a la función
@pytest.fixture
def mock_data():
    return {
        "pms": {
            "fecha_hora": "09/10/2024 14:30:00"
        }
    }

# Test para la funcionalidad de Minio
@patch('inesdata_mov_datasets.sources.extract.informo.check_s3_file_exists')  # Parchea la función que verifica si el archivo existe en Minio
@patch('inesdata_mov_datasets.sources.extract.informo.upload_objs')  # Parchea la función que sube los objetos a Minio
@pytest.mark.asyncio
async def test_save_informo_minio(mock_upload_objs, mock_check_s3_file_exists, mock_settings_minio, mock_data):
    """Test para verificar el almacenamiento en Minio."""
    
    # Simular que el archivo no existe en Minio
    mock_check_s3_file_exists.return_value = False

    # Llamar a la función
    await save_informo(mock_settings_minio, mock_data)

    date_from_file = mock_data["pms"]["fecha_hora"]
    dt = datetime.datetime.strptime(date_from_file, "%d/%m/%Y %H:%M:%S")
    formated_date = dt.strftime("%Y-%m-%dT%H%M")
    # Get the timezone from Madrid and formated the dates for the object_name of the files
    europe_timezone = pytz.timezone("Europe/Madrid")
    current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
    print(current_datetime)
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    ) 
    
    # Verificar que se llamó a check_s3_file_exists con los argumentos correctos
    mock_check_s3_file_exists.assert_called_once_with(
        endpoint_url=mock_settings_minio.storage.config.minio.endpoint,
        aws_secret_access_key=mock_settings_minio.storage.config.minio.secret_key,
        aws_access_key_id=mock_settings_minio.storage.config.minio.access_key,
        bucket_name=mock_settings_minio.storage.config.minio.bucket,
        object_name=f"raw/informo/{formatted_date_slash}/informo_{formated_date}.json"
    )

    # Verificar que se llamó a upload_objs para subir el archivo
    mock_upload_objs.assert_called_once()
    
# Test para la funcionalidad de almacenamiento local
@patch('inesdata_mov_datasets.sources.extract.informo.check_local_file_exists')  # Parchea la función que verifica si el archivo existe localmente
@patch('builtins.open', new_callable=mock_open)  # Parchea 'open' para evitar escribir en el sistema de archivos real
@pytest.mark.asyncio
async def test_save_informo_local(mock_open_func, mock_check_local_file_exists, mock_settings_local, mock_data):
    """Test para verificar el almacenamiento local."""
    
    # Simular que el archivo no existe en local
    mock_check_local_file_exists.return_value = False

    # Llamar a la función
    await save_informo(mock_settings_local, mock_data)
    date_from_file = mock_data["pms"]["fecha_hora"]
    dt = datetime.datetime.strptime(date_from_file, "%d/%m/%Y %H:%M:%S")
    formated_date = dt.strftime("%Y-%m-%dT%H%M")
    # Get the timezone from Madrid and formated the dates for the object_name of the files
    europe_timezone = pytz.timezone("Europe/Madrid")
    current_datetime = datetime.datetime.now(europe_timezone).replace(second=0)
    print(current_datetime)
    formatted_date_slash = current_datetime.strftime(
        "%Y/%m/%d"
    ) 

    # Verificar que se llamó a check_local_file_exists con los argumentos correctos
    mock_check_local_file_exists.assert_called_once_with(
        Path(f"/tmp/raw/informo/{formatted_date_slash}"),
        f"informo_{formated_date}.json"
    )

    # Verificar que se abrió el archivo correctamente para escribir los datos
    mock_open_func.assert_called_once_with(Path(f"/tmp/raw/informo/{formatted_date_slash}") / f"informo_{formated_date}.json", "w")

    # Verificar que se escribió el contenido JSON en el archivo
    mock_open_func().write.assert_called_once_with(json.dumps(mock_data))