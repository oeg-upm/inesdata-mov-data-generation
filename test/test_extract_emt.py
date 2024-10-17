import pytest
from aiohttp import ClientSession
from aioresponses import aioresponses
from unittest.mock import patch, AsyncMock, MagicMock
import os
import json
import datetime
from inesdata_mov_datasets.sources.extract.emt import get_calendar, get_line_detail, get_eta, login_emt, token_control,  get_emt
from inesdata_mov_datasets.settings import Settings

###################### get_calendar
@pytest.mark.asyncio
async def test_get_calendar():
    """Test para verificar la funcionalidad de get_calendar."""

    # Definir los parámetros de entrada
    start_date = "2024-10-01"
    end_date = "2024-10-07"
    headers = {"Authorization": "Bearer your_token"}

    # Crear una sesión de aiohttp
    async with ClientSession() as session:
        # Mockear la respuesta de la API
        calendar_url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/calendar/{start_date}/{end_date}/"
        mock_response = {"calendar": ["2024-10-01", "2024-10-02", "2024-10-03"]}

        with aioresponses() as m:
            m.get(calendar_url, payload=mock_response)

            # Llamar a la función
            result = await get_calendar(session, start_date, end_date, headers)

            # Verificar que la respuesta sea la esperada
            assert result == mock_response

@pytest.mark.asyncio
async def test_get_calendar_error():
    """Test para verificar el manejo de errores en get_calendar."""

    # Definir los parámetros de entrada
    start_date = "2024-10-01"
    end_date = "2024-10-07"
    headers = {"Authorization": "Bearer your_token"}

    # Crear una sesión de aiohttp
    async with ClientSession() as session:
        # Mockear la respuesta de la API con un error
        calendar_url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/calendar/{start_date}/{end_date}/"

        with aioresponses() as m:
            m.get(calendar_url, status=500) 

            # Llamar a la función
            result = await get_calendar(session, start_date, end_date, headers)

            # Verificar que el resultado sea un error manejado
            assert result == {"code": -1}

###################### get_line_detail
@pytest.mark.asyncio
async def test_get_line_detail_success():
    """Test para verificar el comportamiento exitoso de get_line_detail."""

    # Definir los parámetros de entrada
    date = "2024-10-01"
    line_id = "123"
    headers = {"Authorization": "Bearer your_token"}

    # Crear una sesión de aiohttp
    async with ClientSession() as session:
        # Mockear la respuesta de la API
        line_detail_url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{line_id}/info/{date}/"

        mock_response = {"line": line_id, "status": "operational"}  # Ejemplo de respuesta exitosa

        with aioresponses() as m:
            m.get(line_detail_url, payload=mock_response)  # Simular respuesta exitosa

            # Llamar a la función
            result = await get_line_detail(session, date, line_id, headers)

            # Verificar que el resultado sea el esperado
            assert result == mock_response

@pytest.mark.asyncio
async def test_get_line_detail_error():
    """Test para verificar el manejo de errores en get_line_detail."""

    # Definir los parámetros de entrada
    date = "2024-10-01"
    line_id = "123"
    headers = {"Authorization": "Bearer your_token"}

    # Crear una sesión de aiohttp
    async with ClientSession() as session:
        # Mockear la respuesta de la API con un error
        line_detail_url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{line_id}/info/{date}/"

        with aioresponses() as m:
            m.get(line_detail_url, status=500)  # Simular un error del servidor

            # Llamar a la función
            result = await get_line_detail(session, date, line_id, headers)

            # Verificar que el resultado sea un error manejado
            assert result == {"code": -1}

###################### get_eta
@pytest.mark.asyncio
async def test_get_eta_success():
    """Test para verificar el comportamiento exitoso de get_eta."""

    # Definir los parámetros de entrada
    stop_id = "456"
    headers = {"Authorization": "Bearer your_token"}

    # Crear una sesión de aiohttp
    async with ClientSession() as session:
        # Mockear la respuesta de la API
        eta_url = f"https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{stop_id}/arrives/"

        mock_response = {
            "estimatedArrivalTimes": [
                {"line": "L1", "arrivalTime": "10:00"},
                {"line": "L2", "arrivalTime": "10:05"},
            ]
        }  # Ejemplo de respuesta exitosa

        with aioresponses() as m:
            m.post(eta_url, payload=mock_response)  # Simular respuesta exitosa

            # Llamar a la función
            result = await get_eta(session, stop_id, headers)

            # Verificar que el resultado sea el esperado
            assert result == mock_response

@pytest.mark.asyncio
async def test_get_eta_error():
    """Test para verificar el manejo de errores en get_eta."""

    # Definir los parámetros de entrada
    stop_id = "456"
    headers = {"Authorization": "Bearer your_token"}

    # Crear una sesión de aiohttp
    async with ClientSession() as session:
        # Mockear la respuesta de la API con un error
        eta_url = f"https://openapi.emtmadrid.es/v2/transport/busemtmad/stops/{stop_id}/arrives/"

        with aioresponses() as m:
            m.post(eta_url, status=500)  # Simular un error del servidor

            # Llamar a la función
            result = await get_eta(session, stop_id, headers)

            # Verificar que el resultado sea un error manejado
            assert result == {"code": -1}

###################### login_emt
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.sources.emt.credentials.x_client_id = "test_client_id"
    settings.sources.emt.credentials.passkey = "test_passkey"
    settings.storage.default = "local"
    return settings

@patch('inesdata_mov_datasets.sources.extract.emt.requests.get')
@patch('builtins.open', new_callable=MagicMock)
@patch('os.makedirs')
@pytest.mark.asyncio
async def test_login_emt_success(mock_makedirs, mock_open, mock_requests_get, mock_settings):
    """Test para verificar el inicio de sesión exitoso en EMT."""

    # Simula la respuesta de la API
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [{"accessToken": "mock_access_token"}]
    }
    mock_requests_get.return_value = mock_response

    # Llama a la función
    token = await login_emt(mock_settings, object_login_name="login_response.json", local_path="/test/storage/")
    # Verifica que el token retornado sea el esperado
    assert token == "mock_access_token"

    # Verifica que se llamara a requests.get con los headers correctos
    mock_requests_get.assert_called_once_with(
        "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/",
        headers={
            "X-ClientId": "test_client_id",
            "passKey": "test_passkey",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        verify=True
    )

    # Verifica que se creara el directorio si no existe
    mock_makedirs.assert_called_once_with("/test/storage/", exist_ok=True)

    # Verifica que se escribiera el archivo con la respuesta de la API
    mock_open.assert_called_once_with("/test/storage/login_response.json", "w")

@patch('inesdata_mov_datasets.sources.extract.emt.requests.get')
@pytest.mark.asyncio
async def test_login_emt_failure(mock_requests_get, mock_settings):
    """Test para verificar el manejo de errores en el inicio de sesión."""

    # Simula una respuesta de error de la API
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": []
    }
    mock_requests_get.return_value = mock_response

    # Llama a la función
    token = await login_emt(mock_settings, "login_response.json")

    # Verifica que el token retornado sea una cadena vacía
    assert token == ""

    # Verifica que se llamara a requests.get una vez
    mock_requests_get.assert_called_once()
    
#TODO ###################### token_control
@pytest.fixture
def mock_config():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.storage.default = "local"  # Cambiar a "minio" si es necesario para otros tests
    settings.storage.config.local.path = "/tmp"
    return settings


@pytest.fixture
def mock_login_emt():
    """Fixture para simular la función login_emt."""
    with patch('inesdata_mov_datasets.sources.extract.emt.login_emt', new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def mock_check_local_file_exists():
    """Fixture para simular la función check_local_file_exists."""
    with patch('inesdata_mov_datasets.sources.extract.emt.check_local_file_exists') as mock:
        yield mock


@pytest.fixture
def mock_read_obj():
    """Fixture para simular la función read_obj."""
    with patch('inesdata_mov_datasets.sources.extract.emt.read_obj', new_callable=AsyncMock) as mock:
        yield mock


@pytest.mark.asyncio
async def test_token_control_regenerate_token(mock_config, mock_login_emt, mock_check_local_file_exists, mock_read_obj):
    """Test para verificar que se regenera el token si no existe o ha caducado."""

    # Simula que el archivo no existe
    mock_check_local_file_exists.return_value = False
    
    # Simula el token devuelto por login_emt
    mock_login_emt.return_value = "new_token"

    # Llama a la función
    token = await token_control(mock_config, "2024/10/08", "20241008")

    # Verificar que se llama a login_emt
    mock_login_emt.assert_called_once()
    
    # Verificar que el token es el esperado
    assert token == "new_token"

@pytest.fixture
def mock_config_minio():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.storage.default = "minio"  # Cambiar a "minio" si es necesario para otros tests
    settings.storage.config.local.path = "/tmp"
    return settings

@patch('inesdata_mov_datasets.sources.extract.emt.check_s3_file_exists')
@patch('inesdata_mov_datasets.sources.extract.emt.login_emt')
@pytest.mark.asyncio
async def test_token_control_minio_file_not_exists(mock_login_emt, mock_check_s3_file_exists, mock_config_minio):

    # Simular que el archivo de login no existe en MinIO
    mock_check_s3_file_exists.return_value = False
    mock_login_emt.return_value = "fake_token"

    token = await token_control(mock_config_minio, "2023-10-09", "20231009")

    assert token == "fake_token"
    mock_login_emt.assert_called_once()
    mock_check_s3_file_exists.assert_called_once()


###################### get_emt
@pytest.fixture
def mock_settings_get_emt():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.sources = MagicMock()
    settings.sources.emt.lines = ["line1", "line2"]  # Ejemplo de líneas
    settings.sources.emt.stops = ["1", "2"]  # Ejemplo de paradas
    settings.storage.default = "local"  # Cambia a "minio" si es necesario
    settings.storage.config.local.path = "/fake/path"  # Ruta ficticia para pruebas
    return settings


@patch('inesdata_mov_datasets.sources.extract.emt.instantiate_logger')  
@patch('inesdata_mov_datasets.sources.extract.emt.logger.info')  
@patch('inesdata_mov_datasets.sources.extract.emt.logger.debug')  
@patch('inesdata_mov_datasets.sources.extract.emt.logger.error')  
@patch('inesdata_mov_datasets.sources.extract.emt.token_control')  
@patch('inesdata_mov_datasets.sources.extract.emt.get_line_detail')  
@patch('inesdata_mov_datasets.sources.extract.emt.get_calendar')  
@patch('inesdata_mov_datasets.sources.extract.emt.get_eta')  
@patch('inesdata_mov_datasets.sources.extract.emt.upload_objs')  
@patch('inesdata_mov_datasets.sources.extract.emt.check_local_file_exists')  
@patch('inesdata_mov_datasets.sources.extract.emt.check_s3_file_exists')  
@pytest.mark.asyncio
async def test_get_emt_local(mock_check_s3_file_exists, mock_check_local_file_exists, mock_upload_objs,
                        mock_get_eta, mock_get_calendar, mock_get_line_detail,
                        mock_token_control, mock_error, mock_debug, mock_info, mock_instantiate_logger, mock_settings_get_emt):
    """Test para verificar la extracción de datos de EMT."""

    # Configura los mocks
    mock_token_control.return_value = "fake_token"
    mock_check_local_file_exists.return_value = False  # Simula que los archivos no existen
    mock_check_s3_file_exists.return_value = False  # Simula que los archivos no existen en S3

    # Simula las respuestas de los métodos asíncronos
    mock_get_line_detail.return_value = {"code": "00", "data": "line_data"}
    mock_get_calendar.return_value = {"code": "00", "data": "calendar_data"}
    mock_get_eta.return_value = {"code": "00", "data": "eta_data"}

    # Ejecutar la función
    await get_emt(mock_settings_get_emt)

    # Verificar que se llama a instantiate_logger
    mock_instantiate_logger.assert_called_once_with(mock_settings_get_emt, "EMT", "extract")

    # Verificar que se llama a logger.info para iniciar la extracción
    mock_info.assert_any_call("Extracting EMT")
    mock_info.assert_any_call("Extracted EMT")

    # Verificar que se llama a token_control una vez
    assert mock_token_control.call_count ==1  # Verifica que se llama con los argumentos correctos

    # Verificar que se llama a get_line_detail para cada línea
    assert mock_get_line_detail.call_count == len(mock_settings_get_emt.sources.emt.lines)

    # Verificar que se llama a get_calendar una vez
    mock_get_calendar.assert_called_once()


    # Verificar que se llama a upload_objs al final si hay datos
    mock_upload_objs.assert_not_called()

    # Verificar que se llama a logger.debug
    mock_debug.assert_called()


@patch('inesdata_mov_datasets.sources.extract.emt.check_local_file_exists')
@patch('inesdata_mov_datasets.sources.extract.emt.get_line_detail')
@pytest.mark.asyncio
async def test_get_emt_file_exists_local(mock_check_local_file_exists, mock_get_line_detail, mock_settings_get_emt):
    mock_check_local_file_exists.return_value = True  # Simula que el archivo ya existe
    
    await get_emt(mock_settings_get_emt)
    
    # Verifica que no se haya llamado a la función get_line_detail
    mock_get_line_detail.assert_not_called()


@patch('inesdata_mov_datasets.sources.extract.emt.get_line_detail')
@patch('inesdata_mov_datasets.sources.extract.emt.logger.error')
@pytest.mark.asyncio
async def test_get_emt_line_detail_error_local(mock_logger_error,mock_get_line_detail, mock_settings_get_emt):
    mock_get_line_detail.return_value = {"code": "99", "error": "Line Error"}  # Simula error en la respuesta
    
    await get_emt(mock_settings_get_emt)
    
    # Verifica que el error se haya registrado en los logs
    assert mock_logger_error.call_count == 2

@patch('inesdata_mov_datasets.sources.extract.emt.get_eta')
@patch('inesdata_mov_datasets.sources.extract.emt.logger.error')
@pytest.mark.asyncio
async def test_get_emt_eta_errors_local(mock_logger_error,mock_get_eta, mock_settings_get_emt):
    mock_get_eta.return_value = {"code": "99", "error": "ETA Error"}  # Simula error en las paradas
    
    await get_emt(mock_settings_get_emt)
    
    assert mock_logger_error.call_count == 2

@patch('inesdata_mov_datasets.sources.extract.emt.get_calendar')
@patch('inesdata_mov_datasets.sources.extract.emt.logger.error')
@pytest.mark.asyncio
async def test_get_emt_calendar_errors_local(mock_logger_error,mock_get_calendar, mock_settings_get_emt):
    mock_get_calendar.return_value = {"code": "99", "error": "ETA Error"}  # Simula error en las paradas
    
    await get_emt(mock_settings_get_emt)
    
    assert mock_logger_error.call_count == 2

@pytest.fixture
async def test_get_emt_minio_success():
    config = Settings()  # Instancia la configuración que se utiliza en producción.
    config.storage.default = "minio"
    
    # Mock del response de token_control
    with patch("inesdata_mov_datasets.sources.extract.emt.token_control", return_value="mocked_token") as mock_token_control:
        # Mock del método de ClientSession
        with patch("aiohttp.ClientSession") as MockClientSession:
            session_mock = MagicMock()
            MockClientSession.return_value.__aenter__.return_value = session_mock
            
            # Mock de los métodos relacionados con MinIO
            with patch("inesdata_mov_datasets.sources.extract.emt.check_s3_file_exists", return_value=False) as mock_check_s3:
                with patch("inesdata_mov_datasets.sources.extract.emt.get_line_detail", return_value={"code": "00"}) as mock_line_detail:
                    with patch("inesdata_mov_datasets.sources.extract.emt.get_calendar", return_value={"code": "00"}) as mock_calendar:
                        with patch("inesdata_mov_datasets.sources.extract.emt.get_eta", return_value={"code": "00"}) as mock_eta:
                            with patch("inesdata_mov_datasets.sources.extract.emt.upload_objs") as mock_upload_objs:
                                # Ejecución de la función a probar
                                await get_emt(config)
                                
                                # Verificación de que las funciones han sido llamadas correctamente
                                mock_token_control.assert_called_once()
                                session_mock.get.assert_called()  # Verifica que la sesión hizo peticiones
                                mock_check_s3.assert_called()  # Verifica que se comprobaron archivos en S3
                                mock_line_detail.assert_called()  # Verifica que se llamaron las líneas de detalle
                                mock_calendar.assert_called()  # Verifica que se llamó el calendario
                                mock_eta.assert_called()  # Verifica que se llamaron las ETAs
                                mock_upload_objs.assert_called()  # Verifica que se subieron archivos a S3

@pytest.fixture
def mock_settings_get_emt_minio():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.sources = MagicMock()
    settings.sources.emt.lines = ["line1", "line2"]  # Ejemplo de líneas
    settings.sources.emt.stops = ["1", "2"]  # Ejemplo de paradas
    settings.storage.default = "minio"  # Cambia a "minio" si es necesario
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "minio_access_key"
    settings.storage.config.minio.secret_key =  "minio_secret_key"
    settings.storage.config.minio.bucket = "emt_bucket"
      # Ruta ficticia para pruebas
    return settings

@pytest.mark.asyncio
async def test_get_emt_minio_error_handling(mock_settings_get_emt_minio):
    
    with patch("inesdata_mov_datasets.sources.extract.emt.token_control", return_value="mocked_token"):
        with patch("aiohttp.ClientSession") as MockClientSession:
            session_mock = MagicMock()
            MockClientSession.return_value.__aenter__.return_value = session_mock
            
            # Simulamos errores en la respuesta de los endpoints
            with patch("inesdata_mov_datasets.sources.extract.emt.get_line_detail", return_value={"code": "01"}):  # Error en line_detail
                with patch("inesdata_mov_datasets.sources.extract.emt.get_eta", return_value={"code": "01"}):  # Error en ETA
                    with patch("inesdata_mov_datasets.sources.extract.emt.upload_objs") as mock_upload_objs:
                        await get_emt(mock_settings_get_emt_minio)
                        
                        # Verificamos que no se subieron archivos debido a errores
                        mock_upload_objs.assert_not_called()


@patch('inesdata_mov_datasets.sources.extract.emt.check_s3_file_exists')
@patch('inesdata_mov_datasets.sources.extract.emt.get_line_detail')
@pytest.mark.asyncio
async def test_get_emt_file_exists_minio(mock_check_s3_file_exists, mock_get_line_detail, mock_settings_get_emt):
    mock_check_s3_file_exists.return_value = True  # Simula que el archivo ya existe
    
    await get_emt(mock_settings_get_emt)
    
    # Verifica que no se haya llamado a la función get_line_detail
    mock_get_line_detail.assert_not_called()


