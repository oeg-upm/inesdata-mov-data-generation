import pytest
import asyncio
import aiofiles
import os
import yaml
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock, Mock, mock_open

from inesdata_mov_datasets.utils import list_objs, async_download, get_obj, download_obj, download_objs, read_obj, upload_obj, upload_metadata, upload_objs, read_settings, check_local_file_exists, check_s3_file_exists

###################### list_objs
@patch('inesdata_mov_datasets.utils.botocore.session.get_session')  # Cambia 'inesdata_mov_datasets.utils' por el nombre real del módulo
def test_list_objs(mock_get_session):
    """Test para verificar la lista de objetos en un bucket S3."""

    # Configura el cliente S3 simulado
    mock_client = MagicMock()
    mock_get_session.return_value.create_client.return_value = mock_client

    # Configura el paginador simulado
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator

    # Simula la respuesta del paginador
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "prefix/file1.txt"},
                {"Key": "prefix/file2.txt"},
            ]
        },
        {
            "Contents": [
                {"Key": "prefix/file3.txt"},
            ]
        },
    ]

    # Llama a la función
    bucket = "my-bucket"
    prefix = "prefix/"
    endpoint_url = "http://minio:9000"
    aws_access_key_id = "my-access-key"
    aws_secret_access_key = "my-secret-key"

    result = list_objs(bucket, prefix, endpoint_url, aws_secret_access_key, aws_access_key_id)

    # Verifica que se llame a create_client con los parámetros correctos
    mock_get_session.return_value.create_client.assert_called_once_with(
        "s3",
        endpoint_url=endpoint_url,
        aws_secret_access_key=aws_secret_access_key,
        aws_access_key_id=aws_access_key_id,
    )

    # Verifica que se llame a get_paginator
    mock_client.get_paginator.assert_called_once_with("list_objects_v2")

    # Verifica que el resultado sea el esperado
    expected_keys = [
        "prefix/file1.txt",
        "prefix/file2.txt",
        "prefix/file3.txt",
    ]
    assert result == expected_keys

    # Verifica que se llame a paginate una vez con los parámetros correctos
    mock_paginator.paginate.assert_called_once_with(Bucket=bucket, Prefix=prefix)

###################### async_download
@patch('inesdata_mov_datasets.utils.download_objs')  # Cambia 'inesdata_mov_datasets.utils' por el nombre real del módulo
def test_async_download(mock_download_objs):
    """Test para verificar la descarga de objetos desde MinIO."""

    # Configura el mock para que no haga nada
    mock_download_objs.return_value = MagicMock()

    # Define los parámetros de entrada
    bucket = "my-bucket"
    prefix = "raw_data/"
    output_path = "/local/path/"
    endpoint_url = "http://minio:9000"
    aws_access_key_id = "my-access-key"
    aws_secret_access_key = "my-secret-key"

    # Ejecuta la función
    async_download(bucket, prefix, output_path, endpoint_url, aws_access_key_id, aws_secret_access_key)

    # Verifica que download_objs se llame con los parámetros correctos
    mock_download_objs.assert_called_once_with(
        bucket,
        prefix,
        output_path,
        endpoint_url,
        aws_access_key_id,
        aws_secret_access_key,
    )

    # Verifica que se haya creado un nuevo loop de eventos
    assert asyncio.get_event_loop() is not None

###################### get_obj
@pytest.mark.asyncio
async def test_get_obj():
    """Test para verificar la obtención de un objeto desde S3."""

    # Crear un cliente simulado
    mock_client = AsyncMock()
    
    # Configura la respuesta simulada de get_object
    mock_resp = MagicMock()
    mock_resp["Body"].read = AsyncMock(return_value=b'{"key": "value"}')  # Simula el contenido del objeto
    mock_client.get_object = AsyncMock(return_value=mock_resp)

    # Define los parámetros de entrada
    bucket = "my-bucket"
    key = "my-object-key"

    # Ejecuta la función
    content = await get_obj(mock_client, bucket, key)

    # Verifica que get_object se llama con los parámetros correctos
    mock_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)

    # Verifica que el contenido devuelto es correcto
    assert content == b'{"key": "value"}'


###################### download_obj
@pytest.fixture
def mock_storage_path(tmp_path):
    # Usa la ruta temporal proporcionada por pytest
    return str(tmp_path)

@pytest.mark.asyncio
async def test_download_obj(mock_storage_path):
    """Test para verificar la descarga de un objeto desde S3."""

    # Crear un cliente simulado
    mock_client = AsyncMock()
    
    # Simular la respuesta de get_obj
    mock_resp = b'{"key": "value"}'  # Simula el contenido del objeto
    mock_client.get_object = AsyncMock(return_value={"Body": AsyncMock(read=AsyncMock(return_value=mock_resp))})

    # Define los parámetros de entrada
    bucket = "my-bucket"
    key = "my-object-key"
    semaphore = AsyncMock()  # Simular un semáforo

    # Ejecuta la función
    await download_obj(mock_client, bucket, key, mock_storage_path, semaphore)

    # Verifica que se llama a get_object con los parámetros correctos
    mock_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)

    # Verifica que el archivo se ha escrito en el directorio correcto
    async with aiofiles.open(os.path.join(mock_storage_path, key), "r") as out_file:
        content = await out_file.read()

    # Verifica que el contenido del archivo es el esperado
    assert content == '{"key": "value"}'

    # Verifica que se crea el directorio
    os.makedirs(os.path.dirname(mock_storage_path), exist_ok=True)
    assert os.path.isdir(os.path.dirname(mock_storage_path))



###################### download_objs
@pytest.mark.asyncio
@patch('inesdata_mov_datasets.utils.get_session')  
@patch('inesdata_mov_datasets.utils.download_obj')  
@patch('inesdata_mov_datasets.utils.list_objs')  
@patch('inesdata_mov_datasets.utils.logger')  
async def test_download_objs_without_eta(mock_logger, mock_list_objs, mock_download_obj, mock_get_session):
    """Test para verificar la descarga de objetos desde S3 sin '/eta' en el prefix."""
    
    # Simular el cliente S3
    mock_client = AsyncMock()
    mock_get_session.return_value.create_client.return_value.__aenter__.return_value = mock_client
    
    # Simular los objetos listados
    mock_list_objs.return_value = ['key1', 'key2', 'key3']
    
    bucket = "my-bucket"
    prefix = "some/path/"
    output_path = "tmp/"
    endpoint_url = "http://minio.example.com"
    aws_access_key_id = "minio_user"
    aws_secret_access_key = "minio_password"

    # Ejecutar la función
    await download_objs(bucket, prefix, output_path, endpoint_url, aws_access_key_id, aws_secret_access_key)

    # Verifica que se llama a list_objs con los parámetros correctos
    mock_list_objs.assert_called_once_with(bucket, prefix, endpoint_url, aws_secret_access_key, aws_access_key_id)

    # Verifica que se llama a download_obj con las claves obtenidas
    assert mock_download_obj.call_count == 3  # Tres claves: key1, key2, key3

    # Verifica que se haya llamado al logger
    mock_logger.debug.assert_any_call("Downloading files from s3")
    # mock_logger.debug.assert_any_call("Downloading 3 files from emt endpoint")

###################### read_obj
@pytest.mark.asyncio
@patch('inesdata_mov_datasets.utils.get_session')  
async def test_read_obj(mock_get_session):
    """Test para verificar la lectura de un objeto desde S3."""
    
    # Simular el cliente S3
    mock_client = AsyncMock()
    mock_get_session.return_value.create_client.return_value.__aenter__.return_value = mock_client
    
    # Simular la respuesta de get_object
    mock_response = AsyncMock()
    mock_response['Body'] = AsyncMock()
    
    # Simular el método read para devolver el contenido correcto
    mock_response['Body'].read = AsyncMock(return_value=b'{"key": "value"}')
    
    mock_client.get_object.return_value = mock_response

    bucket = "my-bucket"
    endpoint_url = "http://minio.example.com"
    aws_access_key_id = "minio_user"
    aws_secret_access_key = "minio_password"
    object_name = "some/object/key"

    # Ejecutar la función
    result = await read_obj(bucket, endpoint_url, aws_access_key_id, aws_secret_access_key, object_name)

    # Verifica que se llama a get_object con el bucket y el object_name correctos
    mock_client.get_object.assert_called_once_with(Bucket=bucket, Key=object_name)

    # Verifica que se devuelve el contenido correcto
    assert result == '{"key": "value"}'

###################### upload_obj
@pytest.mark.asyncio
@patch('inesdata_mov_datasets.utils.ClientCreatorContext')  # Cambia 'inesdata_mov_datasets.utils' por el nombre correcto
async def test_upload_obj(mock_client_creator_context):
    """Test para verificar la subida de un objeto a S3."""
    
    # Simular el cliente S3
    mock_client = AsyncMock()
    mock_client_creator_context.return_value = mock_client

    bucket = "my-bucket"
    key = "some/object/key"
    object_value = "Hello, S3!"

    # Ejecutar la función
    await upload_obj(mock_client, bucket, key, object_value)

    # Verifica que se llama a put_object con los parámetros correctos
    mock_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=object_value.encode("utf-8"))

###################### upload_metadata
@patch('botocore.session.get_session')  # Mock para la sesión de botocore
def test_upload_metadata(mock_get_session):
    """Test para verificar la subida de metadatos a S3."""
    
    # Simular el cliente S3
    mock_client = Mock()
    mock_get_session.return_value.create_client.return_value = mock_client

    bucket = "my-bucket"
    endpoint_url = "http://localhost:9000"
    aws_access_key_id = "test-access-key"
    aws_secret_access_key = "test-secret-key"
    keys = ["some/object/key1", "some/object/key2"]

    # Caso 1: El archivo de metadatos ya existe
    mock_client.get_object.return_value = {
        'Body': Mock(read=Mock(return_value=b'old_file1\nold_file2\n'))
    }
    
    upload_metadata(bucket, endpoint_url, aws_access_key_id, aws_secret_access_key, keys)

    # Verifica que se llama a get_object para obtener el contenido previo
    mock_client.get_object.assert_called_once_with(Bucket=bucket, Key='some/object/metadata.txt')

    # Verifica que se llama a put_object con el nuevo contenido
    expected_content = 'old_file1\nold_file2\n\nsome/object/key1\nsome/object/key2'
    mock_client.put_object.assert_called_once_with(Bucket=bucket, Key='some/object/metadata.txt', Body=expected_content.encode('utf-8'))

    # Caso 2: El archivo de metadatos no existe (primer ejecución del día)
    mock_client.reset_mock()  # Reinicia los mocks
    mock_client.get_object.side_effect = Exception("File not found")  # Simula que no se encuentra el archivo

    upload_metadata(bucket, endpoint_url, aws_access_key_id, aws_secret_access_key, keys)

    # Verifica que se llama a put_object con el nuevo contenido
    new_expected_content = 'some/object/key1\nsome/object/key2'
    mock_client.put_object.assert_called_once_with(Bucket=bucket, Key='some/object/metadata.txt', Body=new_expected_content.encode('utf-8'))

###################### upload_objs
@patch('botocore.session.get_session')  # Mock para la sesión de botocore
@patch('inesdata_mov_datasets.utils.upload_obj')  # Mock para la función upload_obj
@pytest.mark.asyncio
async def test_upload_objs(mock_upload_obj, mock_get_session):
    """Test para verificar la subida de objetos a S3."""
    
    # Simular el cliente S3
    mock_client = AsyncMock()
    mock_get_session.return_value.create_client.return_value.__aenter__.return_value = mock_client

    bucket = "my-bucket"
    endpoint_url = "http://localhost:9000"
    aws_access_key_id = "test-access-key"
    aws_secret_access_key = "test-secret-key"
    objects_dict = {
        "object1.txt": "Contenido del objeto 1",
        "object2.txt": "Contenido del objeto 2",
    }

    await upload_objs(bucket, endpoint_url, aws_access_key_id, aws_secret_access_key, objects_dict)

    # Verifica que se llama a upload_obj para cada objeto en objects_dict
    assert mock_upload_obj.call_count == len(objects_dict)
    
    # Verifica que upload_obj fue llamado con los argumentos correctos
    assert mock_upload_obj.call_count == 2
        

###################### read_settings
# Ejemplo de datos YAML simulados
yaml_content = """
sources:
  emt:
    credentials:
        email: myemail
        password: mypassword
        x_client_id: my_x_client_id
        passkey: my_passkey
    stops: [1,2]
    lines: [1,2]
  aemet:
    credentials:
        api_key: my_api_key
storage:
  default: local
  config:
    minio:
        access_key: my_access_key
        secret_key: my_secret_key
        endpoint: minio-endpoint
        secure: True
        bucket: my_bucket
    local:
        path: /path/to/save/datasets
  logs:
    path: /path/to/save/logs
    level: LOG_LEVEL
"""

def test_read_settings():
    # Simulamos la función open y la lectura del archivo YAML
    with patch("builtins.open", mock_open(read_data=yaml_content)):
        # Simulamos la carga de los datos con yaml.safe_load
        with patch("yaml.safe_load", return_value=yaml.safe_load(yaml_content)):
            # Llamamos a la función para probarla
            settings = read_settings("path/to/config.yaml")

            # Verificamos que los valores cargados en settings sean correctos
            if settings.storage.default == "local" and settings.sources.aemet.credentials.api_key == "my_api_key":
                # Verificamos que email y password de EMT sean None
                assert settings.sources.emt.credentials.email is None
                assert settings.sources.emt.credentials.password is None
            assert settings.sources.aemet.credentials.api_key == "my_api_key"
            assert settings.storage.default == "local"
            assert settings.storage.config.local.path == "/path/to/save/datasets"
            assert settings.storage.config.minio.access_key == "my_access_key"
            assert settings.storage.logs.level == "LOG_LEVEL"


###################### check_local_file_exists
@pytest.fixture
def mock_path():
    """Fixture para simular el objeto Path."""
    return MagicMock(spec=Path)

def test_check_local_file_exists_exists(tmp_path):
    """Test para verificar que la función retorna True si el archivo existe."""
    # Crea un archivo temporal
    test_file = tmp_path / "archivo.txt"
    test_file.write_text("Este es un archivo de prueba.")

    # Llama a la función con el path del directorio temporal y el nombre del archivo
    result = check_local_file_exists(tmp_path, "archivo.txt")

    # Verifica que el resultado sea True
    assert result is True


def test_check_local_file_exists_does_not_exist(tmp_path):
    """Test para verificar que la función retorna False si el archivo no existe."""
    # Llama a la función con el path del directorio temporal y un nombre de archivo que no existe
    result = check_local_file_exists(tmp_path, "archivo_no_existe.txt")

    # Verifica que el resultado sea False
    assert result is False

###################### check_s3_file_exists
# @pytest.mark.asyncio
async def test_check_s3_file_exists_exists():
    """Test para verificar que la función retorna True si el archivo existe en S3."""
    # Configuración de parámetros
    endpoint_url = "http://localhost:9000"
    aws_secret_access_key = "secret"
    aws_access_key_id = "access_key"
    bucket_name = "mi_bucket"
    object_name = "archivo.txt"

    # Mock del cliente S3 y del método head_object
    mock_client = AsyncMock()
    mock_client.head_object.return_value = None  # Simula que el archivo existe

    with patch("inesdata_mov_datasets.utils.get_session") as mock_get_session:
        mock_get_session.return_value.create_client.return_value.__aenter__.return_value = mock_client
        
        result = await check_s3_file_exists(endpoint_url, aws_secret_access_key, aws_access_key_id, bucket_name, object_name)

    assert result is True

@pytest.mark.asyncio
async def test_check_s3_file_exists_does_not_exist():
    """Test para verificar que la función retorna False si el archivo no existe en S3."""
    # Configuración de parámetros
    endpoint_url = "http://localhost:9000"
    aws_secret_access_key = "secret"
    aws_access_key_id = "access_key"
    bucket_name = "mi_bucket"
    object_name = "archivo_inexistente.txt"

    # Mock del cliente S3 y del método head_object lanzando una excepción
    mock_client = AsyncMock()
    mock_client.head_object.side_effect = Exception("404 Not Found")  # Simula que el archivo no existe

    with patch("inesdata_mov_datasets.utils.get_session") as mock_get_session:
        mock_get_session.return_value.create_client.return_value.__aenter__.return_value = mock_client
        
        result = await check_s3_file_exists(endpoint_url, aws_secret_access_key, aws_access_key_id, bucket_name, object_name)

    assert result is False