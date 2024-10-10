import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
from datetime import datetime
from inesdata_mov_datasets.sources.create.informo import generate_df_from_file, generate_day_df, create_informo

###################### generate_df_from_file
@patch('inesdata_mov_datasets.sources.create.informo.logger')  # Parchea el logger para evitar la salida real en los tests
def test_generate_df_from_file_valid_data(mock_logger):
    """Test para verificar la generación de DataFrame con datos válidos."""
    content = {
        "pm": [
            {"valor": 10, "otro_valor": 20},
            {"valor": 15, "otro_valor": 25}
        ],
        "fecha_hora": "2024-10-01 12:00:00"
    }

    expected_df = pd.DataFrame(content["pm"])
    expected_df["datetime"] = pd.to_datetime(content["fecha_hora"], dayfirst=True)
    expected_df["date"] = pd.to_datetime(expected_df["datetime"].dt.date)

    # Ejecutar la función
    result_df = generate_df_from_file(content)

    # Verificar que el DataFrame resultante es igual al esperado
    pd.testing.assert_frame_equal(result_df, expected_df)

@patch('inesdata_mov_datasets.sources.create.informo.logger')
def test_generate_df_from_file_empty_content(mock_logger):
    """Test para verificar el comportamiento con contenido vacío."""
    content = {}

    # Ejecutar la función
    result_df = generate_df_from_file(content)

    # Verificar que el DataFrame resultante está vacío
    assert result_df.empty, "El DataFrame no debe estar vacío para contenido vacío"

@patch('inesdata_mov_datasets.sources.create.informo.logger')
def test_generate_df_from_file_invalid_data(mock_logger):
    """Test para verificar el manejo de excepciones con datos inválidos."""
    content = {
        "pm": "not a valid data structure",
        "fecha_hora": "2024-10-01 12:00:00"
    }

    # Ejecutar la función
    result_df = generate_df_from_file(content)

    # Verificar que el DataFrame resultante está vacío y no se produjo una excepción
    assert result_df.empty, "El DataFrame no debe estar vacío para datos inválidos"

    # Verificar que se registró el error
    mock_logger.error.assert_called()

@patch('inesdata_mov_datasets.sources.create.informo.logger')
def test_generate_df_from_file_no_pm_key(mock_logger):
    """Test para verificar el comportamiento cuando no hay clave 'pm' en el contenido."""
    content = {
        "fecha_hora": "2024-10-01 12:00:00"
    }

    # Ejecutar la función
    result_df = generate_df_from_file(content)

    # Verificar que el DataFrame resultante está vacío
    assert result_df.empty, "El DataFrame no debe estar vacío si falta la clave 'pm'"

###################### generate_day_df
@patch('inesdata_mov_datasets.sources.create.informo.logger')
@patch('inesdata_mov_datasets.sources.create.informo.os.listdir')
@patch('inesdata_mov_datasets.sources.create.informo.open', new_callable=mock_open, read_data='{"pms": [{"valor": 10, "fecha_hora": "2024-10-01 12:00:00"}]}')
@patch('inesdata_mov_datasets.sources.create.informo.generate_df_from_file')

def test_generate_day_df_valid_data(mock_generate_df_from_file, mock_open, mock_listdir, mock_logger):
    """Test para verificar la generación de DataFrame con datos válidos."""
    mock_listdir.return_value = ["file1.json", "file2.json"]
    mock_generate_df_from_file.return_value = pd.DataFrame(
        {"valor": [10], 
        "datetime": pd.to_datetime("2024-10-01 12:00:00")})

    storage_path = "/tmp"
    date = "2024/10/01"

    # Ejecutar la función
    generate_day_df(storage_path, date)

    # Verificar que el DataFrame fue generado y guardado
    processed_file_path = Path(storage_path) / Path("processed") / "informo" / date / f"informo_{date.replace('/', '')}.csv"
    assert processed_file_path.is_file(), "El archivo procesado no fue creado"

@patch('inesdata_mov_datasets.sources.create.informo.logger')
@patch('inesdata_mov_datasets.sources.create.informo.os.listdir')
def test_generate_day_df_no_files(mock_listdir, mock_logger):
    """Test para verificar el comportamiento cuando no hay archivos en el directorio."""
    mock_listdir.return_value = []

    storage_path = "/tmp"
    date = "2024/10/01"

    # Ejecutar la función
    generate_day_df(storage_path, date)

    # Verificar que no se generó el archivo procesado
    processed_file_path = Path(storage_path) / Path("processed") / "informo" / date.replace("/", "") / f"informo_{date.replace('/', '')}.csv"
    assert not processed_file_path.is_file(), "No debería haberse creado un archivo procesado"


@patch('inesdata_mov_datasets.sources.create.informo.logger')
@patch('inesdata_mov_datasets.sources.create.informo.os.listdir')
@patch('builtins.open', new_callable=mock_open, read_data='{"not_pms": [{"valor": 10}]}')
def test_generate_day_df_no_pms_key(mock_open, mock_listdir, mock_logger):
    """Test para verificar el comportamiento cuando no hay clave 'pms' en el contenido."""
    mock_listdir.return_value = ["file1.json"]

    storage_path = "/tmp"
    date = "2024/10/01"

    # Ejecutar la función
    generate_day_df(storage_path, date)

    # Verificar que el logger registró un mensaje de debug
    mock_logger.debug.assert_called_with("There is no data to create")


###################### create_informo
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    settings = MagicMock()
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.local = MagicMock()
    settings.storage.config.local.path = "/tmp"
    settings.storage.default = "minio"
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"
    return settings

@patch('inesdata_mov_datasets.sources.create.informo.instantiate_logger')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.create.informo.download_informo')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.create.informo.generate_day_df')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.create.informo.logger.info')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.create.informo.logger.debug')  # Cambia esto por el nombre real de tu módulo
@patch('inesdata_mov_datasets.sources.create.informo.logger.error')  # Cambia esto por el nombre real de tu módulo
def test_create_informo(mock_error, mock_debug, mock_info, mock_generate_day_df, mock_download_informo, mock_instantiate_logger, mock_settings):
    """Test para verificar la creación del dataset de INFORMO."""
    date = "2024/10/01"

    # Ejecutar la función
    create_informo(mock_settings, date)

    # Verificar que se llamó a instantiate_logger
    mock_instantiate_logger.assert_called_once_with(mock_settings, "INFORMO", "create")

    # Verificar que se llama a logger.info
    mock_info.assert_called_once_with(f"Creating INFORMO dataset for date: {date}")

    # Verificar que se llama a download_informo
    mock_download_informo.assert_called_once_with(
        bucket=mock_settings.storage.config.minio.bucket,
        prefix=f"raw/informo/{date}/",
        output_path=mock_settings.storage.config.local.path,
        endpoint_url=mock_settings.storage.config.minio.endpoint,
        aws_access_key_id=mock_settings.storage.config.minio.access_key,
        aws_secret_access_key=mock_settings.storage.config.minio.secret_key,
    )

    # Verificar que se llama a generate_day_df
    mock_generate_day_df.assert_called_once_with(storage_path=mock_settings.storage.config.local.path, date=date)

    # Verificar que se llama a logger.debug
    mock_debug.assert_called()
    assert any("Time duration of INFORMO dataset creation" in call[0][0] for call in mock_debug.call_args_list)