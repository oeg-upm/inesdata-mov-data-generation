import pytest
import pandas as pd
from pathlib import Path

import os
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from inesdata_mov_datasets.sources.create.aemet import generate_df_from_file, download_aemet, generate_day_df, create_aemet  
from inesdata_mov_datasets.settings import Settings

import io

import json
from datetime import datetime
import tempfile
from loguru import logger
import traceback

###################### generate_df_from_file
# Caso exitoso
def test_generate_df_from_file_success():
    """Test de éxito para la función `generate_df_from_file`."""

    # Simulación de contenido de entrada (dict)
    mock_content = [{
        "prediccion": {
            "dia": [{
                "fecha": "2024/10/07",
                "orto": "07:00",
                "ocaso": "19:00",
                "temperatura": [{
                    "periodo": 0,
                    "value": 15,
                    "descripcion": "15"
                }],
                "viento": [{
                    "periodo": 0,
                    "value": 10,
                    "descripcion": "SO"
                }]
            }]
        }
    }]

    # Fecha para filtrar
    date = "2024/10/07"

    # Ejecutar la función
    result_df = generate_df_from_file(mock_content, date)

    # Comprobaciones
    assert not result_df.empty
    assert "temperatura_value" in result_df.columns
    assert "viento_value" in result_df.columns
    assert result_df["temperatura_value"].iloc[0] == 15
    assert result_df["viento_value"].iloc[0] == 10

# Caso con predicción vacía
def test_generate_df_from_file_empty_prediction():
    """Test cuando no hay predicción en el contenido."""

    # Simulación de contenido con predicción vacía
    mock_content = [{"prediccion": []}]

    # Fecha para filtrar
    date = "2024/10/07"

    # Ejecutar la función
    result_df = generate_df_from_file(mock_content, date)

    # Comprobaciones
    assert result_df.empty

# Caso de excepción
def test_generate_df_from_file_exception():
    """Test para manejar una excepción dentro de `generate_df_from_file`."""
    
    # Simulación de contenido incorrecto que cause una excepción
    mock_content = [{"prediccion": None}]
    date = "2024/10/07"

    with patch('loguru.logger.error') as mock_logger_error:
        # Ejecutar la función
        result_df = generate_df_from_file(mock_content, date)

        # Comprobaciones
        assert result_df.empty

        # Verificar que el error fue registrado en los logs
        assert mock_logger_error.called



###################### generate_day_df
@patch('inesdata_mov_datasets.sources.create.aemet.logger')
@patch('inesdata_mov_datasets.sources.create.aemet.os.listdir')
@patch('inesdata_mov_datasets.sources.create.aemet.open', new_callable=mock_open, read_data='{"data": [{"valor": 10, "periodo": "1200"}]}')
@patch('inesdata_mov_datasets.sources.create.aemet.generate_df_from_file')
def test_generate_day_df_valid_data(mock_generate_df_from_file, mock_open, mock_listdir, mock_logger):
    """Test para verificar la generación de DataFrame con datos válidos."""
    # Mock listdir para devolver archivos ficticios
    mock_listdir.return_value = ["file1.json", "file2.json"]
    
    # Simular la salida de generate_df_from_file con un DataFrame válido
    mock_generate_df_from_file.return_value = pd.DataFrame(
        {"periodo": ["1200"], "valor": [10], "datetime": pd.to_datetime("2024-10-01 12:00:00")}
    )

    storage_path = "/tmp"
    date = "2024/10/01"

    # Ejecutar la función
    generate_day_df(storage_path, date)

    # Verificar que el DataFrame fue generado y guardado correctamente
    processed_file_path = Path(storage_path) / Path("processed") / "aemet" / date / f"aemet_{date.replace('/', '')}.csv"
    assert processed_file_path.is_file(), "El archivo procesado no fue creado"

@patch('inesdata_mov_datasets.sources.create.aemet.logger')
@patch('inesdata_mov_datasets.sources.create.aemet.os.listdir')
def test_generate_day_df_no_files(mock_listdir, mock_logger):
    """Test para verificar el comportamiento cuando no hay archivos en el directorio."""
    # Simular que no hay archivos en el directorio
    mock_listdir.return_value = []

    storage_path = "/tmp"
    date = "2024/10/02"

    # Ejecutar la función
    generate_day_df(storage_path, date)

    # Verificar que no se generó ningún archivo procesado
    processed_file_path = Path(storage_path) / Path("processed") / "aemet" / date / f"aemet_{date.replace('/', '')}.csv"
    print(processed_file_path)
    assert not processed_file_path.is_file(), "No debería haberse creado un archivo procesado"

@patch('inesdata_mov_datasets.sources.create.aemet.logger')
@patch('inesdata_mov_datasets.sources.create.aemet.os.listdir')
@patch('builtins.open', new_callable=mock_open, read_data='{"no_data_key": [{"valor": 10}]}')
def test_generate_day_df_incorrect_structure(mock_open, mock_listdir, mock_logger):
    """Test para verificar el comportamiento cuando la estructura del archivo es incorrecta."""
    # Simular que hay un archivo en el directorio, pero con una estructura incorrecta
    mock_listdir.return_value = ["file1.json"]

    storage_path = "/tmp"
    date = "2024/10/03"

    # Ejecutar la función
    generate_day_df(storage_path, date)

    # Verificar que el logger registró un mensaje de debug indicando que no se creó un DataFrame
    mock_logger.debug.assert_called_with("There is no data to create")

###################### create_aemet
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    # Crear un mock para la clase Settings
    settings = MagicMock()
    
    # Configuración del mock
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.local = MagicMock()
    settings.storage.config.local.path = "/tmp"
    settings.storage.default = "local"
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"

    return settings

@patch('inesdata_mov_datasets.sources.create.aemet.instantiate_logger')
@patch('inesdata_mov_datasets.sources.create.aemet.logger.info')
@patch('inesdata_mov_datasets.sources.create.aemet.download_aemet')
@patch('inesdata_mov_datasets.sources.create.aemet.generate_day_df')
@patch('inesdata_mov_datasets.sources.create.aemet.logger.debug')
def test_create_aemet(mock_logger_debug, mock_generate_day_df, mock_download_aemet, mock_logger_info, mock_instantiate_logger, mock_settings):
    """Test para la función `create_aemet`."""
    
    date = "2024/10/07"

    # Ejecutar la función
    create_aemet(mock_settings, date)

    # Verificar que se llamó al logger de info
    mock_logger_info.assert_called_once_with(f"Creating AEMET dataset for date: {date}")

    # Verificar que el logger fue instanciado
    mock_instantiate_logger.assert_called_once_with(mock_settings, "AEMET", "create")

    # Verificar que se descargaron los datos si no está en modo local
    mock_download_aemet.assert_not_called()  # Cambia esto si es necesario

    # Verificar que se llamó a `generate_day_df` con los argumentos correctos
    mock_generate_day_df.assert_called_once_with(storage_path="/tmp", date=date)

@patch('inesdata_mov_datasets.sources.create.aemet.instantiate_logger')
@patch('inesdata_mov_datasets.sources.create.aemet.logger.error')
@patch('inesdata_mov_datasets.sources.create.aemet.download_aemet')
@patch('inesdata_mov_datasets.sources.create.aemet.generate_day_df')
@patch('inesdata_mov_datasets.sources.create.aemet.logger.info')
def test_create_aemet_exception(
    mock_logger_info,
    mock_generate_day_df,
    mock_settings,
    mock_logger_error,
    mock_instantiate_logger):
    """Test para manejar excepciones en `create_aemet`."""
    
    # Simular una excepción al llamar a `generate_day_df`
    mock_generate_day_df.side_effect = Exception("Test error")
    
    date = "2024/10/07"

    # Llamar a la función
    create_aemet(mock_settings, date)
    # Verificar que el logger de error fue llamado dos veces
    assert mock_logger_error.call_count == 2  # Verificar que se llame dos veces

    # Verificar que el logger de info fue llamado
    mock_logger_info.assert_called_once_with(f"Creating AEMET dataset for date: {date}")

    # Verificar que el logger fue instanciado
    mock_instantiate_logger.assert_called_once_with(mock_settings, "AEMET", "create")