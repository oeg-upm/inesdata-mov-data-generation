import pytest
import pandas as pd
from pathlib import Path

import os
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from inesdata_mov_datasets.sources.create.aemet import generate_df_from_file, download_aemet, generate_day_df, create_aemet  
from inesdata_mov_datasets.settings import Settings

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
# @pytest.fixture
# def setup_temp_files(tmp_path):
#     """Fixture para crear archivos temporales para la prueba."""
#     storage_path = tmp_path / "raw" / "aemet" / "2024" / "10" / "07"
#     storage_path.mkdir(parents=True, exist_ok=True)

#     # Crear archivos de ejemplo
#     example_data_1 = [{"periodo": "10", "value": 1}, {"periodo": "20", "value": 2}]
#     with open(storage_path / "data1.json", "w") as f:
#         json.dump(example_data_1, f)

#     example_data_2 = [{"periodo": "30", "value": 3}, {"periodo": "40", "value": 4}]
#     with open(storage_path / "data2.json", "w") as f:
#         json.dump(example_data_2, f)

#     return str(storage_path)

# @patch('inesdata_mov_datasets.sources.create.aemet.generate_df_from_file')  # Cambia 'your_module' por el nombre de tu módulo
# def test_generate_day_df(mock_generate_df_from_file, setup_temp_files):
#     """Test para la función `generate_day_df`."""
    
#     # Configurar el mock para que devuelva DataFrames
#     mock_generate_df_from_file.side_effect = [
#         pd.DataFrame({"periodo": ["10", "20"], "value": [1, 2]}),
#         pd.DataFrame({"periodo": ["30", "40"], "value": [3, 4]})
#     ]

#     # Llamar a la función
#     generate_day_df(storage_path=setup_temp_files, date="2024/10/07")
#     print(mock_generate_df_from_file.call_count)
#     # Verificar que se llamara al mock la cantidad correcta de veces
#     # assert mock_generate_df_from_file.call_count == 2

#     # Comprobar que los archivos se han procesado correctamente
#     processed_file_path = os.path.join(setup_temp_files.replace("raw", "processed"), "aemet", "20241007.csv")
#     assert os.path.exists(processed_file_path)

#     # Cargar el DataFrame procesado para verificar su contenido
#     processed_df = pd.read_csv(processed_file_path)
#     assert processed_df.shape[0] == 4  # Verificar que hay 4 filas
#     assert "datetime" in processed_df.columns  # Comprobar que la columna datetime está presente
#     assert "data" in processed_df.columns  # Comprobar que la columna data está presente

# @patch('inesdata_mov_datasets.sources.create.aemet.os.listdir')
# @patch('inesdata_mov_datasets.sources.create.aemet.logger.debug')
# def test_generate_day_df_no_files(mock_logger_debug, mock_listdir):
#     """Test para manejar el caso donde no hay archivos para procesar."""
    
#     mock_listdir.return_value = []
    
#     # Llamar a la función
#     storage_path = "/tmp"
#     date = "2024/10/07"
#     generate_day_df(storage_path, date)

#     # Verificar que el logger fue llamado indicando que no hay datos
#     mock_logger_debug.assert_called_once_with("There is no data to create")

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